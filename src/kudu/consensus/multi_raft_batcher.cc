// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "multi_raft_batcher.h"
#include <functional>
#include <memory>
#include <utility>
#include <mutex>
#include <vector>

#include "kudu/common/wire_protocol.h"

#include "kudu/consensus/consensus_meta.h"
#include "kudu/rpc/proxy.h"

#include "kudu/rpc/periodic.h"

#include "kudu/util/flag_tags.h"

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/util/monotime.h"

using namespace std::literals;
using namespace std::placeholders;

DEFINE_int32(multi_raft_heartbeat_interval_ms,
             100,
             "The heartbeat interval for batch Raft replication.");
TAG_FLAG(multi_raft_heartbeat_interval_ms, experimental);
TAG_FLAG(multi_raft_heartbeat_interval_ms, hidden);

DEFINE_bool(enable_multi_raft_heartbeat_batcher,
            false,
            "Whether to enable the batching of raft heartbeats.");
TAG_FLAG(enable_multi_raft_heartbeat_batcher, experimental);
TAG_FLAG(enable_multi_raft_heartbeat_batcher, hidden);

DEFINE_int32(multi_raft_batch_size, 30, "Maximum batch size for a multi-raft consensus payload.");
TAG_FLAG(multi_raft_batch_size, experimental);
TAG_FLAG(multi_raft_batch_size, hidden);

DECLARE_int32(consensus_rpc_timeout_ms);

namespace kudu {
namespace consensus {

using rpc::PeriodicTimer;
using kudu::DnsResolver;

class ConsensusServiceProxy;
typedef std::unique_ptr<ConsensusServiceProxy> ConsensusServiceProxyPtr;

struct MultiRaftHeartbeatBatcher::MultiRaftConsensusData  {
  MultiRaftConsensusRequestPB batch_req;
  MultiRaftConsensusResponsePB batch_res;
  rpc::RpcController controller;
  std::vector<ResponseCallbackData> response_callback_data;
};

MultiRaftHeartbeatBatcher::MultiRaftHeartbeatBatcher(const kudu::HostPort& hostport,
                                                     DnsResolver* dns_resolver,
                                                     std::shared_ptr<kudu::rpc::Messenger> messenger)
    : messenger_(messenger),
      consensus_proxy_(std::make_unique<ConsensusServiceProxy>(messenger, hostport, dns_resolver)),
      current_batch_(std::make_shared<MultiRaftConsensusData>()) {}

void MultiRaftHeartbeatBatcher::Start() {
  std::weak_ptr<MultiRaftHeartbeatBatcher> const weak_peer = shared_from_this();
  batch_sender_ = PeriodicTimer::Create(
      messenger_,
      [weak_peer]() {
        if (auto peer = weak_peer.lock()) {
          peer->PrepareAndSendBatchRequest();
        }
      },
      MonoDelta::FromMilliseconds(FLAGS_multi_raft_heartbeat_interval_ms));
  batch_sender_->Start();
}

MultiRaftHeartbeatBatcher::~MultiRaftHeartbeatBatcher() = default;

void MultiRaftHeartbeatBatcher::AddRequestToBatch(ConsensusRequestPB* request,
                                                  ConsensusResponsePB* response,
                                                  HeartbeatResponseCallback callback) {
  std::shared_ptr<MultiRaftConsensusData> data = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    current_batch_->response_callback_data.push_back({response, std::move(callback)});
    // Add a ConsensusRequestPB to the batch
    current_batch_->batch_req.add_consensus_request()->Swap(request);
    if (FLAGS_multi_raft_batch_size > 0 &&
        current_batch_->response_callback_data.size() >= FLAGS_multi_raft_batch_size) {
      data = PrepareNextBatchRequest();
    }
  }
  if (data) {
    SendBatchRequest(data);
  }
}

void MultiRaftHeartbeatBatcher::PrepareAndSendBatchRequest() {
  std::shared_ptr<MultiRaftConsensusData> data;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    data = PrepareNextBatchRequest();
  }
  SendBatchRequest(data);
}

std::shared_ptr<MultiRaftHeartbeatBatcher::MultiRaftConsensusData>
MultiRaftHeartbeatBatcher::PrepareNextBatchRequest() {
  if (current_batch_->batch_req.consensus_request_size() == 0) {
    return nullptr;
  }
  batch_sender_->Snooze();
  auto data = std::move(current_batch_);
  current_batch_ = std::make_shared<MultiRaftConsensusData>();
  return data;
}

void MultiRaftHeartbeatBatcher::SendBatchRequest(std::shared_ptr<MultiRaftConsensusData> data) {
  if (!data) {
    return;
  }

  data->controller.Reset();
  data->controller.set_timeout(MonoDelta::FromMilliseconds(
      FLAGS_consensus_rpc_timeout_ms * data->batch_req.consensus_request_size()));
  consensus_proxy_->MultiRaftUpdateConsensusAsync(
      data->batch_req,
      &data->batch_res,
      &data->controller,
      std::bind(&MultiRaftHeartbeatBatcher::MultiRaftUpdateHeartbeatResponseCallback,
                shared_from_this(),
                data));
}

void MultiRaftHeartbeatBatcher::MultiRaftUpdateHeartbeatResponseCallback(
    std::shared_ptr<MultiRaftConsensusData> data) {
  auto status = data->controller.status();
  for (int i = 0; i < data->batch_req.consensus_request_size(); i++) {
    auto callback_data = data->response_callback_data[i];
    if (status.ok()) {
      callback_data.resp->Swap(data->batch_res.mutable_consensus_response(i));
    }
    callback_data.callback();
  }
}

MultiRaftManager::MultiRaftManager(std::shared_ptr<rpc::Messenger> messenger, kudu::DnsResolver* dns_resolver)
    : messenger_(messenger),
      dns_resolver_(dns_resolver) {}

MultiRaftHeartbeatBatcherPtr MultiRaftManager::AddOrGetBatcher(const kudu::consensus::RaftPeerPB& remote_peer_pb) {
  if (!FLAGS_enable_multi_raft_heartbeat_batcher) {
    return nullptr;
  }

  auto hostport = HostPortFromPB(remote_peer_pb.last_known_addr());
  std::lock_guard<std::mutex> lock(mutex_);
  MultiRaftHeartbeatBatcherPtr batcher;

  // After taking the lock, check if there is already a batcher
  // for the same remote host and return it.
  auto res = batchers_.find(hostport);
  if (res != batchers_.end() && (batcher = res->second.lock())) {
    return batcher;
  }
  batcher = std::make_shared<MultiRaftHeartbeatBatcher>(hostport, dns_resolver_, messenger_);
  batchers_[hostport] = batcher;
  batcher->Start();
  return batcher;
}

}  // namespace consensus
}  // namespace kudu
