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

#include "kudu/cdc/cdc_service.h"

#include <memory>
#include <chrono>
#include <thread>

#include "kudu/cdc/cdc_producer.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
// #include "kudu/tserver/service_util.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"

namespace kudu {
namespace cdc {
// using consensus::LeaderStatus;
using consensus::RaftConsensus;
using rpc::RpcContext;
using tserver::TSTabletManager;
using master::MasterServiceProxy;
using strings::Substitute;
using kudu::Status;
using kudu::rpc::RpcController;

namespace {

constexpr std::chrono::seconds leader_scan_period(2);

}
class PoorMansAsyncClient {
 public:
  PoorMansAsyncClient(const std::vector<HostPort>& masters, tserver::TabletServer& this_server) {
    for (const auto& master: masters) {
       masters_.emplace_back(std::make_unique<MasterProxyWrapper>(master, this_server));
    }
  }

  Status Call(const std::function<Status(kudu::master::MasterServiceProxy&)>& sender) {
    MasterProxyWrapper* current_leader; 
    RETURN_NOT_OK(CheckLeaderStatus(&current_leader));
    // TODO invalidate if not a leader or network error
    return current_leader->Call(sender);
  }


 private:
  class MasterProxyWrapper {
  public:
    MasterProxyWrapper(const HostPort& address, tserver::TabletServer& server) 
      :address_(address), server_(server) {}

    Status Call(const std::function<Status(kudu::master::MasterServiceProxy&)>& sender) {
      RETURN_NOT_OK(CheckInitialized());    
      return sender(*proxy_);
    }

    Status CheckInitialized() {
      std::lock_guard<std::mutex> guard(lock_);
      if (proxy_)
        return Status::OK();  
      return CreateMasterProxy();
    }

    Status CreateMasterProxy() {
      std::vector<kudu::Sockaddr> addrs;
      RETURN_NOT_OK(server_.dns_resolver()->ResolveAddresses(address_, &addrs));
      CHECK(!addrs.empty());
      if (addrs.size() > 1) {
        LOG(WARNING) << Substitute("Master address '$0' resolves to $1 different addresses. Using $2",
                                  address_.ToString(),
                                  addrs.size(),
                                  addrs[0].ToString());
      }
      proxy_.reset(new MasterServiceProxy(server_.messenger(), addrs[0], address_.host()));
      return Status::OK();
    }
    HostPort address_;
    std::unique_ptr<MasterServiceProxy> proxy_; 
    std::mutex lock_;
    tserver::TabletServer& server_;
  };

  Status CheckLeaderStatus(MasterProxyWrapper** current_leader) {
    std::lock_guard<std::mutex> guard(leader_lookup_lock_);
    if (current_leader_) {
      *current_leader = current_leader_;
    }
    if (std::chrono::steady_clock::now() - last_lookup_time_ < leader_scan_period) {
      return Status::NetworkError("Could not connect to active leader");
    }
    last_lookup_time_ = std::chrono::steady_clock::now();
    // TODO scan for active master
    current_leader_ = masters_.front().get(); 
    *current_leader = current_leader_;
    return Status::OK();     
  }

  void InvalidateCurrentLeader() {
    std::lock_guard<std::mutex> guard(leader_lookup_lock_);
    current_leader_ = nullptr;
  }

  std::vector<std::unique_ptr<MasterProxyWrapper> > masters_;
  MasterProxyWrapper* current_leader_ = nullptr;
  std::mutex leader_lookup_lock_;
  // just set something old as 
  std::chrono::steady_clock::time_point last_lookup_time_ = std::chrono::steady_clock::now() - std::chrono::seconds(3600);
};

namespace {

Status MasterCreateStreamId(PoorMansAsyncClient& client, const std::string& table_id, std::string& stream_id) {
  master::CreateCDCStreamRequestPB req;
  master::CreateCDCStreamResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromMilliseconds(2)); // TODO: timeout???
  req.set_table_id(table_id);
  RETURN_NOT_OK(client.Call([&](kudu::master::MasterServiceProxy& proxy) { return proxy.CreateCDCStream(req, &resp, &rpc); }));
  if (resp.has_error()) {
    return Status::RuntimeError("Something went wrong");
  }
  if (!resp.has_stream_id()) {
    return Status::RuntimeError("SteamId missing.");
  }
  stream_id = resp.stream_id();
  return Status::OK();
}

}  // namespace

// using consensus::LeaderStatus;
using consensus::RaftConsensus;
using rpc::RpcContext;
using tserver::TSTabletManager;

CDCServiceImpl::CDCServiceImpl(tserver::TabletServer* tserver,
                               const std::vector<HostPort>& masters,
                               TSTabletManager* tablet_manager,
                               const scoped_refptr<MetricEntity>& metric_entity,
                               const scoped_refptr<::kudu::rpc::ResultTracker>& result_tracker)
    : CDCServiceIf(metric_entity, result_tracker),
      tserver_(tserver),
      tablet_manager_(tablet_manager),
      client_(new PoorMansAsyncClient(masters , *tserver))
{

}

void CDCServiceImpl::CreateCDCStream(const class CreateCDCStreamRequestPB* req,
                     class CreateCDCStreamResponsePB* resp,
                     ::kudu::rpc::RpcContext* context) {
  if (!CheckOnline(req, resp, context)) {
    return;
  }  
  if (!req->has_table_id()) {
    SetupErrorAndRespond(resp->mutable_error(),
                      Status::InvalidArgument("table_id required"),
                      CDCErrorPB::NOT_RUNNING,
                      context);
    return;
  }

  std::string stream_id;
  if (auto s = MasterCreateStreamId(*client_, req->table_id(), stream_id); !s.ok()) {
    SetupErrorAndRespond(resp->mutable_error(),
                      s,
                      s.IsNotFound() ? CDCErrorPB::TABLE_NOT_FOUND : CDCErrorPB::INTERNAL_ERROR, 
                      context);
    return;
  }
  
  resp->set_stream_id(stream_id);  
  context->RespondSuccess();
}

void CDCServiceImpl::DeleteCDCStream(const class DeleteCDCStreamRequestPB* req,
                     class DeleteCDCStreamResponsePB* resp,
                     ::kudu::rpc::RpcContext* context) {
  if (!CheckOnline(req, resp, context)) {
    return;
  }


}

void CDCServiceImpl::ListTablets(const ListTabletsRequestPB* req,
                                 ListTabletsResponsePB* resp,
                                 RpcContext* context) {
  if (!CheckOnline(req, resp, context)) {
    return;
  }

  // TODO: Add implementation.
  context->RespondSuccess();
}

void CDCServiceImpl::GetChanges(const GetChangesRequestPB* req,
                                GetChangesResponsePB* resp,
                                RpcContext* context) {

  const auto tablet_replica = GetLeaderTabletReplica(req->tablet_id(), resp, context);


  if (!tablet_replica.ok()) {
    return;
  }

   CDCProducer cdc_producer(*tablet_replica);
   Status status = cdc_producer.GetChanges(*req, resp);
   if (PREDICT_FALSE(!status.ok())) {
     // TODO: Map other error statuses to CDCErrorPB.
     SetupErrorAndRespond(
         resp->mutable_error(),
         status,
         status.IsNotFound() ? CDCErrorPB::CHECKPOINT_TOO_OLD : CDCErrorPB::UNKNOWN_ERROR,
         context);
     return;
   }

  context->RespondSuccess();
}

void CDCServiceImpl::GetCheckpoint(const GetCheckpointRequestPB* req,
                                   GetCheckpointResponsePB* resp,
                                   RpcContext* context) {
  if (!CheckOnline(req, resp, context)) {
    return;
  }

  // TODO: Add implementation.
  context->RespondSuccess();
}

}  // namespace cdc
}  // namespace kudu