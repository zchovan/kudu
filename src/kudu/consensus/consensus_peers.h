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
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/locks.h"
#include "kudu/util/make_shared.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

namespace kudu {
class DnsResolver;
class ThreadPoolToken;

namespace rpc {
class Messenger;
class PeriodicTimer;
}

namespace consensus {
class PeerMessageQueue;
class PeerProxy;
class PeerProxyFactory;
class MultiRaftHeartbeatBatcher;
using MultiRaftHeartbeatBatcherPtr = std::shared_ptr<MultiRaftHeartbeatBatcher>;


// A remote peer in consensus.
//
// Leaders use peers to update the remote replicas. Each peer
// may have at most one outstanding request at a time. If a
// request is signaled when there is already one outstanding,
// the request will be generated once the outstanding one finishes.
//
// Peers are owned by the consensus implementation and do not keep
// state aside from the most recent request and response.
//
// Peers are also responsible for sending periodic heartbeats
// to assert liveness of the leader. The peer constructs a heartbeater
// thread to trigger these heartbeats.
//
// The actual request construction is delegated to a PeerMessageQueue
// object, and performed on a thread pool (since it may do IO). When a
// response is received, the peer updates the PeerMessageQueue
// using PeerMessageQueue::ResponseFromPeer(...) on the same thread pool.
class Peer :
    public std::enable_shared_from_this<Peer>,
    public enable_make_shared<Peer> {
 public:
  // Initializes a peer and start sending periodic heartbeats.
  void Init();

  // Signals that this peer has a new request to replicate/store.
  // 'even_if_queue_empty' indicates whether the peer should force
  // send the request even if the queue is empty. This is used for
  // status-only requests.
  Status SignalRequest(bool even_if_queue_empty = false);

  // Starts a leader election on this peer.
  //
  // This method is ad hoc, using this instance's PeerProxy to send the
  // StartElection request.
  //
  // The StartElection RPC does not count as the single outstanding request
  // that this class tracks.
  void StartElection();

  const RaftPeerPB& peer_pb() const { return peer_pb_; }

  // Stop sending requests and periodic heartbeats.
  //
  // This does not block waiting on any current outstanding requests to finish.
  // However, when they do finish, the results will be disregarded, so this
  // is safe to call at any point.
  //
  // This method must be called before the Peer's associated ThreadPoolToken
  // is destructed. Once this method returns, it is safe to destruct
  // the ThreadPoolToken.
  void Close();

  ~Peer();

  // Creates a new remote peer and makes the queue track it.'
  //
  // Requests to this peer (which may end up doing IO to read non-cached
  // log entries) are assembled on 'raft_pool_token'.
  // Response handling may also involve IO related to log-entry lookups and is
  // also done on 'raft_pool_token'.
  //
  // If 'proxy' is nullptr, uses 'peer_proxy_factory' to create and cache a
  // proxy once needed.
  static void NewRemotePeer(RaftPeerPB peer_pb,
                            std::string tablet_id,
                            std::string leader_uuid,
                            PeerMessageQueue* queue,
                            MultiRaftHeartbeatBatcherPtr multi_raft_batcher,
                            ThreadPoolToken* raft_pool_token,
                            PeerProxyFactory* peer_proxy_factory,
                            std::shared_ptr<Peer>* peer);

 protected:
  Peer(RaftPeerPB peer_pb,
       std::string tablet_id,
       std::string leader_uuid,
       PeerMessageQueue* queue,
       MultiRaftHeartbeatBatcherPtr multi_raft_batcher,
       ThreadPoolToken* raft_pool_token,
       PeerProxyFactory* peer_proxy_factory);

 private:
  void SendNextRequest(bool even_if_queue_empty);

  // Signals that a response was received from the peer.
  //
  // This method is called from the reactor thread and calls
  // DoProcessResponse() on raft_pool_token_ to do any work that requires IO or
  // lock-taking.
  void ProcessResponse();

  // Run on 'raft_pool_token'. Does response handling that requires IO or may block.
  void DoProcessResponse();

  // Fetch the desired tablet copy request from the queue and set up
  // tc_request_ appropriately.
  //
  // Returns a bad Status if tablet copy is disabled, or if the
  // request cannot be generated for some reason.
  Status PrepareTabletCopyRequest();

  // Handle RPC callback from initiating tablet copy.
  void ProcessTabletCopyResponse();

  // Signals there was an error sending the request to the peer.
  void ProcessResponseErrorUnlocked(const Status& status);

  // Sets 'proxy_' if needed. Returns 'false' if 'proxy_' is not set and a new
  // proxy could not be created. Otherwise returns 'true'.
  bool CreateProxyIfNeeded();

  const std::string& LogPrefixUnlocked() const;

  const std::string& tablet_id() const { return tablet_id_; }

  const std::string tablet_id_;
  const std::string leader_uuid_;
  const RaftPeerPB peer_pb_;
  const std::string log_prefix_;

  // A factory with which to create peer proxies, in case 'proxy_' is null.
  PeerProxyFactory* peer_proxy_factory_;

  // Taken when setting 'proxy_'.
  simple_spinlock proxy_lock_;

  // May be null if proxy creation failed, in which case further attempts to
  // use this should be preceeded with an attempt to create a new proxy. Once
  // set, should not be unset.
  std::unique_ptr<PeerProxy> proxy_;

  PeerMessageQueue* queue_;
  uint64_t failed_attempts_;

  // The latest consensus update request and response.
  ConsensusRequestPB request_;
  ConsensusResponsePB response_;

  // The latest tablet copy request and response.
  StartTabletCopyRequestPB tc_request_;
  StartTabletCopyResponsePB tc_response_;

  // Reference-counted pointers to any ReplicateMsgs which are in-flight to the peer. We
  // may have loaded these messages from the LogCache, in which case we are potentially
  // sharing the same object as other peers. Since the PB request_ itself can't hold
  // reference counts, this holds them.
  std::vector<ReplicateRefPtr> replicate_msg_refs_;

  rpc::RpcController controller_;

  std::shared_ptr<rpc::Messenger> messenger_;

  // Thread pool token used to construct requests to this peer.
  //
  // RaftConsensus owns this shared token and is responsible for destroying it.
  ThreadPoolToken* raft_pool_token_;

  // Batcher that currently batches heartbeat requests that are sent by each consensus peer
  // on a per tserver level
  MultiRaftHeartbeatBatcherPtr multi_raft_batcher_;

  // Repeating timer responsible for scheduling heartbeats to this peer.
  std::shared_ptr<rpc::PeriodicTimer> heartbeater_;

  // Lock that protects Peer state changes, initialization, etc. It's necessary
  // to hold 'peer_lock_' if setting 'request_pending_', 'closed_', and
  // 'has_sent_first_request_' fields below. To read 'has_sent_first_request_',
  // it's necessary to hold 'peer_lock_'. To read the 'closed_' and
  // 'request_pending_' fields, there is no need to hold 'peer_lock_' unless
  // it's necessary to block the threads which might be setting these fields
  // concurrently.
  simple_spinlock peer_lock_;

  std::atomic<bool> request_pending_;
  std::atomic<bool> closed_;
  bool has_sent_first_request_;
};

// A proxy to another peer. Usually a thin wrapper around an rpc proxy but can
// be replaced for tests.
class PeerProxy {
 public:
  virtual ~PeerProxy() = default;

  // Sends a request, asynchronously, to a remote peer.
  virtual void UpdateAsync(const ConsensusRequestPB& request,
                           ConsensusResponsePB* response,
                           rpc::RpcController* controller,
                           const rpc::ResponseCallback& callback) = 0;

  // Asks a peer to vote for a candidate.
  virtual void RequestConsensusVoteAsync(const VoteRequestPB& request,
                                         VoteResponsePB* response,
                                         rpc::RpcController* controller,
                                         const rpc::ResponseCallback& callback) = 0;

  // Instructs a peer to kick off an election to elect itself leader.
  virtual void StartElectionAsync(const RunLeaderElectionRequestPB& request,
                                  RunLeaderElectionResponsePB* response,
                                  rpc::RpcController* controller,
                                  const rpc::ResponseCallback& callback) = 0;

  // Instructs a peer to begin a tablet copy session.
  virtual void StartTabletCopyAsync(const StartTabletCopyRequestPB& /*request*/,
                                    StartTabletCopyResponsePB* /*response*/,
                                    rpc::RpcController* /*controller*/,
                                    const rpc::ResponseCallback& /*callback*/) {
    LOG(DFATAL) << "Not implemented";
  }

  // Remote endpoint or description of the peer.
  virtual std::string PeerName() const = 0;
};

// A peer proxy factory. Usually just obtains peers through the rpc implementation
// but can be replaced for tests.
class PeerProxyFactory {
 public:
  virtual ~PeerProxyFactory() = default;

  virtual Status NewProxy(const RaftPeerPB& peer_pb,
                          std::unique_ptr<PeerProxy>* proxy) = 0;

  virtual const std::shared_ptr<rpc::Messenger>& messenger() const = 0;
};

// PeerProxy implementation that does RPC calls
class RpcPeerProxy : public PeerProxy {
 public:
  RpcPeerProxy(HostPort hostport,
               std::unique_ptr<ConsensusServiceProxy> consensus_proxy);

  void UpdateAsync(const ConsensusRequestPB& request,
                   ConsensusResponsePB* response,
                   rpc::RpcController* controller,
                   const rpc::ResponseCallback& callback) override;

  void RequestConsensusVoteAsync(const VoteRequestPB& request,
                                 VoteResponsePB* response,
                                 rpc::RpcController* controller,
                                 const rpc::ResponseCallback& callback) override;

  void StartElectionAsync(const RunLeaderElectionRequestPB& request,
                          RunLeaderElectionResponsePB* response,
                          rpc::RpcController* controller,
                          const rpc::ResponseCallback& callback) override;

  void StartTabletCopyAsync(const StartTabletCopyRequestPB& request,
                            StartTabletCopyResponsePB* response,
                            rpc::RpcController* controller,
                            const rpc::ResponseCallback& callback) override;

  std::string PeerName() const override;

 private:
  const HostPort hostport_;
  std::unique_ptr<ConsensusServiceProxy> consensus_proxy_;
};

// PeerProxyFactory implementation that generates RPCPeerProxies
class RpcPeerProxyFactory : public PeerProxyFactory {
 public:
  RpcPeerProxyFactory(std::shared_ptr<rpc::Messenger> messenger,
                      DnsResolver* dns_resolver);
  ~RpcPeerProxyFactory() = default;

  Status NewProxy(const RaftPeerPB& peer_pb,
                  std::unique_ptr<PeerProxy>* proxy) override;

  const std::shared_ptr<rpc::Messenger>& messenger() const override {
    return messenger_;
  }

 private:
  std::shared_ptr<rpc::Messenger> messenger_;
  DnsResolver* dns_resolver_;
};

// Query the consensus service at last known host/port that is
// specified in 'remote_peer' and set the 'permanent_uuid' field based
// on the response.
Status SetPermanentUuidForRemotePeer(
    const std::shared_ptr<rpc::Messenger>& messenger,
    DnsResolver* resolver,
    RaftPeerPB* remote_peer);

}  // namespace consensus
}  // namespace kudu
