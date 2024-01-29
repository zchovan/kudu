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
#include <functional>
#include <iosfwd>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta.h"  // IWYU pragma: keep
#include "kudu/consensus/consensus_queue.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/multi_raft_batcher.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/locks.h"
#include "kudu/util/make_shared.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/status_callback.h"

namespace kudu {

typedef std::lock_guard<simple_spinlock> Lock;
typedef std::unique_ptr<Lock> ScopedLock;

class Status;
class ThreadPool;
class ThreadPoolToken;

namespace rpc {
class PeriodicTimer;
}

namespace consensus {

class ConsensusMetadataManager;
class ConsensusRound;
class ConsensusRoundHandler;
class PeerManager;
class PeerProxyFactory;
class PendingRounds;
class TimeManager;
struct ConsensusBootstrapInfo;
struct ElectionResult;

// Context containing resources shared by the Raft consensus instances on a
// single server.
struct ServerContext {
  // Shared boolean that indicates whether the server is quiescing, in which
  // case this replica should not attempt to become leader.
  std::atomic<bool>* quiescing;

  // Gauge indicating how many Raft tablet leaders are hosted on the server.
  scoped_refptr<AtomicGauge<int32_t>> num_leaders;

  // Threadpool on which to run Raft tasks.
  ThreadPool* raft_pool;

  // Shared boolean indicating whether Raft consensus should continue sending request messages
  // even if a peer is considered as failed.
  const bool* allow_status_msg_for_failed_peer = nullptr;
};

struct ConsensusOptions {
  std::string tablet_id;
};

struct TabletVotingState {
  std::optional<OpId> tombstone_last_logged_opid_;
  tablet::TabletDataState data_state_;
  TabletVotingState(std::optional<OpId> tombstone_last_logged_opid,
                    tablet::TabletDataState data_state)
          : tombstone_last_logged_opid_(std::move(tombstone_last_logged_opid)),
            data_state_(data_state) {}
};

typedef int64_t ConsensusTerm;
typedef StatusCallback ConsensusReplicatedCallback;
typedef std::function<void(const std::string& reason)> MarkDirtyCallback;

class RaftConsensus : public std::enable_shared_from_this<RaftConsensus>,
                      public enable_make_shared<RaftConsensus>,
                      public PeerMessageQueueObserver {
 public:

  // Modes for StartElection().
  enum ElectionMode {
    // A normal leader election. Peers will not vote for this node
    // if they believe that a leader is alive.
    NORMAL_ELECTION,

    // A "pre-election". Peers will vote as they would for a normal
    // election, except that the votes will not be "binding". In other
    // words, they will not durably record their vote.
    PRE_ELECTION,

    // In this mode, peers will vote for this candidate even if they
    // think a leader is alive. This can be used for a faster hand-off
    // between a leader and one of its replicas.
    ELECT_EVEN_IF_LEADER_IS_ALIVE
  };

  // Reasons for StartElection().
  enum ElectionReason {
    // The election is being called because the Raft configuration has only
    // a single node and has just started up.
    INITIAL_SINGLE_NODE_ELECTION,

    // The election is being called because the timeout expired. In other
    // words, the previous leader probably failed (or there was no leader
    // in this term)
    ELECTION_TIMEOUT_EXPIRED,

    // The election is being started because of an explicit external request.
    EXTERNAL_REQUEST
  };

  typedef std::pair<RaftPeerPB::Role, RaftPeerPB::MemberType> RoleAndMemberType;

  ~RaftConsensus();

  // Factory method to construct and initialize a RaftConsensus instance.
  static Status Create(ConsensusOptions options,
                       RaftPeerPB local_peer_pb,
                       scoped_refptr<ConsensusMetadataManager> cmeta_manager,
                       ServerContext server_ctx,
                       std::shared_ptr<RaftConsensus>* consensus_out);

  // Starts running the Raft consensus algorithm.
  // Start() is not thread-safe. Calls to Start() should be externally
  // synchronized with calls accessing non-const members of this class.
  Status Start(const ConsensusBootstrapInfo& info,
               std::unique_ptr<PeerProxyFactory> peer_proxy_factory,
               scoped_refptr<log::Log> log,
               std::unique_ptr<TimeManager> time_manager,
               ConsensusRoundHandler* round_handler,
               const scoped_refptr<MetricEntity>& metric_entity,
               MultiRaftManager* multi_raft_manager,
               MarkDirtyCallback cb);

  // Returns true if RaftConsensus is running.
  bool IsRunning() const;

  // Emulates an election by increasing the term number and asserting leadership
  // in the configuration by sending a NO_OP to other peers.
  // This is NOT safe to use in a distributed configuration with failure detection
  // enabled, as it could result in a split-brain scenario.
  Status EmulateElectionForTests();

  // Triggers a leader election.
  Status StartElection(ElectionMode mode, ElectionReason reason);

  // Wait until the node has LEADER role.
  // Returns Status::TimedOut if the role is not LEADER within 'timeout'.
  // NOTE: the implementation is a busy loop; as such, this method should be
  // used sparingly, e.g. only in tests, or in applications that don't require
  // high concurrency.
  Status WaitUntilLeader(const MonoDelta& timeout) WARN_UNUSED_RESULT;

  // Return a copy of the failure detector instance. Only for use in tests.
  std::shared_ptr<rpc::PeriodicTimer> GetFailureDetectorForTests() const {
    return failure_detector_;
  }

  // Performs an abrupt leader step down. This node, if the leader, becomes a
  // follower immediately and sleeps its failure detector for an extra election
  // timeout to decrease its chances of being reelected.
  Status StepDown(LeaderStepDownResponsePB* resp);

  // Attempts to gracefully transfer leadership to the peer with uuid
  // 'new_leader_uuid' or to the next up-to-date peer the leader gets
  // a response from if 'new_leader_uuid' is std::nullopt. To allow peers time
  // to catch up, the leader will not accept write or config change requests
  // during a 'transfer period' that lasts one election timeout. If no
  // successor is eligible by the end of the transfer period, leadership
  // transfer fails and the leader resumes normal operation. The transfer is
  // asynchronous: once the transfer period is started the method returns
  // success.
  // Additional calls to this method during the transfer period prolong it.
  Status TransferLeadership(const std::optional<std::string>& new_leader_uuid,
                            LeaderStepDownResponsePB* resp);

  // Begin or end a leadership transfer period. During a transfer period, a
  // leader will not accept writes or config changes, but will continue updating
  // followers. If a leader transfer period is already in progress,
  // BeginLeaderTransferPeriodUnlocked returns ServiceUnavailable.
  Status BeginLeaderTransferPeriodUnlocked(
      const std::optional<std::string>& successor_uuid);
  void EndLeaderTransferPeriod();

  // Creates a new ConsensusRound, the entity that owns all the data
  // structures required for a consensus round, such as the ReplicateMsg
  // (and later on the CommitMsg). ConsensusRound will also point to and
  // increase the reference count for the provided callbacks.
  scoped_refptr<ConsensusRound> NewRound(
      std::unique_ptr<ReplicateMsg> replicate_msg,
      ConsensusReplicatedCallback replicated_cb);

  // Called by a Leader to replicate an entry to the state machine.
  //
  // From the leader instance perspective execution proceeds as follows:
  //
  //           Leader                               RaftConfig
  //             +                                     +
  //     1) Req->| Replicate()                         |
  //             |                                     |
  //     2)      +-------------replicate-------------->|
  //             |<---------------ACK------------------+
  //             |                                     |
  //     3)      +--+                                  |
  //           <----+ round.NotifyReplicationFinished()|
  //             |                                     |
  //     3a)     |  +------ update commitIndex ------->|
  //             |                                     |
  //
  // 1) Caller calls Replicate(), method returns immediately to the caller and
  //    runs asynchronously.
  //
  // 2) Leader replicates the entry to the peers using the consensus
  //    algorithm, proceeds as soon as a majority of voters acknowledges the
  //    entry.
  //
  // 3) Leader defers to the caller by calling ConsensusRound::NotifyReplicationFinished,
  //    which calls the ConsensusReplicatedCallback.
  //
  // 3a) The leader asynchronously notifies other peers of the new
  //     commit index, which tells them to apply the operation.
  //
  // This method can only be called on the leader, i.e. role() == LEADER
  Status Replicate(const scoped_refptr<ConsensusRound>& round);

  // Ensures that the consensus implementation is currently acting as LEADER,
  // and thus is allowed to submit operations to be prepared before they are
  // replicated. To avoid a time-of-check-to-time-of-use (TOCTOU) race, the
  // implementation also stores the current term inside the round's "bound_term"
  // member. When we eventually are about to replicate the op, we verify
  // that the term has not changed in the meantime.
  Status CheckLeadershipAndBindTerm(const scoped_refptr<ConsensusRound>& round);

  // Messages sent from LEADER to FOLLOWERS and LEARNERS to update their
  // state machines. This is equivalent to "AppendEntries()" in Raft
  // terminology.
  //
  // ConsensusRequestPB contains a sequence of 0 or more operations to apply
  // on the replica. If there are 0 operations the request is considered
  // 'status-only' i.e. the leader is communicating with the follower only
  // in order to pass back and forth information on watermarks (eg committed
  // operation ID, replicated op id, etc).
  //
  // If the sequence contains 1 or more operations they will be replicated
  // in the same order as the leader, and submitted for asynchronous Prepare
  // in the same order.
  //
  // The leader also provides information on the index of the latest
  // operation considered committed by consensus. The replica uses this
  // information to update the state of any pending (previously replicated/prepared)
  // ops.
  //
  // Returns Status::OK if the response has been filled (regardless of accepting
  // or rejecting the specific request). Returns non-OK Status if a specific
  // error response could not be formed, which will result in the service
  // returning an UNKNOWN_ERROR RPC error code to the caller and including the
  // stringified Status message.
  Status Update(const ConsensusRequestPB* request,
                ConsensusResponsePB* response);

  // Messages sent from CANDIDATEs to voting peers to request their vote
  // in leader election.
  //
  // If 'tombstone_last_logged_opid' is set, this replica will attempt to vote
  // in kInitialized and kStopped states, instead of just in the kRunning
  // state.
  Status RequestVote(const VoteRequestPB* request,
                     TabletVotingState tablet_voting_state,
                     VoteResponsePB* response);

  // Implement a ChangeConfig() request.
  Status ChangeConfig(const ChangeConfigRequestPB& req,
                      StatusCallback client_cb,
                      std::optional<tserver::TabletServerErrorPB::Code>* error_code);

  // Implement a BulkChangeConfig() request.
  Status BulkChangeConfig(const BulkChangeConfigRequestPB& req,
                          StatusCallback client_cb,
                          std::optional<tserver::TabletServerErrorPB::Code>* error_code);

  // Implement an UnsafeChangeConfig() request.
  Status UnsafeChangeConfig(const UnsafeChangeConfigRequestPB& req,
                            std::optional<tserver::TabletServerErrorPB::Code>* error_code);

  // Returns the last OpId (either received or committed, depending on the
  // 'type' argument) that the Consensus implementation knows about.
  // Returns std::nullopt if RaftConsensus was not properly initialized.
  std::optional<OpId> GetLastOpId(OpIdType type);

  // Returns the current Raft role of this instance.
  RaftPeerPB::Role role() const;

  // Returns the current Raft role and member type of this instance.
  // May return <UNKNOWN_ROLE, UNKNOWN_MEMBER_TYPE> if the information is not available.
  RoleAndMemberType GetRoleAndMemberType() const;

  // Returns the current term.
  int64_t CurrentTerm() const;

  // Returns the uuid of this peer.
  // Thread-safe.
  const std::string& peer_uuid() const;

  // Returns the id of the tablet whose updates this consensus instance helps coordinate.
  // Thread-safe.
  const std::string& tablet_id() const;

  TimeManager* time_manager() const { return time_manager_.get(); }

  // Returns a copy of the state of the consensus system.
  // If 'report_health' is set to 'INCLUDE_HEALTH_REPORT', and if the
  // local replica believes it is the leader of the config, it will include a
  // health report about each active peer in the committed config.
  // If RaftConsensus has been shut down, returns Status::IllegalState.
  // Does not modify the out-param 'cstate' unless an OK status is returned.
  Status ConsensusState(ConsensusStatePB* cstate,
                        IncludeHealthReport report_health = EXCLUDE_HEALTH_REPORT) const;

  // Returns a copy of the current committed Raft configuration.
  RaftConfigPB CommittedConfig() const;

  void DumpStatusHtml(std::ostream& out) const;

  // Transition to kStopped state. See State enum definition for details.
  // This is a no-op if the tablet is already in kStopped or kShutdown state;
  // otherwise, Raft will pass through the kStopping state on the way to
  // kStopped.
  void Stop();

  // Transition to kShutdown state. See State enum definition for details.
  // It is legal to call this method while in any lifecycle state.
  void Shutdown();

  // Makes this peer advance it's term (and step down if leader), for tests.
  Status AdvanceTermForTests(int64_t new_term);

  int update_calls_for_tests() const {
    return update_calls_for_tests_;
  }

  //------------------------------------------------------------
  // PeerMessageQueueObserver implementation
  //------------------------------------------------------------

  // Updates the committed_index and triggers the Apply()s for whatever
  // ops were pending.
  // This is idempotent.
  void NotifyCommitIndex(int64_t commit_index) override;

  void NotifyTermChange(int64_t term) override;

  void NotifyFailedFollower(const std::string& uuid,
                            int64_t term,
                            const std::string& reason) override;

  void NotifyPeerToPromote(const std::string& peer_uuid) override;

  void NotifyPeerToStartElection(const std::string& peer_uuid) override;

  void NotifyPeerHealthChange() override;

  // Return the log indexes which the consensus implementation would like to retain.
  //
  // The returned 'for_durability' index ensures that no logs are GCed before
  // the operation is fully committed. The returned 'for_peers' index indicates
  // the index of the farthest-behind peer so that the log will try to avoid
  // GCing these before the peer has caught up.
  log::RetentionIndexes GetRetentionIndexes();

  // Return the on-disk size of the consensus metadata, in bytes.
  int64_t MetadataOnDiskSize() const;

  int64_t GetMillisSinceLastLeaderHeartbeat() const;

 protected:
  RaftConsensus(ConsensusOptions options,
                RaftPeerPB local_peer_pb,
                scoped_refptr<ConsensusMetadataManager> cmeta_manager,
                ServerContext server_ctx);

 private:
  friend class RaftConsensusQuorumTest;
  FRIEND_TEST(RaftConsensusQuorumTest, TestConsensusContinuesIfAMinorityFallsBehind);
  FRIEND_TEST(RaftConsensusQuorumTest, TestConsensusStopsIfAMajorityFallsBehind);
  FRIEND_TEST(RaftConsensusQuorumTest, TestLeaderElectionWithQuiescedQuorum);
  FRIEND_TEST(RaftConsensusQuorumTest, TestReplicasEnforceTheLogMatchingProperty);
  FRIEND_TEST(RaftConsensusQuorumTest, TestRequestVote);

  // RaftConsensus lifecycle states.
  //
  // Legal state transitions:
  //
  //   kNew -> kInitialized -+-> kRunning -> kStopping -> kStopped -> kShutdown
  //                          `----------------^
  //
  // NOTE: When adding / changing values in this enum, add the corresponding
  // values to State_Name() as well.
  //
  enum State {
    // The RaftConsensus object has been freshly constructed and is not yet
    // initialized. A RaftConsensus object will never be made externally
    // visible in this state.
    kNew,

    // Raft has been initialized. It cannot accept writes, but it may be able
    // to vote. See RequestVote() for details.
    kInitialized,

    // Raft is running normally and will accept write requests and vote
    // requests.
    kRunning,

    // Raft is in the process of stopping and will not accept writes. Voting
    // may still be allowed. See RequestVote() for details.
    kStopping,

    // Raft is stopped and no longer accepting writes. However, voting may
    // still be allowed; See RequestVote() for details.
    kStopped,

    // Raft is fully shut down and cannot accept writes or vote requests.
    kShutdown,
  };

  // Enum for the 'flush' argument to SetCurrentTermUnlocked() below.
  enum FlushToDisk {
    SKIP_FLUSH_TO_DISK,
    FLUSH_TO_DISK,
  };

  // Helper struct that contains the messages from the leader that we need to
  // append to our log, after they've been deduplicated.
  struct LeaderRequest {
    std::string leader_uuid;
    const OpId* preceding_opid;
    std::vector<ReplicateRefPtr> messages;
    // The positional index of the first message selected to be appended, in the
    // original leader's request message sequence.
    int64_t first_message_idx;

    std::string OpsRangeString() const;
  };

  using LockGuard = std::lock_guard<simple_spinlock>;
  using UniqueLock = std::unique_lock<simple_spinlock>;

  // Returns string description for State enum value.
  static const char* State_Name(State state);

  // Return the minimum election timeout. Due to backoff and random
  // jitter, election timeouts may be longer than this.
  static MonoDelta MinimumElectionTimeout();

  // Initializes the RaftConsensus object, including loading the consensus
  // metadata.
  Status Init();

  // Change the lifecycle state of RaftConsensus. The definition of the State
  // enum documents legal state transitions.
  void SetStateUnlocked(State new_state);

  // Set the leader UUID of the configuration and mark the tablet config dirty for
  // reporting to the master.
  void SetLeaderUuidUnlocked(const std::string& uuid);

  // Replicate (as leader) a config change. This includes validating the new
  // config and updating the peers and setting the new_configuration as pending.
  // The old_configuration must be the currently-committed configuration.
  Status ReplicateConfigChangeUnlocked(
      RaftConfigPB old_config,
      RaftConfigPB new_config,
      StatusCallback client_cb);

  // Update the peers and queue to be consistent with a new active configuration.
  // Should only be called by the leader.
  void RefreshConsensusQueueAndPeersUnlocked();

  // Makes the peer become leader.
  // Returns OK once the change config op that has this peer as leader
  // has been enqueued, the op will complete asynchronously.
  //
  // 'lock_' must be held for configuration change before calling.
  Status BecomeLeaderUnlocked();

  // Makes the peer become a replica, i.e. a FOLLOWER or a LEARNER.
  // See EnableFailureDetector() for description of the 'fd_delta' parameter.
  //
  // 'lock_' must be held for configuration change before calling.
  Status BecomeReplicaUnlocked(std::optional<MonoDelta> fd_delta = std::nullopt);

  // Updates the state in a replica by storing the received operations in the log
  // and triggering the required ops. This method won't return until all
  // operations have been stored in the log and all Prepares() have been completed,
  // and a replica cannot accept any more Update() requests until this is done.
  Status UpdateReplica(const ConsensusRequestPB* request,
                       ConsensusResponsePB* response);

  // Deduplicates an RPC request making sure that we get only messages that we
  // haven't appended to our log yet.
  // On return 'deduplicated_req' is instantiated with only the new messages
  // and the correct preceding id.
  void DeduplicateLeaderRequestUnlocked(const ConsensusRequestPB* rpc_req,
                                        LeaderRequest* deduplicated_req);

  // Handles a request from a leader, refusing the request if the term is lower than
  // ours or stepping down if it's higher.
  Status HandleLeaderRequestTermUnlocked(const ConsensusRequestPB* request,
                                         ConsensusResponsePB* response);

  // Checks that the preceding op in 'req' is locally committed or pending and sets an
  // appropriate error message in 'response' if not.
  // If there is term mismatch between the preceding op id in 'req' and the local log's
  // pending operations, we proactively abort those pending operations after and including
  // the preceding op in 'req' to avoid a pointless cache miss in the leader's log cache.
  Status EnforceLogMatchingPropertyMatchesUnlocked(const LeaderRequest& req,
                                                   ConsensusResponsePB* response)
         WARN_UNUSED_RESULT;

  // Check a request received from a leader, making sure:
  // - The request is in the right term
  // - The log matching property holds
  // - Messages are de-duplicated so that we only process previously unprocessed requests.
  // - We abort ops if the leader sends ops that have the same index as ops
  //   currently on the pendings set, but different terms.
  // If this returns ok and the response has no errors, 'deduped_req' is set with only
  // the messages to add to our state machine.
  Status CheckLeaderRequestUnlocked(const ConsensusRequestPB* request,
                                    ConsensusResponsePB* response,
                                    LeaderRequest* deduped_req) WARN_UNUSED_RESULT;

  // Abort any pending operations after the given op index,
  // and also truncate the LogCache accordingly.
  void TruncateAndAbortOpsAfterUnlocked(int64_t truncate_after_index);

  // Begin a replica op. If the type of message in 'msg' is not a type
  // that uses ops, delegates to StartConsensusOnlyRoundUnlocked().
  Status StartFollowerOpUnlocked(const ReplicateRefPtr& msg);

  // Returns true if this node is the only voter in the Raft configuration.
  bool IsSingleVoterConfig() const;

  // Return header string for RequestVote log messages, no 'lock_' is necessary.
  std::string GetRequestVoteLogPrefixThreadSafe(const VoteRequestPB& request) const;

  // Similar to the method above, but outputs more detailed information on the
  // metadata of the RaftConsensus object. 'lock_' must be held.
  std::string GetRequestVoteLogPrefixUnlocked(const VoteRequestPB& request) const;

  // Fills the response with the current status, if an update was successful.
  void FillConsensusResponseOKUnlocked(ConsensusResponsePB* response);

  // Fills the response with an error code and error message.
  void FillConsensusResponseError(ConsensusResponsePB* response,
                                  ConsensusErrorPB::Code error_code,
                                  const Status& status);

  // Fill VoteResponsePB with the following information:
  // - Update responder_term to current local term.
  // - Set vote_granted to true.
  void FillVoteResponseVoteGranted(VoteResponsePB* response);

  // Enum for the 'responder_term' parameter of the FillVoterResponseVoteDenied()
  // method below. Controls whether to populate the 'responder_term' field
  // in the 'response' output parameter.
  enum class ResponderTermPolicy {
    DO_NOT_SET,  // don't set the field
    SET,          // populate/set the field
  };

  // Fill VoteResponsePB with the following information:
  // - Set vote_granted to false.
  // - Set consensus_error.code to the given code.
  // - Set or leave the responder_term field unset as prescribed by the
  //   'responder_term' parameter.
  void FillVoteResponseVoteDenied(
      ConsensusErrorPB::Code error_code,
      VoteResponsePB* response,
      ResponderTermPolicy responder_term_policy = ResponderTermPolicy::SET);

  // Respond to VoteRequest that the candidate has an old term.
  Status RequestVoteRespondInvalidTerm(const VoteRequestPB* request, VoteResponsePB* response);

  // Respond to VoteRequest that we already granted our vote to the candidate.
  Status RequestVoteRespondVoteAlreadyGranted(const VoteRequestPB* request,
                                              VoteResponsePB* response);

  // Respond to VoteRequest that we already granted our vote to someone else.
  Status RequestVoteRespondAlreadyVotedForOther(const VoteRequestPB* request,
                                                VoteResponsePB* response);

  // Respond to VoteRequest that the candidate's last-logged OpId is too old.
  Status RequestVoteRespondLastOpIdTooOld(const OpId& local_last_logged_opid,
                                          const VoteRequestPB* request,
                                          VoteResponsePB* response);

  // Respond to VoteRequest that the vote was not granted because we believe
  // the leader to be alive.
  Status RequestVoteRespondLeaderIsAlive(const VoteRequestPB* request,
                                         VoteResponsePB* response);

  // Respond to VoteRequest that the replica is already in the middle of servicing
  // another vote request or an update from a valid leader.
  Status RequestVoteRespondIsBusy(const VoteRequestPB* request,
                                  VoteResponsePB* response);

  // Respond to VoteRequest that the vote is granted for candidate.
  Status RequestVoteRespondVoteGranted(const VoteRequestPB* request,
                                       VoteResponsePB* response);

  // Callback for leader election driver. ElectionCallback is run on the
  // reactor thread, so it simply defers its work to DoElectionCallback.
  void ElectionCallback(ElectionReason reason, const ElectionResult& result);
  void DoElectionCallback(ElectionReason reason, const ElectionResult& result);

  // Starts tracking the leader for failures. This occurs at startup, when a
  // local peer transitions from LEADER to FOLLOWER or from NON_VOTER to VOTER,
  // or after a failed election.
  //
  // If the failure detector is "snoozed" (see SnoozeFailureDetector()), it
  // means some leader activity was observed and the failure detection period
  // should be reset.
  //
  // If 'delta' is set, it is used as the initial period for leader failure
  // detection. Otherwise, the minimum election timeout is used.
  //
  // If the failure detector is already enabled, this has no effect.
  void EnableFailureDetector(std::optional<MonoDelta> delta = std::nullopt);

  // Stops tracking the leader for failures. This occurs when a local peer
  // transitions from FOLLOWER to LEADER or from VOTER to NON_VOTER.
  //
  // If the failure detector is already disabled, this has no effect.
  void DisableFailureDetector();

  // Enables or disables the failure detector based on the role of the local
  // peer in the active config. If the local peer a VOTER, but not the leader,
  // then failure detection will be enabled. If the local peer is the leader,
  // or a NON_VOTER, then failure detection will be disabled.
  //
  // See EnableFailureDetector() for an explanation of the 'delta' parameter,
  // which is used if it is determined that the failure detector should be
  // enabled.
  void UpdateFailureDetectorState(std::optional<MonoDelta> delta = std::nullopt);

  // "Reset" the failure detector to indicate leader activity.
  //
  // When this is called a failure is guaranteed not to be detected before
  // 'FLAGS_leader_failure_max_missed_heartbeat_periods' *
  // 'FLAGS_raft_heartbeat_interval_ms' has elapsed, unless 'delta' is set, in
  // which case its value is used as the next failure period.
  //
  // If 'reason_for_log' is set, then this method will print a log message when called.
  //
  // If the failure detector is unregistered, has no effect.
  void SnoozeFailureDetector(std::optional<std::string> reason_for_log = std::nullopt,
                             std::optional<MonoDelta> delta = std::nullopt);

  // Update the voting withhold interval, bumping it up for the minimum
  // election timeout interval, i.e. 'FLAGS_raft_heartbeat_interval_ms' *
  // 'FLAGS_leader_failure_max_missed_heartbeat_periods' milliseconds.
  // This method is safe to call even it's a leader replica.
  void WithholdVotes();

  // Calculates a snooze delta for leader election.
  //
  // The delta increases exponentially with the difference between the current
  // term and the term of the last committed operation.
  //
  // The maximum delta is capped by 'FLAGS_leader_failure_exp_backoff_max_delta_ms'.
  MonoDelta LeaderElectionExpBackoffDeltaUnlocked();

  // Handle when the term has advanced beyond the current term.
  //
  // 'flush' may be used to control whether the term change is flushed to disk.
  Status HandleTermAdvanceUnlocked(ConsensusTerm new_term,
                                   FlushToDisk flush = FLUSH_TO_DISK);

  // Asynchronously (on thread_pool_) notify the TabletReplica that the consensus configuration
  // has changed, thus reporting it back to the master.
  void MarkDirty(const std::string& reason);

  // Calls MarkDirty() if 'status' == OK. Then, always calls 'client_cb' with
  // 'status' as its argument.
  void MarkDirtyOnSuccess(const std::string& reason,
                          const StatusCallback& client_cb,
                          const Status& status);

  // Attempt to remove the follower with the specified 'uuid' from the config,
  // if the 'committed_config' is still the committed config and if the current
  // node is the leader.
  //
  // Since this is inherently an asynchronous operation run on a thread pool,
  // it may fail due to the configuration changing, the local node losing
  // leadership, or the tablet shutting down.
  // Logs a warning on failure.
  void TryRemoveFollowerTask(const std::string& uuid,
                             const RaftConfigPB& committed_config,
                             const std::string& reason);

  // Attempt to promote the given non-voter to a voter.
  void TryPromoteNonVoterTask(const std::string& peer_uuid);

  void TryStartElectionOnPeerTask(const std::string& peer_uuid);

  // Called when the failure detector expires.
  // Submits ReportFailureDetectedTask() to a thread pool.
  void ReportFailureDetected();

  // Call StartElection(), log a warning if the call fails (usually due to
  // being shut down).
  void ReportFailureDetectedTask();

  // Handle the completion of replication of a config change operation.
  // If 'status' is OK, this takes care of persisting the new configuration
  // to disk as the committed configuration. A non-OK status indicates that
  // the replication failed, in which case the pending configuration needs
  // to be cleared such that we revert back to the old configuration.
  void CompleteConfigChangeRoundUnlocked(ConsensusRound* round,
                                         const Status& status);

  // Trigger that a no-op ConsensusRound has finished replication.
  // If the replication was successful, an status will be OK. Otherwise, it
  // may be Aborted or some other error status.
  // If 'status' is OK, write a Commit message to the local WAL based on the
  // type of message it is.
  // The 'client_cb' will be invoked at the end of this execution.
  //
  // NOTE: Must be called while holding 'lock_'.
  void NonTxRoundReplicationFinished(ConsensusRound* round,
                                     const StatusCallback& client_cb,
                                     const Status& status);

  // As a leader, append a new ConsensusRound to the queue.
  Status AppendNewRoundToQueueUnlocked(const scoped_refptr<ConsensusRound>& round);

  // As a follower, start a consensus round not associated with an op.
  Status StartConsensusOnlyRoundUnlocked(const ReplicateRefPtr& msg);

  // Add a new pending operation to PendingRounds, including the special handling
  // necessary if this round contains a configuration change. These rounds must
  // take effect as soon as they are received, rather than waiting for commitment
  // (see Diego Ongaro's thesis section 4.1).
  Status AddPendingOperationUnlocked(const scoped_refptr<ConsensusRound>& round);

  // Checks that the replica is in the appropriate state and role to replicate
  // the provided operation and that the replicate message does not yet have an
  // OpId assigned.
  Status CheckSafeToReplicateUnlocked(const ReplicateMsg& msg) const WARN_UNUSED_RESULT;

  // Return Status::IllegalState if 'state_' != kRunning, OK otherwise.
  Status CheckRunningUnlocked() const WARN_UNUSED_RESULT;

  // Ensure the local peer is the active leader.
  // Returns OK if leader, IllegalState otherwise.
  Status CheckActiveLeaderUnlocked() const WARN_UNUSED_RESULT;

  // Returns OK if there is currently *no* configuration change pending, and
  // IllegalState is there *is* a configuration change pending.
  Status CheckNoConfigChangePendingUnlocked() const WARN_UNUSED_RESULT;

  // Sets the given configuration as pending commit. Does not persist into the peers
  // metadata. In order to be persisted, SetCommittedConfigUnlocked() must be called.
  Status SetPendingConfigUnlocked(const RaftConfigPB& new_config) WARN_UNUSED_RESULT;

  // Changes the committed config for this replica. Checks that there is a
  // pending configuration and that it is equal to this one. Persists changes to disk.
  // Resets the pending configuration to null.
  Status SetCommittedConfigUnlocked(const RaftConfigPB& config_to_commit);

  // Checks if the term change is legal. If so, sets 'current_term'
  // to 'new_term' and sets 'has voted' to no for the current term.
  //
  // If the caller knows that it will call another method soon after
  // to flush the change to disk, it may set 'flush' to 'SKIP_FLUSH_TO_DISK'.
  Status SetCurrentTermUnlocked(int64_t new_term,
                                FlushToDisk flush) WARN_UNUSED_RESULT;

  // Returns the term set in the last config change round.
  int64_t CurrentTermUnlocked() const;

  // Accessors for the leader of the current term.
  std::string GetLeaderUuidUnlocked() const;
  bool HasLeaderUnlocked() const;
  void ClearLeaderUnlocked();

  // Return whether this peer has voted in the current term.
  bool HasVotedCurrentTermUnlocked() const;

  // Record replica's vote for the current term, then flush the consensus
  // metadata to disk.
  Status SetVotedForCurrentTermUnlocked(const std::string& uuid) WARN_UNUSED_RESULT;

  // Return replica's vote for the current term.
  // The vote must be set; use HasVotedCurrentTermUnlocked() to check.
  const std::string& GetVotedForCurrentTermUnlocked() const;

  const ConsensusOptions& GetOptions() const;

  // See GetLastOpId().
  std::optional<OpId> GetLastOpIdUnlocked(OpIdType type);

  std::string LogPrefix() const;
  std::string LogPrefixUnlocked() const;

  // A variant of LogPrefix which does not take the lock. This is a slightly
  // less thorough prefix which only includes immutable (and thus thread-safe)
  // information, but does not require the lock.
  const std::string& LogPrefixThreadSafe() const {
    return log_prefix_;
  }

  std::string ToString() const;
  std::string ToStringUnlocked() const;

  ConsensusMetadata* consensus_metadata_for_tests() const;

  const ConsensusOptions options_;

  // Information about the local peer, including the local UUID.
  const RaftPeerPB local_peer_pb_;

  // Log prefix for this peer.
  const std::string log_prefix_;

  // Consensus metadata service.
  const scoped_refptr<ConsensusMetadataManager> cmeta_manager_;

  // State shared by Raft instances on a given server.
  const ServerContext server_ctx_;

  // TODO(dralves) hack to serialize updates due to repeated/out-of-order messages
  // should probably be refactored out.
  //
  // Lock ordering note: If both 'update_lock_' and 'lock_' are to be taken,
  // 'update_lock_' lock must be taken first.
  mutable simple_spinlock update_lock_;

  // Coarse-grained lock that protects all mutable data members.
  mutable simple_spinlock lock_;

  std::atomic<State> state_;

  // Consensus metadata persistence object.
  scoped_refptr<ConsensusMetadata> cmeta_;

  // Threadpool token for constructing requests to peers, handling RPC callbacks, etc.
  std::unique_ptr<ThreadPoolToken> raft_pool_token_;

  scoped_refptr<log::Log> log_;
  std::unique_ptr<TimeManager> time_manager_;
  std::unique_ptr<PeerProxyFactory> peer_proxy_factory_;

  // When we receive a message from a remote peer telling us to start an op, or
  // finish a round, we use this handler to handle it. This may update replica
  // state (e.g. the tablet replica).
  ConsensusRoundHandler* round_handler_;

  std::unique_ptr<PeerManager> peer_manager_;

  // The queue of messages that must be sent to peers.
  std::unique_ptr<PeerMessageQueue> queue_;

  // The currently pending rounds that have not yet been committed by
  // consensus. Protected by 'lock_'.
  // TODO(todd) these locks will become more fine-grained.
  std::unique_ptr<PendingRounds> pending_;

  Random rng_;

  std::shared_ptr<rpc::PeriodicTimer> failure_detector_;

  // Whether a replica, switched into the leader mode, has successfully
  // scheduled a NO_OP Raft message to replicate, asserting its leadership in
  // the term where it has just become a leader.
  std::atomic<bool> leader_is_ready_;

  // A few fields used for the leadership transfer process.
  std::atomic<bool> leader_transfer_in_progress_;
  std::optional<std::string> designated_successor_uuid_;
  std::shared_ptr<rpc::PeriodicTimer> transfer_period_timer_;

  // Any RequestVote() arriving before this timestamp is ignored (i.e. responded
  // to with NO vote). This prevents abandoned or partitioned nodes from
  // disturbing the healthy leader.
  std::atomic<MonoTime> withhold_votes_until_;

  // The last OpId received from the current leader. This is updated whenever the follower
  // accepts operations from a leader, and passed back so that the leader knows from what
  // point to continue sending operations.
  OpId last_received_cur_leader_;

  // The number of times this node has called and lost a leader election since
  // the last time it saw a stable leader (either itself or another node).
  // This is used to calculate back-off of the election timeout.
  int64_t failed_elections_since_stable_leader_;

  MarkDirtyCallback mark_dirty_clbk_;

  // A flag to help us avoid taking a lock on the reactor thread if the object
  // is already in kShutdown state.
  // TODO(mpercy): Try to get rid of this extra flag.
  std::atomic<bool> shutdown_;

  // The number of times Update() has been called, used for some test assertions.
  std::atomic<int32_t> update_calls_for_tests_;

  // The wrapping into std::atomic<> is to simplify the synchronization between
  // consensus-related writers and readers of the attached metric gauge.
  std::atomic<int64_t> last_leader_communication_time_micros_;

  scoped_refptr<Counter> follower_memory_pressure_rejections_;
  scoped_refptr<AtomicGauge<int64_t>> term_metric_;
  scoped_refptr<AtomicGauge<int64_t>> num_failed_elections_metric_;

  // NOTE: it's important that this is the first member to be destructed. This
  // ensures we do not attempt to collect metrics while calling the destructor.
  FunctionGaugeDetacher metric_detacher_;

  DISALLOW_COPY_AND_ASSIGN(RaftConsensus);
};

// After completing bootstrap, some of the results need to be plumbed through
// into the consensus implementation.
struct ConsensusBootstrapInfo {
  ConsensusBootstrapInfo();
  ~ConsensusBootstrapInfo();

  // The id of the last operation in the log
  OpId last_id;

  // The id of the last committed operation in the log.
  OpId last_committed_id;

  // REPLICATE messages which were in the log with no accompanying
  // COMMIT. These need to be passed along to consensus init in order
  // to potentially commit them.
  //
  // These are owned by the ConsensusBootstrapInfo instance.
  std::vector<ReplicateMsg*> orphaned_replicates;

 private:
  DISALLOW_COPY_AND_ASSIGN(ConsensusBootstrapInfo);
};

// Handler for consensus rounds.
// An implementation of this handler must be registered prior to consensus
// start, and is used to:
// - Create ops when the consensus implementation receives messages from the
//   leader.
// - Handle when the consensus implementation finishes a no-op round
//
// Follower ops execute the following way:
//
// - When a ReplicateMsg is first received from the leader, the RaftConsensus
//   instance creates the ConsensusRound and calls StartFollowerOp().
//   This will trigger the Prepare(). At the same time, the follower's consensus
//   instance immediately stores the ReplicateMsg in the Log. Once the
//   message is stored in stable storage an ACK is sent to the leader (i.e. the
//   replica RaftConsensus instance does not wait for Prepare() to finish).
//
// - When the CommitMsg for a replicate is first received from the leader, the
//   follower waits for the corresponding Prepare() to finish (if it has not
//   completed yet) and then proceeds to trigger the Apply().
//
// - Once Apply() completes the ConsensusRoundHandler is responsible for logging
//   a CommitMsg to the log to ensure that the operation can be properly restored
//   on a restart.
class ConsensusRoundHandler {
 public:
  virtual ~ConsensusRoundHandler() = default;

  virtual Status StartFollowerOp(const scoped_refptr<ConsensusRound>& context) = 0;

  // Consensus-only rounds complete when the no-op finishes replication. This
  // can be used to trigger callbacks, akin to an Apply() for regular ops.
  virtual void FinishConsensusOnlyRound(ConsensusRound* round) = 0;
};

// Context for a consensus round on the LEADER side, typically created as an
// out-parameter of RaftConsensus::Append().
// This class is ref-counted because we want to ensure it stays alive for the
// duration of the Op when it is associated with a Op, while
// we also want to ensure it has a proper lifecycle when a ConsensusRound is
// pushed that is not associated with a Tablet op.
class ConsensusRound : public RefCountedThreadSafe<ConsensusRound> {

 public:
  // Ctor used for leader ops. Leader ops can and must specify the
  // callbacks prior to initiating the consensus round.
  ConsensusRound(RaftConsensus* consensus,
                 std::unique_ptr<ReplicateMsg> replicate_msg,
                 ConsensusReplicatedCallback replicated_cb);

  // Ctor used for follower/learner ops. These ops do not use the
  // replicate callback and the commit callback is set later, after the op
  // is actually started.
  ConsensusRound(RaftConsensus* consensus,
                 ReplicateRefPtr replicate_msg);

  ReplicateMsg* replicate_msg() {
    return replicate_msg_->get();
  }

  const ReplicateRefPtr& replicate_scoped_refptr() {
    return replicate_msg_;
  }

  // Returns the id of the (replicate) operation this context
  // refers to. This is only set _after_ RaftConsensus::Replicate(context).
  const OpId& id() const {
    return replicate_msg_->get()->id();
  }

  // Register a callback that is called by RaftConsensus to notify that the round
  // is considered either replicated, if 'status' is OK(), or that it has
  // permanently failed to replicate if 'status' is anything else. If 'status'
  // is OK() then the operation can be applied to the state machine, otherwise
  // the operation should be aborted.
  void SetConsensusReplicatedCallback(ConsensusReplicatedCallback replicated_cb) {
    replicated_cb_ = std::move(replicated_cb);
  }

  // If a continuation was set, notifies it that the round has been replicated.
  void NotifyReplicationFinished(const Status& status);

  // Binds this round such that it may not be eventually executed in any term
  // other than 'term'. See CheckBoundTerm().
  void BindToTerm(int64_t term) {
    DCHECK_EQ(bound_term_, -1);
    bound_term_ = term;
  }

  // Check for a rare race in which an operation is submitted to the LEADER in some term,
  // then before the operation is prepared, the replica loses its leadership, receives
  // more operations as a FOLLOWER, and then regains its leadership. We detect this case
  // by setting the ConsensusRound's "bound term" when it is first submitted to the
  // PREPARE queue, and validate that the term is still the same when we have finished
  // preparing it. See KUDU-597 for details.
  //
  // If this round has not been bound to any term, this is a no-op.
  Status CheckBoundTerm(int64_t current_term) const;

 private:
  friend class RefCountedThreadSafe<ConsensusRound>;
  friend class RaftConsensusQuorumTest;

  ~ConsensusRound() {}

  RaftConsensus* consensus_;
  // This round's replicate message.
  ReplicateRefPtr replicate_msg_;

  // The continuation that will be called once the op is deemed
  // committed/aborted by consensus.
  ConsensusReplicatedCallback replicated_cb_;

  // The leader term that this round was submitted in. CheckBoundTerm()
  // ensures that, when it is eventually replicated, the term has not
  // changed in the meantime.
  //
  // Set to -1 if no term has been bound.
  int64_t bound_term_;
};

}  // namespace consensus
}  // namespace kudu
