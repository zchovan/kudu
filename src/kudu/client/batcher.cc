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

#include "kudu/client/batcher.h"

#include <cstddef>
#include <functional>
#include <mutex>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/client/callbacks.h"
#include "kudu/client/client-internal.h"
#include "kudu/client/client.h"
#include "kudu/client/error_collector.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/schema.h"
#include "kudu/client/session-internal.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op-internal.h"
#include "kudu/client/write_op.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/txn_id.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/connection.h"
#include "kudu/rpc/request_tracker.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/retriable_rpc.h"
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/security/token.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"

namespace kudu {
namespace rpc {
class Messenger;
}  // namespace rpc
}  // namespace kudu

using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::CredentialsPolicy;
using kudu::rpc::ErrorStatusPB;
using kudu::rpc::Messenger;
using kudu::rpc::RequestTracker;
using kudu::rpc::ResponseCallback;
using kudu::rpc::RetriableRpc;
using kudu::rpc::RetriableRpcStatus;
using kudu::security::SignedTokenPB;
using kudu::tserver::WriteRequestPB;
using kudu::tserver::WriteResponsePB;
using kudu::tserver::WriteResponsePB_PerRowErrorPB;
using kudu::TxnId;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {

class RowOperationsPB;
class Schema;

namespace client {
namespace internal {

// About lock ordering in this file:
// ------------------------------
// The locks must be acquired in the following order:
//   - Batcher::lock_
//   - InFlightOp::lock_
//
// It's generally important to release all the locks before either calling
// a user callback, or chaining to another async function, since that function
// may also chain directly to the callback. Without releasing locks first,
// the lock ordering may be violated, or a lock may deadlock on itself (these
// locks are non-reentrant).
// ------------------------------------------------------------

// An operation which has been submitted to the batcher and not yet completed.
// The operation goes through a state machine as it progress through the
// various stages of a request. See the State enum for details.
//
// Note that in-flight ops *conceptually* hold a reference to the Batcher object.
// However, since there might be millions of these objects floating around,
// we can save a pointer per object by manually incrementing the Batcher ref-count
// when we create the object, and decrementing when we delete it.
struct InFlightOp {
  InFlightOp() : state(kNew) {
  }

  // Lock protecting the internal state of the op.
  // This is necessary since callbacks may fire from IO threads
  // concurrent with the user trying to abort/delete the batch.
  // See comment above about lock ordering.
  simple_spinlock lock_;

  enum State {
    // Newly created op.
    //
    // OWNERSHIP: The op is only in this state when in local function scope (Batcher::Add)
    kNew = 0,

    // Waiting for the MetaCache to determine which tablet ID hosts the row associated
    // with this operation. In the case that the relevant tablet's key range was
    // already cached, this state will be passed through immediately. Otherwise,
    // the op may sit in this state for some amount of time while waiting on the
    // MetaCache to perform an RPC to the master and find the correct tablet.
    //
    // OWNERSHIP: the op is present in the 'ops_' set, and also referenced by the
    // in-flight callback provided to MetaCache.
    kLookingUpTablet,

    // Once the correct tablet has been determined, and the tablet locations have been
    // refreshed, we are ready to send the operation to the server.
    //
    // In MANUAL_FLUSH mode, the operations wait in this state until Flush has been called.
    //
    // In AUTO_FLUSH_BACKGROUND mode, the operations may wait in this state for one of
    // two reasons:
    //
    //   1) There are already too many outstanding RPCs to the given tablet server.
    //
    //      We restrict the number of concurrent RPCs from one client to a given TS
    //      to achieve better batching and throughput.
    //      TODO: not implemented yet
    //
    //   2) Batching delay.
    //
    //      In order to achieve better batching, we do not immediately send a request
    //      to a TS as soon as we have one pending. Instead, we can wait for a configurable
    //      number of milliseconds for more requests to enter the queue for the same TS.
    //      This makes it likely that if a caller simply issues a small number of requests
    //      to the same tablet in AUTO_FLUSH_BACKGROUND mode that we'll batch all of the
    //      requests together in a single RPC.
    //      TODO: not implemented yet
    //
    // OWNERSHIP: When the operation is in this state, it is present in the 'ops_' set
    // and also in the 'per_tablet_ops' map.
    kBufferedToTabletServer,

    // Once the operation has been flushed (either due to explicit Flush() or background flush)
    // it will enter this state.
    //
    // OWNERSHIP: when entering this state, the op is removed from 'per_tablet_ops' map
    // and ownership is transfered to a WriteRPC's 'ops_' vector. The op still
    // remains in the 'ops_' set.
    kRequestSent
  };
  State state;

  // The actual operation.
  unique_ptr<KuduWriteOperation> write_op;

  // The tablet the operation is destined for.
  // This is only filled in after passing through the kLookingUpTablet state.
  scoped_refptr<RemoteTablet> tablet;

  // Each operation has a unique sequence number which preserves the user's intended
  // order of operations. This is important when multiple operations act on the same row.
  int sequence_number_;

  // Stringifies the InFlightOp.
  //
  // This should be used in log messages instead of KuduWriteOperation::ToString
  // because it handles redaction.
  string ToString() const {
    return strings::Substitute("op[state=$0, write_op=$1]",
                               state,
                               KUDU_REDACT(write_op->ToString()));
  }
};

// A Write RPC which is in-flight to a tablet. Initially, the RPC is sent
// to the leader replica, but it may be retried with another replica if the
// leader fails.
//
// Keeps a reference on the owning batcher while alive.
class WriteRpc : public RetriableRpc<RemoteTabletServer, WriteRequestPB, WriteResponsePB> {
 public:
  WriteRpc(const scoped_refptr<Batcher>& batcher,
           const scoped_refptr<MetaCacheServerPicker>& replica_picker,
           const scoped_refptr<RequestTracker>& request_tracker,
           vector<InFlightOp*> ops,
           const MonoTime& deadline,
           shared_ptr<Messenger> messenger,
           const string& tablet_id,
           uint64_t propagated_timestamp);
  virtual ~WriteRpc();
  string ToString() const override;

  const KuduTable* table() const {
    // All of the ops for a given tablet obviously correspond to the same table,
    // so we'll just grab the table from the first.
    return ops_[0]->write_op->table();
  }
  const vector<InFlightOp*>& ops() const { return ops_; }
  const WriteResponsePB& resp() const { return resp_; }
  const string& tablet_id() const { return tablet_id_; }

 protected:
  void Try(RemoteTabletServer* replica, const ResponseCallback& callback) override;
  RetriableRpcStatus AnalyzeResponse(const Status& rpc_cb_status) override;
  void Finish(const Status& status) override;
  bool GetNewAuthnTokenAndRetry() override;

  // Asynchonously attempts to retrieve a new authz token from the master, and
  // runs GotNewAuthzTokenRetryCb on success.
  bool GetNewAuthzTokenAndRetry() override;

  // Callback to run after going through the steps to get a new authz token.
  void GotNewAuthzTokenRetryCb(const Status& status) override;

 private:
  // Fetches the appropriate authz token for this request from the client
  // cache. Note that this doesn't get a new token from the master, but rather,
  // it updates 'req_' with one from the cache in case the client has recently
  // received one.
  //
  // If an appropriate authz token is not in the cache, e.g. because the client
  // has been communicating with an older-versioned master that doesn't support
  // authz tokens, this is a no-op.
  void FetchCachedAuthzToken();

  // Pointer back to the batcher. Processes the write response when it
  // completes, regardless of success or failure.
  scoped_refptr<Batcher> batcher_;

  // Operations which were batched into this RPC.
  // These operations are in kRequestSent state.
  vector<InFlightOp*> ops_;

  // The id of the tablet being written to.
  string tablet_id_;
};

WriteRpc::WriteRpc(const scoped_refptr<Batcher>& batcher,
                   const scoped_refptr<MetaCacheServerPicker>& replica_picker,
                   const scoped_refptr<RequestTracker>& request_tracker,
                   vector<InFlightOp*> ops,
                   const MonoTime& deadline,
                   shared_ptr<Messenger> messenger,
                   const string& tablet_id,
                   uint64_t propagated_timestamp)
    : RetriableRpc(replica_picker, request_tracker, deadline, std::move(messenger)),
      batcher_(batcher),
      ops_(std::move(ops)),
      tablet_id_(tablet_id) {
  const Schema* schema = table()->schema().schema_;

  req_.set_tablet_id(tablet_id_);
  switch (batcher->external_consistency_mode()) {
    case kudu::client::KuduSession::CLIENT_PROPAGATED:
      req_.set_external_consistency_mode(kudu::CLIENT_PROPAGATED);
      break;
    case kudu::client::KuduSession::COMMIT_WAIT:
      req_.set_external_consistency_mode(kudu::COMMIT_WAIT);
      break;
    default:
      LOG(FATAL) << "Unsupported consistency mode: " << batcher->external_consistency_mode();

  }
  // If set, propagate the latest observed timestamp.
  if (PREDICT_TRUE(propagated_timestamp != KuduClient::kNoTimestamp)) {
    req_.set_propagated_timestamp(propagated_timestamp);
  }

  if (batcher->txn_id().IsValid()) {
    req_.set_txn_id(batcher->txn_id());
  }

  // Set up schema
  CHECK_OK(SchemaToPB(*schema, req_.mutable_schema(),
                      SCHEMA_PB_WITHOUT_STORAGE_ATTRIBUTES |
                      SCHEMA_PB_WITHOUT_IDS |
                      SCHEMA_PB_WITHOUT_COMMENT));

  // Pick up the authz token for the table.
  FetchCachedAuthzToken();
  RowOperationsPB* requested = req_.mutable_row_operations();

  // Add the rows
  int ctr = 0;
  RowOperationsPBEncoder enc(requested);
  for (InFlightOp* op : ops_) {
#ifndef NDEBUG
    // Run the same verification that is about to be run by the tablet server
    // the data is sent to.
    const Partition& partition = op->tablet->partition();
    const PartitionSchema& partition_schema = table()->partition_schema();
    const KuduPartialRow& row = op->write_op->row();
    CHECK(partition_schema.PartitionContainsRow(partition, row))
        << "Row " << partition_schema.PartitionKeyDebugString(row)
        << " not in partition " << partition_schema.PartitionDebugString(partition, *schema);
#endif

    enc.Add(ToInternalWriteType(op->write_op->type()), op->write_op->row());

    // Set the state now, even though we haven't yet sent it -- at this point
    // there is no return, and we're definitely going to send it. If we waited
    // until after we sent it, the RPC callback could fire before we got a chance
    // to change its state to 'sent'.
    op->state = InFlightOp::kRequestSent;
    VLOG(4) << ++ctr << ". Encoded row " << op->ToString();
  }

  VLOG(3) << Substitute("Created batch for $0:\n$1",
                        tablet_id, SecureShortDebugString(req_));
}

WriteRpc::~WriteRpc() {
  // Since the WriteRpc is destructed a while after all of the
  // InFlightOps and other associated objects were last touched,
  // and because those operations were not all allocated together,
  // they're likely to be strewn all around in RAM. This function
  // then ends up cache-miss-bound.
  //
  // Ideally, we could change the allocation pattern to make them
  // more contiguous, but it's a bit tricky -- this is client code,
  // so we don't really have great control over how the write ops
  // themselves are allocated.
  //
  // So, instead, we do some prefetching. The pointer graph looks like:
  //
  // vector<InFlightOp*>
  //    [i] InFlightOp* pointer
  //         \----> InFlightOp instance
  //                | WriteOp* pointer
  //                |   \-----> WriteOp instance
  //                            | KuduPartialRow (embedded)
  //                            | | isset_bitmap_
  //                                   \-----> heap allocated memory
  //
  //
  // So, we need to do three "layers" of prefetch. First, prefetch the
  // InFlightOp instance. Then, prefetch the KuduPartialRow contained by
  // the WriteOp that it points to. Then, prefetch the isset bitmap that
  // the PartialRow points to.
  //
  // In order to get parallelism here, we need to stagger the prefetches:
  // the "root" of the tree needs to look farthest in the future, then
  // prefetch the next level, then prefetch the closest level, before
  // eventually calling the destructor.
  //
  // Experimentally, it seems we get enough benefit from only prefetching
  // one entry "ahead" in between each.
  constexpr static int kPrefetchDistance = 1;
  const int size = ops_.size();

  auto iter = [this, size](int i) {
    int ifo_prefetch = i + kPrefetchDistance * 3;
    int op_prefetch = i + kPrefetchDistance * 2;
    int row_prefetch = i + kPrefetchDistance;
    if (ifo_prefetch >= 0 && ifo_prefetch < size) {
      __builtin_prefetch(ops_[ifo_prefetch], 0, PREFETCH_HINT_T0);
    }
    if (op_prefetch >= 0 && op_prefetch < size) {
      const auto* op = ops_[op_prefetch]->write_op.get();
      if (op) {
        __builtin_prefetch(&op->row().isset_bitmap_, 0, PREFETCH_HINT_T0);
      }
    }
    if (row_prefetch >= 0 && row_prefetch < size) {
      const auto* op = ops_[row_prefetch]->write_op.get();
      if (op) {
        __builtin_prefetch(op->row().isset_bitmap_, 0, PREFETCH_HINT_T0);
      }
    }
    if (i >= 0) {
      ops_[i]->~InFlightOp();
    }
  };

  // Explicitly perform "loop splitting" to avoid the branches in the main
  // body of the loop.
  int i = -kPrefetchDistance * 3;
  while (i < 0) {
    iter(i++);
  }
  while (i < size - kPrefetchDistance * 3) {
    iter(i++);
  }
  while (i < size) {
    iter(i++);
  }
}

string WriteRpc::ToString() const {
  return Substitute("Write(tablet: $0, num_ops: $1, num_attempts: $2)",
                    tablet_id_, ops_.size(), num_attempts());
}

void WriteRpc::Try(RemoteTabletServer* replica, const ResponseCallback& callback) {
  VLOG(2) << "Tablet " << tablet_id_ << ": Writing batch to replica " << replica->ToString();
  replica->proxy()->WriteAsync(req_, &resp_,
                               mutable_retrier()->mutable_controller(),
                               callback);
}

void WriteRpc::Finish(const Status& status) {
  unique_ptr<WriteRpc> this_instance(this);
  Status final_status = status;
  if (!final_status.ok()) {
    final_status = final_status.CloneAndPrepend(
        Substitute("Failed to write batch of $0 ops to tablet $1 after $2 attempt(s)",
                   ops_.size(), tablet_id_, num_attempts()));
    KLOG_EVERY_N_SECS(WARNING, 1) << final_status.ToString();
  }
  batcher_->ProcessWriteResponse(*this, final_status);
}

RetriableRpcStatus WriteRpc::AnalyzeResponse(const Status& rpc_cb_status) {
  RetriableRpcStatus result;
  result.status = rpc_cb_status;

  // If we didn't fail on tablet lookup/proxy initialization, check if we failed actually performing
  // the write.
  if (rpc_cb_status.ok()) {
    result.status = mutable_retrier()->controller().status();
  }

  // Check for specific RPC errors.
  if (result.status.IsRemoteError()) {
    const ErrorStatusPB* err = mutable_retrier()->controller().error_response();
    if (err && err->has_code()) {
      switch (err->code()) {
        case ErrorStatusPB::ERROR_SERVER_TOO_BUSY:
        case ErrorStatusPB::ERROR_UNAVAILABLE:
          result.result = RetriableRpcStatus::SERVICE_UNAVAILABLE;
          return result;
        case ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN:
          result.result = RetriableRpcStatus::INVALID_AUTHORIZATION_TOKEN;
          return result;
        default:
          break;
      }
    }
  }

  if (result.status.IsServiceUnavailable() ||
      (resp_.has_error() &&
       resp_.error().code() == tserver::TabletServerErrorPB::TXN_LOCKED_RETRY_OP)) {
    result.result = RetriableRpcStatus::SERVICE_UNAVAILABLE;
    return result;
  }

  // Check whether it's an invalid authn token. That's the error code the server
  // sends back if authn token is expired.
  if (result.status.IsNotAuthorized()) {
    const ErrorStatusPB* err = mutable_retrier()->controller().error_response();
    if (err && err->has_code() &&
        err->code() == ErrorStatusPB::FATAL_INVALID_AUTHENTICATION_TOKEN) {
      result.result = RetriableRpcStatus::INVALID_AUTHENTICATION_TOKEN;
      return result;
    }
  }

  // Failover to a replica in the event of any network failure or of a DNS resolution problem.
  //
  // TODO(adar): This is probably too harsh; some network failures should be
  // retried on the current replica.
  if (result.status.IsNetworkError()) {
    result.result = RetriableRpcStatus::SERVER_NOT_ACCESSIBLE;
    return result;
  }

  // Prefer controller failures over response failures.
  if (result.status.ok() && resp_.has_error()) {
    result.status = StatusFromPB(resp_.error().status());
  }

  // If we get TABLET_NOT_FOUND, the replica we thought was leader has been deleted.
  if (resp_.has_error() && resp_.error().code() == tserver::TabletServerErrorPB::TABLET_NOT_FOUND) {
    result.result = RetriableRpcStatus::RESOURCE_NOT_FOUND;
    return result;
  }

  if (resp_.has_error() &&
      (resp_.error().code() == tserver::TabletServerErrorPB::TXN_ILLEGAL_STATE ||
       resp_.error().code() == tserver::TabletServerErrorPB::TXN_LOCKED_ABORT)) {
    result.result = RetriableRpcStatus::NON_RETRIABLE_ERROR;
    return result;
  }

  // Alternatively, when we get a status code of IllegalState or Aborted, we
  // assume this means that the replica we attempted to write to is not the
  // current leader (maybe it got partitioned or slow and another node took
  // over).
  //
  // TODO: This error handling block should really be rewritten to handle
  // specific error codes exclusively instead of Status codes (this may
  // require some server-side changes). For example, IllegalState is
  // obviously way too broad an error category for this case.
  if (result.status.IsIllegalState() || result.status.IsAborted()) {
    // TODO(aserbin): this very broad transformation of Status::IllegalState()
    //                becomes a real issue when handling responses to write
    //                operations in the context of multi-row transactions.
    //                For example, Status::IllegalState() originated from
    //                TabletServerErrorPB::TXN_ILLEGAL_STATE response and
    //                Status::Abort() originated from TabletServerErrorPB::TXN_LOCKED_ABORT
    //                response are needlessly retried.
    result.result = RetriableRpcStatus::REPLICA_NOT_LEADER;
    return result;
  }

  // Handle the connection negotiation failure case if overall RPC's timeout
  // hasn't expired yet: if the connection negotiation returned non-OK status,
  // mark the server as not accessible and rely on the RetriableRpc's logic
  // to switch to an alternative tablet replica.
  //
  // NOTE: Connection negotiation errors related to security are handled in the
  //       code above: see the handlers for IsNotAuthorized(), IsRemoteError().
  if (!rpc_cb_status.IsTimedOut() && !result.status.ok() &&
      mutable_retrier()->controller().negotiation_failed()) {
    result.result = RetriableRpcStatus::SERVER_NOT_ACCESSIBLE;
    return result;
  }

  if (result.status.ok()) {
    result.result = RetriableRpcStatus::OK;
  } else {
    result.result = RetriableRpcStatus::NON_RETRIABLE_ERROR;
  }
  return result;
}

bool WriteRpc::GetNewAuthnTokenAndRetry() {
  // Since we know we may retry, clear the existing response.
  resp_.Clear();
  // To get a new authn token it's necessary to authenticate with the master
  // using any other credentials but already existing authn token.
  KuduClient* c = batcher_->client_;
  VLOG(1) << "Retrieving new authn token from master";
  c->data_->ConnectToClusterAsync(c, retrier().deadline(),
      [this](const Status& s) { this->GotNewAuthnTokenRetryCb(s); },
      CredentialsPolicy::PRIMARY_CREDENTIALS);
  return true;
}

bool WriteRpc::GetNewAuthzTokenAndRetry() {
  // Since we know we may retry, clear the existing response.
  resp_.Clear();
  KuduClient* c = batcher_->client_;
  VLOG(1) << "Retrieving new authz token from master";
  c->data_->RetrieveAuthzTokenAsync(table(),
      [this](const Status& s) { this->GotNewAuthzTokenRetryCb(s); },
      retrier().deadline());
  return true;
}

void WriteRpc::FetchCachedAuthzToken() {
  SignedTokenPB signed_token;
  if (batcher_->client_->data_->FetchCachedAuthzToken(table()->id(), &signed_token)) {
    *req_.mutable_authz_token() = std::move(signed_token);
  } else {
    // Note: this is the expected path if communicating with an older-versioned
    // master that does not support authz tokens.
    VLOG(1) << "no authz token for table " << table()->id();
  }
}

void WriteRpc::GotNewAuthzTokenRetryCb(const Status& status) {
  if (status.ok()) {
    FetchCachedAuthzToken();
  }
  RetriableRpc::GotNewAuthzTokenRetryCb(status);
}

Batcher::Batcher(KuduClient* client,
                 scoped_refptr<ErrorCollector> error_collector,
                 sp::weak_ptr<KuduSession> session,
                 kudu::client::KuduSession::ExternalConsistencyMode consistency_mode,
                 const TxnId& txn_id)
  : state_(kGatheringOps),
    client_(client),
    weak_session_(std::move(session)),
    consistency_mode_(consistency_mode),
    txn_id_(txn_id),
    error_collector_(std::move(error_collector)),
    flush_callback_(nullptr),
    next_op_sequence_number_(0),
    timeout_(client->default_rpc_timeout()),
    outstanding_lookups_(0),
    buffer_bytes_used_(0),
    had_errors_(false),
    arena_(1024) {
  ops_.set_empty_key(nullptr);
  ops_.set_deleted_key(reinterpret_cast<InFlightOp*>(-1));
}

void Batcher::Abort() {
  std::unique_lock<simple_spinlock> l(lock_);
  state_ = kAborted;

  vector<InFlightOp*> to_abort;
  for (InFlightOp* op : ops_) {
    std::lock_guard<simple_spinlock> l(op->lock_);
    if (op->state == InFlightOp::kBufferedToTabletServer) {
      to_abort.push_back(op);
    }
  }

  for (InFlightOp* op : to_abort) {
    VLOG(1) << "Aborting op: " << op->ToString();
    MarkInFlightOpFailedUnlocked(op, Status::Aborted("Batch aborted"));
  }

  if (flush_callback_) {
    l.unlock();

    flush_callback_->Run(Status::Aborted(""));
  }
}

Batcher::~Batcher() {
  if (PREDICT_FALSE(!ops_.empty())) {
    for (InFlightOp* op : ops_) {
      LOG(ERROR) << "Orphaned op: " << op->ToString();
    }
    LOG(FATAL) << "ops_ not empty";
  }
  CHECK(state_ == kFlushed || state_ == kAborted) << "Bad state: " << state_;
}

void Batcher::SetTimeout(const MonoDelta& timeout) {
  std::lock_guard<simple_spinlock> l(lock_);
  timeout_ = timeout;
}


bool Batcher::HasPendingOperations() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return !ops_.empty();
}

int Batcher::CountBufferedOperations() const {
  std::lock_guard<simple_spinlock> l(lock_);
  if (state_ == kGatheringOps) {
    return ops_.size();
  } else {
    // If we've already started to flush, then the ops aren't
    // considered "buffered".
    return 0;
  }
}

void Batcher::CheckForFinishedFlush() {
  sp::shared_ptr<KuduSession> session;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    if (state_ != kFlushing || !ops_.empty()) {
      return;
    }

    session = weak_session_.lock();
    state_ = kFlushed;
  }

  if (session) {
    // Important to do this outside of the lock so that we don't have
    // a lock inversion deadlock -- the session lock should always
    // come before the batcher lock.
    session->data_->FlushFinished(this);
  }
  if (flush_callback_) {
    // In case of a failure flushing the data to the server, a user can get
    // per-row error details by calling KuduSession::GetPendingErrors().
    auto s = had_errors_
        ? Status::IOError("failed to flush last batch of rows: error details "
                          "are available via KuduSession::GetPendingErrors()")
        : Status::OK();
    flush_callback_->Run(s);
  }
}

MonoTime Batcher::ComputeDeadlineUnlocked() const {
  return MonoTime::Now() + timeout_;
}

void Batcher::FlushAsync(KuduStatusCallback* cb) {
  {
    std::lock_guard<simple_spinlock> l(lock_);
    CHECK_EQ(state_, kGatheringOps);
    state_ = kFlushing;
    flush_callback_ = cb;
    deadline_ = ComputeDeadlineUnlocked();
  }

  // In the case that we have nothing buffered, just call the callback
  // immediately. Otherwise, the callback will be called by the last callback
  // when it sees that the ops_ list has drained.
  CheckForFinishedFlush();

  // Trigger flushing of all of the buffers. Some of these may already have
  // been flushed through an async path, but it's idempotent - a second call
  // to flush would just be a no-op.
  //
  // If some of the operations are still in-flight, then they'll get sent
  // when they hit 'per_tablet_ops', since our state is now kFlushing.
  FlushBuffersIfReady();
}

Status Batcher::Add(KuduWriteOperation* write_op) {
  // As soon as we get the op, start looking up where it belongs,
  // so that when the user calls Flush, we are ready to go.
  InFlightOp* op = arena_.NewObject<InFlightOp>();
  op->write_op.reset(write_op);
  op->state = InFlightOp::kLookingUpTablet;

  AddInFlightOp(op);
  VLOG(3) << "Looking up tablet for " << op->ToString();
  // Increment our reference count for the outstanding callback.
  //
  // deadline_ is set in FlushAsync(), after all Add() calls are done, so
  // here we're forced to create a new deadline.
  MonoTime deadline = ComputeDeadlineUnlocked();
  ++outstanding_lookups_;
  scoped_refptr<Batcher> self(this);
  const auto* table = write_op->table();
  DCHECK(table);
  client_->data_->meta_cache_->LookupTabletByKey(
      table,
      table->partition_schema().EncodeKey(write_op->row()),
      deadline,
      MetaCache::LookupType::kPoint,
      &op->tablet,
      [self, op](const Status& s) { self->TabletLookupFinished(op, s); });
  buffer_bytes_used_ += write_op->SizeInBuffer();

  return Status::OK();
}

void Batcher::AddInFlightOp(InFlightOp* op) {
  DCHECK_EQ(op->state, InFlightOp::kLookingUpTablet);

  std::lock_guard<simple_spinlock> l(lock_);
  CHECK_EQ(state_, kGatheringOps);
  InsertOrDie(&ops_, op);
  op->sequence_number_ = next_op_sequence_number_++;

  // Set the time of the first operation in the batch, if not set yet.
  if (PREDICT_FALSE(!first_op_time_.Initialized())) {
    first_op_time_ = MonoTime::Now();
  }
}

bool Batcher::IsAbortedUnlocked() const {
  return state_ == kAborted;
}

void Batcher::MarkInFlightOpFailed(InFlightOp* op, const Status& s) {
  std::lock_guard<simple_spinlock> l(lock_);
  MarkInFlightOpFailedUnlocked(op, s);
}

void Batcher::MarkInFlightOpFailedUnlocked(InFlightOp* op, const Status& s) {
  DCHECK(lock_.is_locked());
  CHECK_EQ(1, ops_.erase(op))
    << "Could not remove op " << op->ToString() << " from in-flight list";
  error_collector_->AddError(unique_ptr<KuduError>(new KuduError(op->write_op.release(), s)));
  had_errors_ = true;
  op->~InFlightOp();
}

void Batcher::TabletLookupFinished(InFlightOp* op, const Status& s) {
  --outstanding_lookups_;

  // Acquire the batcher lock early to atomically:
  // 1. Test if the batcher was aborted, and
  // 2. Change the op state.
  std::unique_lock<simple_spinlock> l(lock_);

  if (IsAbortedUnlocked()) {
    VLOG(1) << "Aborted batch: TabletLookupFinished for " << op->ToString();
    MarkInFlightOpFailedUnlocked(op, Status::Aborted("Batch aborted"));
    // 'op' is deleted by above function.
    return;
  }

  if (VLOG_IS_ON(3)) {
    VLOG(3) << "TabletLookupFinished for " << op->ToString()
            << ": " << s.ToString();
    if (s.ok()) {
      VLOG(3) << "Result: tablet_id = " << op->tablet->tablet_id();
    }
  }

  if (!s.ok()) {
    MarkInFlightOpFailedUnlocked(op, s);
    l.unlock();
    CheckForFinishedFlush();

    // Even if we failed our lookup, it's possible that other requests were still
    // pending waiting for our pending lookup to complete. So, we have to let them
    // proceed.
    FlushBuffersIfReady();
    return;
  }

  {
    std::lock_guard<simple_spinlock> l2(op->lock_);
    CHECK_EQ(op->state, InFlightOp::kLookingUpTablet);
    CHECK(op->tablet != NULL);

    op->state = InFlightOp::kBufferedToTabletServer;

    vector<InFlightOp*>& to_ts = per_tablet_ops_[op->tablet.get()];
    to_ts.push_back(op);

    // "Reverse bubble sort" the operation into the right spot in the tablet server's
    // buffer, based on the sequence numbers of the ops.
    //
    // There is a rare race (KUDU-743) where two operations in the same batch can get
    // their order inverted with respect to the order that the user originally performed
    // the operations. This loop re-sequences them back into the correct order. In
    // the common case, it will break on the first iteration, so we expect the loop to be
    // constant time, with worst case O(n). This is usually much better than something
    // like a priority queue which would have O(lg n) in every case and a more complex
    // code path.
    for (int i = to_ts.size() - 1; i > 0; --i) {
      if (to_ts[i]->sequence_number_ < to_ts[i - 1]->sequence_number_) {
        std::swap(to_ts[i], to_ts[i - 1]);
      } else {
        break;
      }
    }
  }

  l.unlock();

  FlushBuffersIfReady();
}

void Batcher::FlushBuffersIfReady() {
  unordered_map<RemoteTablet*, vector<InFlightOp*> > ops_copy;

  // We're only ready to flush if:
  // 1. The batcher is in the flushing state (i.e. FlushAsync was called).
  // 2. All outstanding ops have finished lookup. Why? To avoid a situation
  //    where ops are flushed one by one as they finish lookup.
  {
    std::lock_guard<simple_spinlock> l(lock_);
    if (state_ != kFlushing) {
      VLOG(3) << "FlushBuffersIfReady: batcher not yet in flushing state";
      return;
    }
    const int32_t pending_lookups_num = outstanding_lookups_;
    if (pending_lookups_num != 0) {
      VLOG(3) << Substitute("FlushBuffersIfReady: $0 ops still in lookup",
                            pending_lookups_num);
      return;
    }
    // Take ownership of the ops while we're under the lock.
    ops_copy.swap(per_tablet_ops_);
  }

  // Now flush the ops for each tablet.
  for (const OpsMap::value_type& e : ops_copy) {
    RemoteTablet* tablet = e.first;
    const vector<InFlightOp*>& ops = e.second;

    VLOG(3) << "FlushBuffersIfReady: already in flushing state, immediately flushing to "
            << tablet->tablet_id();
    FlushBuffer(tablet, ops);
  }
}

void Batcher::FlushBuffer(RemoteTablet* tablet, const vector<InFlightOp*>& ops) {
  CHECK(!ops.empty());

  // Create and send an RPC that aggregates the ops. The RPC is freed when
  // its callback completes.
  //
  // The RPC object takes ownership of the ops.

  // TODO Keep a replica picker per tablet and share it across writes
  // to the same tablet.
  scoped_refptr<MetaCacheServerPicker> server_picker(
      new MetaCacheServerPicker(client_,
                                client_->data_->meta_cache_,
                                ops[0]->write_op->table(),
                                tablet));
  WriteRpc* rpc = new WriteRpc(this,
                               server_picker,
                               client_->data_->request_tracker_,
                               ops,
                               deadline_,
                               client_->data_->messenger_,
                               tablet->tablet_id(),
                               client_->data_->GetLatestObservedTimestamp());
  rpc->SendRpc();
}

void Batcher::ProcessWriteResponse(const WriteRpc& rpc,
                                   const Status& s) {
  // TODO: there is a potential race here -- if the Batcher gets destructed while
  // RPCs are in-flight, then accessing state_ will crash. We probably need to keep
  // track of the in-flight RPCs, and in the destructor, change each of them to an
  // "aborted" state.
  CHECK_EQ(state_, kFlushing);

  if (s.ok()) {
    if (rpc.resp().has_timestamp()) {
      client_->data_->UpdateLatestObservedTimestamp(rpc.resp().timestamp());
    }
  } else {
    // Mark each of the rows in the write op as failed, since the whole RPC failed.
    for (InFlightOp* op : rpc.ops()) {
      unique_ptr<KuduError> error(new KuduError(op->write_op.release(), s));
      error_collector_->AddError(std::move(error));
      had_errors_ = true;
    }
  }

  // Check individual row errors.
  for (const WriteResponsePB_PerRowErrorPB& err_pb : rpc.resp().per_row_errors()) {
    // TODO(todd): handle case where we get one of the more specific TS errors
    // like the tablet not being hosted?

    if (err_pb.row_index() >= rpc.ops().size()) {
      LOG(ERROR) << "Received a per_row_error for an out-of-bound op index "
                 << err_pb.row_index() << " (sent only "
                 << rpc.ops().size() << " ops)";
      LOG(ERROR) << "Response from tablet " << rpc.tablet_id() << ":\n"
                 << SecureDebugString(rpc.resp());
      continue;
    }
    unique_ptr<KuduWriteOperation> op = std::move(rpc.ops()[err_pb.row_index()]->write_op);
    VLOG(2) << "Error on op " << op->ToString() << ": "
            << SecureShortDebugString(err_pb.error());
    Status op_status = StatusFromPB(err_pb.error());
    unique_ptr<KuduError> error(new KuduError(op.release(), op_status));
    error_collector_->AddError(std::move(error));
    had_errors_ = true;
  }

  // Collect metrics
  if (sp::shared_ptr<KuduSession> session = weak_session_.lock()) {
    session->data_->UpdateWriteOpMetrics(rpc.resp().resource_metrics());
  }

  // Remove all the ops from the "in-flight" list. It's essential to do so
  // _after_ adding all errors into the collector, otherwise there might be
  // a race which manifests itself as described at KUDU-1743. Essentially,
  // the race was the following:
  //
  //   * There are two concurrent calls to this method, one from each of RPC
  //     sent to corresponding tservers.
  //
  //   * T1 removes its Write's ops from ops_, adds its errors, and calls
  //     CheckForFinishedFlush().
  //
  //   * T2 does the same.
  //
  //   * T1 is descheduled/delayed after removing from ops_ but before adding
  //     its errors.
  //
  //   * T2 runs completely through ProcessWriteResponse(),
  //     calls CheckForFinishedFlush(), and wakes up the client thread
  //     from which the Flush() is being called.
  {
    std::lock_guard<simple_spinlock> l(lock_);
    for (InFlightOp* op : rpc.ops()) {
      CHECK_EQ(1, ops_.erase(op))
            << "Could not remove op " << op->ToString()
            << " from in-flight list";
    }
  }

  CheckForFinishedFlush();
}

} // namespace internal
} // namespace client
} // namespace kudu
