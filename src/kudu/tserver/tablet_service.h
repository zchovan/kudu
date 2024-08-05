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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "kudu/consensus/consensus.service.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.service.h"
#include "kudu/tserver/tserver_service.service.h"
#include "kudu/util/metrics.h"
#include "kudu/util/random.h"

namespace google {
namespace protobuf {
class Message;
}
}

namespace kudu {

class RowwiseIterator;
class Schema;
class Status;
class Timestamp;

namespace server {
class ServerBase;
} // namespace server

namespace consensus {
class BulkChangeConfigRequestPB;
class ChangeConfigRequestPB;
class ChangeConfigResponsePB;
class ConsensusRequestPB;
class ConsensusResponsePB;
class GetConsensusStateRequestPB;
class GetConsensusStateResponsePB;
class GetLastOpIdRequestPB;
class GetLastOpIdResponsePB;
class GetNodeInstanceRequestPB;
class GetNodeInstanceResponsePB;
class LeaderStepDownRequestPB;
class LeaderStepDownResponsePB;
class RunLeaderElectionRequestPB;
class RunLeaderElectionResponsePB;
class StartTabletCopyRequestPB;
class StartTabletCopyResponsePB;
class TimeManager;
class UnsafeChangeConfigRequestPB;
class UnsafeChangeConfigResponsePB;
class VoteRequestPB;
class VoteResponsePB;
} // namespace consensus

namespace rpc {
class RpcContext;
} // namespace rpc

namespace tablet {
class Tablet;
class TabletReplica;
} // namespace tablet

namespace tserver {

class AlterSchemaRequestPB;
class AlterSchemaResponsePB;
class ChecksumRequestPB;
class ChecksumResponsePB;
class CoordinateTransactionRequestPB;
class CoordinateTransactionResponsePB;
class CreateTabletRequestPB;
class CreateTabletResponsePB;
class DeleteTabletRequestPB;
class DeleteTabletResponsePB;
class ParticipantRequestPB;
class ParticipantResponsePB;
class QuiesceTabletServerRequestPB;
class QuiesceTabletServerResponsePB;
class ScanResultCollector;
class TabletReplicaLookupIf;
class TabletServer;

class TabletServiceImpl : public TabletServerServiceIf {
 public:
  explicit TabletServiceImpl(TabletServer* server);

  bool AuthorizeClient(const google::protobuf::Message* req,
                       google::protobuf::Message* resp,
                       rpc::RpcContext* context) override;

  bool AuthorizeClientOrServiceUser(const google::protobuf::Message* req,
                                    google::protobuf::Message* resp,
                                    rpc::RpcContext* context) override;

  // Note: we authorize ListTablets separately because our fine-grained access
  // model is simpler when authorization is scoped to a single table, which
  // isn't the case for ListTablets. Rather than authorizing multiple tables at
  // once, if enforcing access control, we require the super-user role and omit
  // checking table privileges, and authorize as a client otherwise.
  bool AuthorizeListTablets(const google::protobuf::Message* req,
                            google::protobuf::Message* resp,
                            rpc::RpcContext* context) override;

  void Ping(const PingRequestPB* req,
            PingResponsePB* resp,
            rpc::RpcContext* context) override;

  void Write(const WriteRequestPB* req, WriteResponsePB* resp,
             rpc::RpcContext* context) override;

  void Scan(const ScanRequestPB* req,
            ScanResponsePB* resp,
            rpc::RpcContext* context) override;

  void ScannerKeepAlive(const ScannerKeepAliveRequestPB *req,
                        ScannerKeepAliveResponsePB *resp,
                        rpc::RpcContext *context) override;

  void ListTablets(const ListTabletsRequestPB* req,
                   ListTabletsResponsePB* resp,
                   rpc::RpcContext* context) override;

  void SplitKeyRange(const SplitKeyRangeRequestPB* req,
                     SplitKeyRangeResponsePB* resp,
                     rpc::RpcContext* context) override;

  void Checksum(const ChecksumRequestPB* req,
                ChecksumResponsePB* resp,
                rpc::RpcContext* context) override;

  bool SupportsFeature(uint32_t feature) const override;

  void Shutdown() override;

 private:
  Status HandleNewScanRequest(tablet::TabletReplica* tablet_replica,
                              const ScanRequestPB* req,
                              const rpc::RpcContext* rpc_context,
                              ScanResultCollector* result_collector,
                              std::string* scanner_id,
                              Timestamp* snap_timestamp,
                              bool* has_more_results,
                              TabletServerErrorPB::Code* error_code);

  Status HandleContinueScanRequest(const ScanRequestPB* req,
                                   const rpc::RpcContext* rpc_context,
                                   ScanResultCollector* result_collector,
                                   bool* has_more_results,
                                   TabletServerErrorPB::Code* error_code);

  // Handle READ_AT_SNAPSHOT and READ_YOUR_WRITES scans.
  // Returns the opened row iterator, the start timestamp of a snapshot scan,
  // if applicable, and the ending timestamp of a scan.
  Status HandleScanAtSnapshot(const NewScanRequestPB& scan_pb,
                              const rpc::RpcContext* rpc_context,
                              const Schema& projection,
                              tablet::Tablet* tablet,
                              consensus::TimeManager* time_manager,
                              std::unique_ptr<RowwiseIterator>* iter,
                              std::optional<Timestamp>* snap_start_timestamp,
                              Timestamp* snap_timestamp,
                              TabletServerErrorPB::Code* error_code);

  // Validates the given timestamp is not so far in the future that
  // it exceeds the maximum allowed clock synchronization error time,
  // as such a timestamp is invalid.
  Status ValidateTimestamp(const Timestamp& snap_timestamp);

  // Pick a timestamp according to the scan mode, and verify that the
  // timestamp is after the tablet's ancient history mark.
  Status PickAndVerifyTimestamp(const NewScanRequestPB& scan_pb,
                                tablet::Tablet* tablet,
                                Timestamp* snap_timestamp);

  TabletServer* server_;

  // Random generator used to make a decision on the admission of write
  // operations when the apply queue is overloaded.
  ThreadSafeRandom rng_;

  // Counter to track number of rejected write requests while op apply queue
  // was overloaded.
  scoped_refptr<Counter> num_op_apply_queue_rejections_;
};

class TabletServiceAdminImpl : public TabletServerAdminServiceIf {
 public:
  explicit TabletServiceAdminImpl(TabletServer* server);

  bool AuthorizeServiceUser(const google::protobuf::Message* req,
                            google::protobuf::Message* resp,
                            rpc::RpcContext* context) override;

  void CreateTablet(const CreateTabletRequestPB* req,
                    CreateTabletResponsePB* resp,
                    rpc::RpcContext* context) override;

  void DeleteTablet(const DeleteTabletRequestPB* req,
                    DeleteTabletResponsePB* resp,
                    rpc::RpcContext* context) override;

  void AlterSchema(const AlterSchemaRequestPB* req,
                   AlterSchemaResponsePB* resp,
                   rpc::RpcContext* context) override;

  void Quiesce(const QuiesceTabletServerRequestPB* req,
               QuiesceTabletServerResponsePB* resp,
               rpc::RpcContext* context) override;

  void CoordinateTransaction(const CoordinateTransactionRequestPB* req,
                             CoordinateTransactionResponsePB* resp,
                             rpc::RpcContext* context) override;

  void ParticipateInTransaction(const ParticipantRequestPB* req,
                                ParticipantResponsePB* resp,
                                rpc::RpcContext* context) override;

  bool SupportsFeature(uint32_t feature) const override;

 private:
  TabletServer* server_;
};

class ConsensusServiceImpl : public consensus::ConsensusServiceIf {
 public:
  ConsensusServiceImpl(server::ServerBase* server,
                       TabletReplicaLookupIf* tablet_manager);

  ~ConsensusServiceImpl() override;

  bool AuthorizeServiceUser(const google::protobuf::Message* req,
                            google::protobuf::Message* resp,
                            rpc::RpcContext* context) override;

  void UpdateConsensus(const consensus::ConsensusRequestPB* req,
                       consensus::ConsensusResponsePB* resp,
                       rpc::RpcContext* context) override;

  void MultiRaftUpdateConsensus(
      const consensus::MultiRaftConsensusRequestPB* req,
      consensus::MultiRaftConsensusResponsePB* resp,
      ::kudu::rpc::RpcContext* context) override;

  void RequestConsensusVote(const consensus::VoteRequestPB* req,
                            consensus::VoteResponsePB* resp,
                            rpc::RpcContext* context) override;

  void ChangeConfig(const consensus::ChangeConfigRequestPB* req,
                    consensus::ChangeConfigResponsePB* resp,
                    rpc::RpcContext* context) override;

  void BulkChangeConfig(const consensus::BulkChangeConfigRequestPB* req,
                        consensus::ChangeConfigResponsePB* resp,
                        rpc::RpcContext* context) override;

  void UnsafeChangeConfig(const consensus::UnsafeChangeConfigRequestPB* req,
                          consensus::UnsafeChangeConfigResponsePB* resp,
                          rpc::RpcContext* context) override;

  void GetNodeInstance(const consensus::GetNodeInstanceRequestPB* req,
                       consensus::GetNodeInstanceResponsePB* resp,
                       rpc::RpcContext* context) override;

  void RunLeaderElection(const consensus::RunLeaderElectionRequestPB* req,
                         consensus::RunLeaderElectionResponsePB* resp,
                         rpc::RpcContext* context) override;

  void LeaderStepDown(const consensus::LeaderStepDownRequestPB* req,
                      consensus::LeaderStepDownResponsePB* resp,
                      rpc::RpcContext* context) override;

  void GetLastOpId(const consensus::GetLastOpIdRequestPB* req,
                   consensus::GetLastOpIdResponsePB* resp,
                   rpc::RpcContext* context) override;

  void GetConsensusState(const consensus::GetConsensusStateRequestPB* req,
                         consensus::GetConsensusStateResponsePB* resp,
                         rpc::RpcContext* context) override;

  void StartTabletCopy(const consensus::StartTabletCopyRequestPB* req,
                       consensus::StartTabletCopyResponsePB* resp,
                       rpc::RpcContext* context) override;

 private:
  server::ServerBase* server_;
  TabletReplicaLookupIf* tablet_manager_;
};

} // namespace tserver
} // namespace kudu

