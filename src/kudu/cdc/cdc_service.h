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

#include "kudu/cdc/cdc_service.service.h"

#include "kudu/cdc/cdc_producer.h"
#include "kudu/cdc/cdc_service.pb.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/metrics.h"
#include "kudu/util/service_util.h"
#include "kudu/util/result.h"

namespace kudu {
namespace cdc {

using consensus::RaftPeerPB;


class CDCServiceImpl : public CDCServiceIf {
 public:
  CDCServiceImpl(tserver::TSTabletManager* tablet_manager,
                 const scoped_refptr<MetricEntity>& metric_entity,
                 const scoped_refptr<::kudu::rpc::ResultTracker>& result_tracker);

  CDCServiceImpl(const CDCServiceImpl&) = delete;
  void operator=(const CDCServiceImpl&) = delete;

  void SetupCDC(const SetupCDCRequestPB* req,
                SetupCDCResponsePB* resp,
                rpc::RpcContext* rpc) override;
  void ListTablets(const ListTabletsRequestPB *req,
                   ListTabletsResponsePB* resp,
                   rpc::RpcContext* rpc) override;
  void GetChanges(const GetChangesRequestPB* req,
                  GetChangesResponsePB* resp,
                  rpc::RpcContext* rpc) override;
  void GetCheckpoint(const GetCheckpointRequestPB* req,
                     GetCheckpointResponsePB* resp,
                     rpc::RpcContext* rpc) override;

 private:
  template <class ReqType, class RespType>
  bool CheckOnline(const ReqType* req, RespType* resp, rpc::RpcContext* rpc) {
    TRACE("Received RPC $0: $1", rpc->ToString(), req->DebugString());
    if (PREDICT_FALSE(!tablet_manager_)) {
      SetupErrorAndRespond(resp->mutable_error(),
                            Status::ServiceUnavailable("Tablet server is not running"),
                           CDCErrorPB::NOT_RUNNING,
                           rpc);
      return false;
    }
    return true;
  }

  template <class RespType>
  Result<scoped_refptr<tablet::TabletReplica>> GetLeaderTabletReplica(
      const std::string& tablet_id,
      RespType* resp,
      rpc::RpcContext* rpc) {
    scoped_refptr<tablet::TabletReplica> replica;
    Status status = tablet_manager_->GetTabletReplica(tablet_id, &replica);
    if (PREDICT_FALSE(!status.ok())) {
      CDCErrorPB::Code code = status.IsNotFound() ?
          CDCErrorPB::TABLET_NOT_FOUND : CDCErrorPB::TABLET_NOT_RUNNING;
      SetupErrorAndRespond(resp->mutable_error(), status, code, rpc);
      return status;
    }

    // Check RUNNING state.
    status = replica->CheckRunning();
    if (PREDICT_FALSE(!status.ok())) {
      Status s = Status::IllegalState("Tablet not running");
      SetupErrorAndRespond(resp->mutable_error(), s, CDCErrorPB::TABLET_NOT_RUNNING, rpc);
      return s;
    }

    // Check if tablet peer is leader.
    RaftPeerPB::Role leader_role= replica->consensus()->role();
    if (leader_role != RaftPeerPB::Role::RaftPeerPB_Role_LEADER) {
      // No records to read.
    //   if (leader_status == consensus::LeaderStatus::NOT_LEADER) {
    //     // TODO: Change this to provide new leader
    //   }
      Status s = Status::IllegalState("Tablet Server is not leader");
      SetupErrorAndRespond(
          resp->mutable_error(),
          s,
          CDCErrorPB::NOT_LEADER,
          rpc);
      return s;
    }
    return replica;
  }

  tserver::TSTabletManager* tablet_manager_;
};

}  // namespace cdc
}  // namespace kudu