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

#include "kudu/cdc/cdc_producer.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
// #include "kudu/tserver/service_util.h"
#include "kudu/util/debug/trace_event.h"

namespace kudu {
namespace cdc {

// using consensus::LeaderStatus;
using consensus::RaftConsensus;
using rpc::RpcContext;
using tserver::TSTabletManager;

CDCServiceImpl::CDCServiceImpl(TSTabletManager* tablet_manager,
                               const scoped_refptr<MetricEntity>& metric_entity,
                               const scoped_refptr<::kudu::rpc::ResultTracker>& result_tracker)
    : CDCServiceIf(metric_entity, result_tracker),
      tablet_manager_(tablet_manager) {
}

void CDCServiceImpl::SetupCDC(const SetupCDCRequestPB* req,
                              SetupCDCResponsePB* resp,
                              RpcContext* context) {
  if (!CheckOnline(req, resp, context)) {
    return;
  }

  // TODO: Add implementation.
  context->RespondSuccess();
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
  if (!CheckOnline(req, resp, context)) {
    return;
  }
  const auto& tablet_replica = GetLeaderTabletReplica(req->tablet_id(), resp, context);
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