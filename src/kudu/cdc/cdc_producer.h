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

#include <memory>
#include <string>

#include <boost/functional/hash.hpp>

#include "kudu/cdc/cdc_service.service.h"
#include "kudu/cdc/cdc_service.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/util/result.h"

namespace kudu {
namespace cdc {


struct CDCRecordMetadata {
  CDCRecordType record_type;
  CDCRecordFormat record_format;

  CDCRecordMetadata(CDCRecordType record_type, CDCRecordFormat record_format)
      : record_type(record_type), record_format(record_format) {
  }
};

class CDCProducer {
 public:
  explicit CDCProducer(const std::shared_ptr<tablet::TabletReplica>& tablet_replica)
      : tablet_replica_(tablet_replica) {
  }

  CDCProducer(const CDCProducer&) = delete;
  void operator=(const CDCProducer&) = delete;

  // Get Changes for tablet since given OpId.
  Status GetChanges(const GetChangesRequestPB& req,
                            GetChangesResponsePB* resp);

  // Get CDC record type and format for a given subscriber.
  Result<CDCRecordMetadata> GetRecordMetadataForSubscriber(
      const std::string& subscriber_uuid);

 private:
  // Populate CDC record corresponding to WAL batch in ReplicateMsg.
  Status PopulateWriteRecord(const consensus::ReplicateRefPtr& write_msg,
                                    //  const TxnStatusMap& txn_map,
                                    const CDCRecordMetadata& metadata,
                                     GetChangesResponsePB* resp);



  consensus::OpId GetLastCheckpoint(const std::string& subscriber_uuid);

  std::shared_ptr<tablet::TabletReplica> tablet_replica_;
};

}  // namespace cdc
}  // namespace kudu