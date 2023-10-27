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

#include "kudu/cdc/cdc_producer.h"


// #include "kudu/common/transaction.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/raft_consensus.h"
// #include "kudu/docdb/docdb.pb.h"
// #include "kudu/docdb/primitive_value.h"
// #include "kudu/docdb/value_type.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
// #include "kudu/tablet/transaction_participant.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"

namespace kudu {
namespace cdc {

using consensus::ReplicateRefPtr;
// using docdb::PrimitiveValue;
// using tablet::TransactionParticipant;


Status CDCProducer::GetChanges(const GetChangesRequestPB& req,
                               GetChangesResponsePB* resp) {
//   const auto& record = VERIFY_RESULT(GetRecordMetadataForSubscriber(req.subscriber_uuid()));
  const auto record = GetRecordMetadataForSubscriber(req.subscriber_uuid());
//  KUDU_RETURN_NOT_OK(record.status().OK());

  consensus::OpId from_op_id;
  if (req.has_from_checkpoint()) {
    from_op_id = req.from_checkpoint().op_id();
  } else {
    from_op_id = GetLastCheckpoint(req.subscriber_uuid());
  }


  std::vector<consensus::ReplicateRefPtr> messages;
  RETURN_NOT_OK(tablet_replica_->consensus()->ReadReplicatedMessages(from_op_id, &messages));

  for (const auto& msg : messages) {
    switch (msg->get()->op_type()) {
      case consensus::OperationType::WRITE_OP:
        RETURN_NOT_OK(PopulateWriteRecord(msg, *record, resp));
        break;

      default:
        // Nothing to do for other operation types.
        break;
    }
  }

  resp->mutable_checkpoint()->mutable_op_id()->CopyFrom(
      messages.empty() ? from_op_id : messages.back()->get()->id());
  return Status::OK();
}

std::string get_as_string(const uint8_t *ptr) {
  const Slice *s = reinterpret_cast<const Slice *>(ptr);
  const std::string str = strings::Utf8SafeCEscape(s->ToString());
  return str;


}

void fill_with_data(const ConstContiguousRow &row, KeyValuePairPB* value_pair, int col_idx)
{
  const ColumnSchema& col=row.schema()->column(col_idx);
  value_pair->set_column_id(col.name());
  auto value = value_pair->mutable_value();
  const uint8_t *ptr=row.cell_ptr(col_idx);
  switch (col.type_info()->type()) {
    case INT32:
      value->set_int32_value((*reinterpret_cast<const int32_t *>(ptr)));
      break;
    case VARCHAR:
      value->set_string_value(get_as_string(ptr));
      break;
    default:
      break;
  }

}

Status CDCProducer::PopulateWriteRecord(const consensus::ReplicateRefPtr& write_msg,
                                        const CDCRecordMetadata& metadata,
                                        GetChangesResponsePB* resp) {
  const auto& batch = write_msg->get()->write_request().row_operations();

  Schema client_schema;
  RETURN_NOT_OK_PREPEND(SchemaFromPB(write_msg->get()->write_request().schema(), &client_schema),
                        "Cannot decode client schema");

  RowOperationsPBDecoder decoder(&batch, &client_schema, &client_schema, nullptr);
  std::vector<DecodedRowOperation> ops;
  Status s = decoder.DecodeOperations<WRITE_OPS>(&ops);

  CDCRecordPB* record = nullptr;

  for (const auto& op : ops) {
    auto row = ConstContiguousRow(&client_schema, op.row_data);
    record = resp->add_records();
    // record->set_operation(CDCRecordPB_OperationType_WRITE);
    //  record->set_time(write_msg->get);
    for (size_t col_idx = 0; col_idx < row.schema()->num_key_columns(); col_idx++) {
      KeyValuePairPB* keys = record->add_keys();
      fill_with_data(row, keys, col_idx);
    }

    for (size_t col_idx = row.schema()->num_key_columns(); col_idx < row.schema()->num_columns(); col_idx++) {
      KeyValuePairPB* changes = record->add_changes();
      fill_with_data(row, changes, col_idx);
    }


  }
  return Status::OK();
}



Result<CDCRecordMetadata> CDCProducer::GetRecordMetadataForSubscriber(
    const std::string& subscriber_uuid) {
  // TODO: This should read details from cdc_state table.
  return CDCRecordMetadata(CDCRecordType::CHANGE, CDCRecordFormat::WAL);
}

consensus::OpId CDCProducer::GetLastCheckpoint(const std::string& subscriber_uuid) {
  // TODO: Read value from cdc_subscribers table.
  consensus::OpId op_id;
  op_id.set_term(0);
  op_id.set_index(0);
  return op_id;
}

} // namespace cdc
} // namespace kudu
