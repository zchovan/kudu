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

#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/cdc/cdc_service.proxy.h"
#include "kudu/cdc/cdc_service.pb.h"
#include "kudu/rpc/messenger.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/test_macros.h"
// #include "kudu/cdc/cdc_test_util.h"

using kudu::cdc::GetChangesRequestPB;
using kudu::cdc::GetChangesResponsePB;
using kudu::rpc::RpcController;
using kudu::tablet::TabletReplica;
using kudu::tserver::TabletServerTestBase;
using kudu::tserver::WriteRequestPB;
using kudu::tserver::WriteResponsePB;
using std::string;
using std::unordered_map;
using std::unordered_set;

namespace kudu {
namespace cdc {

class CDCServiceTest : public TabletServerTestBase {
 public:
  CDCServiceTest() : TabletServerTestBase() {}

 protected:
  void SetUp() override {
    TabletServerTestBase::SetUp();
    StartTabletServer(1);
    std::string hostname;
    if (!GetHostname(&hostname).ok()) {
      hostname = "<unknown host>";
    }
    cdc_proxy_ = std::make_unique<CDCServiceProxy>(
        client_messenger_, mini_server_->bound_rpc_addr(), hostname);
  }

  std::unique_ptr<CDCServiceProxy> cdc_proxy_;

  struct TestTableValues {
    int int_val_;
    std::string string_val_;
  };

  unordered_map<int, TestTableValues> test_values_ = {
      {0, {0, "Zero"}},
      {1, {1, "One"}},
      {2, {4, "For"}},
      {3, {9, "Nine"}},
      {4, {16, "sixteen"}},
      {5, {25, "twenty-five"}},
      {32, {512, "five hundred twelve"}},
  };

  const char* const kCDCServiceName = "NotUsedYet";
};

TEST_F(CDCServiceTest, TestGetChanges) {
  // Verify that the tablet exists.
  scoped_refptr<TabletReplica> tablet;
  mini_server_->server()->tablet_manager()->LookupTablet(kTabletId, &tablet);
  ASSERT_TRUE(mini_server_->server()->tablet_manager()->LookupTablet(kTabletId, &tablet));

  // Insert test rows
  WriteRequestPB write_req;
  WriteResponsePB write_resp;

  write_req.set_tablet_id(kTabletId);

  for (const auto& [k, v] : test_values_) {
    AddTestRowToPB(RowOperationsPB::INSERT,
                   schema_,
                   k,
                   v.int_val_,
                   v.string_val_,
                   write_req.mutable_row_operations());
  }

  {
    RpcController controller;
    SCOPED_TRACE(write_req.DebugString());
    ASSERT_OK(proxy_->Write(write_req, &write_resp, &controller));
    SCOPED_TRACE(write_resp.DebugString());
    ASSERT_FALSE(write_resp.has_error());
  }

  GetChangesRequestPB change_req;
  GetChangesResponsePB change_resp;

  change_req.set_tablet_id(kTabletId);
  change_req.set_subscriber_uuid(kCDCServiceName);
  change_req.mutable_from_checkpoint()->mutable_op_id()->set_index(0);
  change_req.mutable_from_checkpoint()->mutable_op_id()->set_term(0);

  // Get CDC changes.
  {
    RpcController rpc;
    SCOPED_TRACE(change_req.DebugString());
    ASSERT_OK(cdc_proxy_->GetChanges(change_req, &change_resp, &rpc));
    SCOPED_TRACE(change_resp.DebugString());
    ASSERT_FALSE(change_resp.has_error());
  }

  ASSERT_EQ(change_resp.records_size(), test_values_.size());

  std::unordered_set<int> unique_keys;
  for (const CDCRecordPB& record : change_resp.records()) {
    ASSERT_EQ(CDCRecordPB::OperationType::CDCRecordPB_OperationType_WRITE, record.operation())
        << "Only write operations should be reported";

    ASSERT_EQ(1, record.keys_size());
    ASSERT_EQ("key", record.keys(0).column_id());
    ASSERT_TRUE(record.keys(0).value().has_int32_value());

    const int row_key = record.keys(0).value().int32_value();
    ASSERT_EQ(1, test_values_.count(row_key));
    const TestTableValues values = test_values_[row_key];
    ASSERT_TRUE(unique_keys.insert(row_key).second) << "Non unique key " << row_key;

    ASSERT_EQ(2, record.changes_size());

    // TODO: Can we build on the order????
    ASSERT_EQ("int_val", record.changes(0).column_id());
    ASSERT_EQ("", record.changes(0).column_id());

    ASSERT_EQ("int_val", record.changes(0).column_id());
    ASSERT_TRUE(record.changes(0).value().has_int32_value());
    ASSERT_EQ(values.int_val_, record.changes(0).value().int32_value());
    ASSERT_EQ("string_val", record.changes(1).column_id());
    ASSERT_TRUE(record.changes(1).value().has_string_value());
    ASSERT_EQ(values.string_val_, record.changes(1).value().string_value());
  }
}

}  // namespace cdc
}  // namespace kudu
