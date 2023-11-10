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

#include <thread>

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
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/rpc/messenger.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/client/table_creator-internal.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/master.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.proxy.h"
// #include "kudu/cdc/cdc_test_util.h"

using kudu::KuduPartialRow;
using kudu::cdc::CreateCDCStreamRequestPB;
using kudu::cdc::CreateCDCStreamResponsePB;
using kudu::cdc::GetChangesRequestPB;
using kudu::cdc::GetChangesResponsePB;
using kudu::client::KuduClient;
using kudu::client::KuduInsert;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::itest::SimpleIntKeyKuduSchema;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using kudu::tablet::TabletReplica;
using kudu::tserver::MiniTabletServer;
using kudu::tserver::TabletServerTestBase;
using kudu::tserver::WriteRequestPB;
using kudu::tserver::WriteResponsePB;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using kudu::master::CatalogManager;

namespace kudu {
namespace cdc {

class CDCiServiceITest : public KuduTest {
 public:
  CDCiServiceITest() : schema_(GetSimpleTestSchema()) {}
  void SetUp() override {
    KuduTest::SetUp();

    MessengerBuilder bld("client");
    ASSERT_OK(bld.Build(&client_messenger_));
  }

  void StartCluster(int num_masters, int num_tservers) {
    InternalMiniClusterOptions opts;
    opts.num_tablet_servers = kNumTabletServers;
    opts.num_masters = num_masters;
    NO_FATALS(StartCluster(std::move(opts)));
  }

 protected:
  void StartCluster(InternalMiniClusterOptions opts) {
    cluster_.reset(new InternalMiniCluster(env_, std::move(opts)));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));

    for (int i = 0; i < kNumTabletServers; ++i) {
      auto* ms = cluster_->mini_tablet_server(i);
      proxies_.emplace_back(std::make_unique<CDCServiceProxy>(

          client_messenger_, ms->bound_rpc_addr(), ms->bound_rpc_addr().host()));
       tserver_uuid_to_index_[ms->uuid()] = i;
       
    }
  }

  KuduSchema GetSimpleTestSchema() {
    KuduSchema s;
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("int_val")->Type(client::KuduColumnSchema::INT32)->NotNull();
    b.AddColumn("string_val")->Type(client::KuduColumnSchema::STRING);
    b.Build(&s);
    EXPECT_OK(b.Build(&s));
    return s;
  }

  const KuduSchema schema_;

  unique_ptr<InternalMiniCluster> cluster_;
  client::sp::shared_ptr<KuduClient> client_;
  std::shared_ptr<Messenger> client_messenger_;
  std::vector<std::unique_ptr<CDCServiceProxy>> proxies_;
  std::map<std::string, int>  tserver_uuid_to_index_;

  void CreateTestTable() {
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    ASSERT_OK(table_creator->table_name(kTableName)
                  .schema(&schema_)
                  .num_replicas(kRf)
                  .set_range_partition_columns({})
                  .Create());
  }

  static constexpr const char* kTableName = "test-table";
  static constexpr int kNumTabletServers = 3;
  static constexpr int kRf = 3;

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

  void WriteData(const unordered_map<int, TestTableValues>& data) {
    client::sp::shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    client::sp::shared_ptr<KuduSession> session(table->client()->NewSession());
    for (const auto& [key, vals] : data) {
      KuduInsert* insert = table->NewInsert();
      KuduPartialRow* row = insert->mutable_row();
      ASSERT_OK(row->SetInt32(0, key));
      ASSERT_OK(row->SetInt32(1, vals.int_val_));
      ASSERT_OK(row->SetString(2, vals.string_val_));
      ASSERT_OK(session->Apply(insert));
    }
    ASSERT_OK(session->Flush());
  }

  void CreateCDCStream(int tserver_idx, const std::string& table_id, std::string& cdc_stream_id) {
    RpcController controller;
    controller.set_timeout(
        kudu::MonoDelta::FromSeconds(3600));  // A sensible value makes debugging very anoying.
    CreateCDCStreamRequestPB req;
    CreateCDCStreamResponsePB resp;
    req.set_table_id(table_id);
    ASSERT_OK(proxies_[tserver_idx]->CreateCDCStream(req, &resp, &controller));
    ASSERT_TRUE(resp.has_stream_id());
    cdc_stream_id = resp.stream_id();
  }

  void GetChanges(int tserver_idx,
                  const string& table_id,
                  const string& cdc_stream_id,
                  GetChangesResponsePB& change_resp,
                  const kudu::consensus::OpId& opId) {
    RpcController controller;
    controller.set_timeout(
        kudu::MonoDelta::FromSeconds(3600));  // A sensible value makes debugging very anoying.
    GetChangesRequestPB change_req;

    change_req.set_tablet_id(table_id);
    change_req.set_stream_id(cdc_stream_id);
    *change_req.mutable_from_checkpoint()->mutable_op_id() = opId;
    auto status = proxies_[tserver_idx]->GetChanges(change_req, &change_resp, &controller);
    ASSERT_OK(status);
  }
  static kudu::consensus::OpId MakeOpid(int64 term, int64 index) {
    kudu::consensus::OpId res;
    res.set_term(term);
    res.set_index(index);
    return res;
  }

  void GetChangesNoError(int tserver_idx,
                         const string& table_id,
                         const string& cdc_stream_id,
                         const kudu::consensus::OpId& opId,
                         std::vector<CDCRecordPB>& changes) {
    GetChangesResponsePB resp;
    ASSERT_NO_FATAL_FAILURE(GetChanges(tserver_idx, table_id, cdc_stream_id, resp, opId));
    ASSERT_FALSE(resp.has_error())
        << (resp.has_error() ? resp.error().DebugString() : std::string(""));
    changes.assign(resp.records().begin(), resp.records().end());
  }

  struct TabletInfo {
    std::string id_;
    int leader_tserver_idx_;
  };

  void GetTabletsForTable(std::string table_id, std::vector<TabletInfo>& tablets) {
    tablets.clear();

    master::GetTableLocationsRequestPB req;
    master::GetTableLocationsResponsePB resp;
    rpc::RpcController controller;
    req.mutable_table()->set_table_name(table_id);
    req.set_intern_ts_infos_in_response(true);

    ASSERT_OK(cluster_->master_proxy()->GetTableLocations(req, &resp, &controller));
    for (const master::TabletLocationsPB& loc : resp.tablet_locations()) {
      for (const auto& replica : loc.interned_replicas()) {
        if (replica.role() == consensus::RaftPeerPB::LEADER) {
          auto tserver_id = resp.ts_infos(replica.ts_info_idx()).permanent_uuid();
          ASSERT_TRUE(tserver_uuid_to_index_.count(tserver_id));
          tablets.push_back({loc.tablet_id(), tserver_uuid_to_index_[tserver_id]});
        }
      }
    }
  }
};

class CDCiServiceITestBasic : public CDCiServiceITest {
 public:
  void SetUp() override {
    CDCiServiceITest::SetUp();
    StartCluster(1, 3);
    CreateTestTable();
    WriteData(test_values_);
  }
};

class CDCiServiceITest3Master : public CDCiServiceITest {
 public:
  void SetUp() override {
    CDCiServiceITest::SetUp();
    StartCluster(3, 3);
    CreateTestTable();
    WriteData(test_values_);
  }
};

TEST_F(CDCiServiceITestBasic, CreateStreamId) {
  std::string stream_id;
  ASSERT_NO_FATAL_FAILURE(CreateCDCStream(0, kTableName, stream_id));
  ASSERT_EQ("fake_stream_id", stream_id);
}

TEST_F(CDCiServiceITest3Master, CreateStreamId) {
  std::string stream_id;
  ASSERT_NO_FATAL_FAILURE(CreateCDCStream(0, kTableName, stream_id));
  ASSERT_EQ("fake_stream_id", stream_id);

  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  cluster_->mini_master(leader_idx)->Shutdown();
  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT_EVENTUALLY([&] {
    ASSERT_NO_FATAL_FAILURE(CreateCDCStream(0, kTableName, stream_id));
    ASSERT_EQ("fake_stream_id", stream_id);
  });   
}


TEST_F(CDCiServiceITestBasic, GetChangesFromBegining) {
  std::string stream_id;
  ASSERT_NO_FATAL_FAILURE(CreateCDCStream(0, kTableName, stream_id));
  ASSERT_EQ("fake_stream_id", stream_id);
  std::vector<CDCRecordPB> changes;
  
  std::vector<TabletInfo> tablets;
  ASSERT_NO_FATAL_FAILURE(GetTabletsForTable(kTableName, tablets));
  ASSERT_EQ(1, tablets.size());
  ASSERT_NO_FATAL_FAILURE(GetChangesNoError(tablets[0].leader_tserver_idx_, tablets[0].id_, stream_id, MakeOpid(0,0), changes ));
  ASSERT_EQ(test_values_.size(), changes.size());

}

}  // namespace cdc
}  // namespace kudu