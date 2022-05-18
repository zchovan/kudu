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

#include "kudu/tools/ksck.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/common.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rebalance/cluster_status.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tools/ksck_checksum.h"
#include "kudu/tools/ksck_results.h"
#include "kudu/transactions/txn_status_tablet.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(checksum_scan);
DECLARE_int32(checksum_idle_timeout_sec);
DECLARE_int32(max_progress_report_wait_ms);
DECLARE_string(color);
DECLARE_string(flags_categories_to_check);
DECLARE_string(ksck_format);
DECLARE_uint32(truncate_server_csv_length);

using kudu::cluster_summary::ConsensusConfigType;
using kudu::cluster_summary::ConsensusState;
using kudu::cluster_summary::ConsensusStateMap;
using kudu::cluster_summary::ReplicaSummary;
using kudu::cluster_summary::ServerHealth;
using kudu::cluster_summary::ServerHealthSummary;
using kudu::cluster_summary::TableSummary;
using kudu::cluster_summary::TabletSummary;
using kudu::server::GetFlagsResponsePB;
using kudu::tablet::TabletDataState;
using kudu::transactions::TxnStatusTablet;

using std::make_shared;
using std::ostringstream;
using std::shared_ptr;
using std::static_pointer_cast;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

class MockKsckMaster : public KsckMaster {
 public:
  explicit MockKsckMaster(const string& address, const string& uuid, bool is_get_flags_available)
      : KsckMaster(address),
        fetch_info_status_(Status::OK()),
        is_get_flags_available_(is_get_flags_available) {
    uuid_ = uuid;
    version_ = "mock-version";
    if (is_get_flags_available_) {
      for (size_t cat = FlagsCategory::MIN; cat <= FlagsCategory::MAX; ++cat) {
        flags_by_category_[cat].flags.emplace();
      }
    }
  }

  Status Init() override {
    return Status::OK();
  }

  Status FetchInfo() override {
    if (fetch_info_status_.ok()) {
      state_ = KsckFetchState::FETCHED;
    } else {
      state_ = KsckFetchState::FETCH_FAILED;
    }
    return fetch_info_status_;
  }

  Status FetchConsensusState() override {
    return fetch_cstate_status_;
  }

  Status FetchFlags(const std::vector<FlagsCategory>& categories) override {
    for (const auto cat : categories) {
      if (is_get_flags_available_) {
        flags_by_category_[cat].state = KsckFetchState::FETCHED;
      } else {
        flags_by_category_[cat].state = KsckFetchState::FETCH_FAILED;
      }
    }
    return is_get_flags_available_
        ? Status::OK() : Status::RemoteError("GetFlags not available");
  }

  // Public because the unit tests mutate these variables directly.
  Status fetch_info_status_;
  Status fetch_cstate_status_;
  using KsckMaster::uuid_;
  using KsckMaster::cstate_;
  using KsckMaster::flags_by_category_;
  using KsckMaster::version_;
 private:
  const bool is_get_flags_available_;
};

class MockKsckTabletServer : public KsckTabletServer {
 public:
  explicit MockKsckTabletServer(const string& uuid, bool is_get_flags_available)
      : KsckTabletServer(uuid),
        fetch_info_status_(Status::OK()),
        fetch_info_health_(ServerHealth::HEALTHY),
        address_("<mock>"),
        is_get_flags_available_(is_get_flags_available) {
    version_ = "mock-version";
    if (is_get_flags_available_) {
      for (size_t cat = FlagsCategory::MIN; cat <= FlagsCategory::MAX; ++cat) {
        flags_by_category_[cat].flags.emplace();
      }
    }
  }

  Status FetchInfo(ServerHealth* health) override {
    CHECK(health);
    *health = fetch_info_health_;
    timestamp_ = 12345;
    if (fetch_info_status_.ok()) {
      state_ = KsckFetchState::FETCHED;
    } else {
      state_ = KsckFetchState::FETCH_FAILED;
    }
    return fetch_info_status_;
  }

  Status FetchConsensusState(ServerHealth* /*health*/) override {
    return Status::OK();
  }

  Status FetchFlags(const std::vector<FlagsCategory>& categories) override {
    for (const auto cat : categories) {
      if (is_get_flags_available_) {
        flags_by_category_[cat].state = KsckFetchState::FETCHED;
      } else {
        flags_by_category_[cat].state = KsckFetchState::FETCH_FAILED;
      }
    }
    return is_get_flags_available_
        ? Status::OK() : Status::RemoteError("GetFlags not available");
  }

  void FetchCurrentTimestampAsync() override {}

  Status FetchCurrentTimestamp() override {
    return Status::OK();
  }

  void FetchQuiescingInfo() override {}

  void RunTabletChecksumScanAsync(
      const std::string& tablet_id,
      const Schema& /*schema*/,
      const KsckChecksumOptions& /*options*/,
      shared_ptr<KsckChecksumManager> manager) override {
    manager->ReportProgress(checksum_progress_, 2 * checksum_progress_);
    if (checksum_progress_ > 0) {
      manager->ReportResult(tablet_id, uuid_, Status::OK(), checksum_);
    }
  }

  std::string address() const override {
    return address_;
  }

  // Public because the unit tests mutate these variables directly.
  Status fetch_info_status_;
  ServerHealth fetch_info_health_;
  // The fake checksum for replicas on this mock server.
  uint64_t checksum_ = 0;
  // The fake progress amount for this mock server, used to mock checksum
  // progress for this server.
  int64_t checksum_progress_ = 10;
  using KsckTabletServer::flags_by_category_;
  using KsckTabletServer::location_;
  using KsckTabletServer::version_;

 private:
  const string address_;
  const bool is_get_flags_available_;
};

class MockKsckCluster : public KsckCluster {
 public:
  MockKsckCluster()
      : fetch_info_status_(Status::OK()) {
  }

  virtual Status Connect() override {
    return fetch_info_status_;
  }

  virtual Status RetrieveTabletServers() override {
    return Status::OK();
  }

  virtual Status RetrieveTablesList() override {
    return Status::OK();
  }

  virtual Status RetrieveAllTablets() override {
    return Status::OK();
  }

  virtual Status RetrieveTabletsList(const shared_ptr<KsckTable>& /* unused */) override {
    return Status::OK();
  }

  // Public because the unit tests mutate these variables directly.
  Status fetch_info_status_;
  using KsckCluster::masters_;
  using KsckCluster::tables_;
  using KsckCluster::tablet_servers_;
  using KsckCluster::txn_sys_table_;
};

class KsckTest : public KuduTest {
 public:
  KsckTest()
      : cluster_(new MockKsckCluster()),
        ksck_(new Ksck(cluster_, &err_stream_)) {
    FLAGS_color = "never";
  }

  void SetUp() override {
    // Set up the master consensus state.
    consensus::ConsensusStatePB cstate;
    cstate.set_current_term(0);
    cstate.set_leader_uuid("master-id-0");
    for (int i = 0; i < 3; i++) {
      auto* peer = cstate.mutable_committed_config()->add_peers();
      peer->set_member_type(consensus::RaftPeerPB::VOTER);
      peer->set_permanent_uuid(Substitute("master-id-$0", i));
    }

    for (int i = 0; i < 3; i++) {
      const string uuid = Substitute("master-id-$0", i);
      const string addr = Substitute("master-$0", i);
      auto master = make_shared<MockKsckMaster>(addr, uuid, IsGetFlagsAvailable());
      master->cstate_ = cstate;
      cluster_->masters_.push_back(master);
    }

    KsckCluster::TSMap tablet_servers;
    for (int i = 0; i < 3; i++) {
      string name = Substitute("ts-id-$0", i);
      auto ts = make_shared<MockKsckTabletServer>(name, IsGetFlagsAvailable());
      InsertOrDie(&tablet_servers, ts->uuid(), ts);
    }
    cluster_->tablet_servers_.swap(tablet_servers);
  }

 protected:
  // Returns the expected summary for a table with the given tablet states.
  std::string ExpectedTableSummary(const string& table_name,
                                   int replication_factor,
                                   int healthy_tablets,
                                   int recovering_tablets,
                                   int underreplicated_tablets,
                                   int consensus_mismatch_tablets,
                                   int unavailable_tablets) {
    TableSummary table_summary;
    table_summary.name = table_name;
    table_summary.replication_factor = replication_factor;
    table_summary.healthy_tablets = healthy_tablets;
    table_summary.recovering_tablets = recovering_tablets;
    table_summary.underreplicated_tablets = underreplicated_tablets;
    table_summary.consensus_mismatch_tablets = consensus_mismatch_tablets;
    table_summary.unavailable_tablets = unavailable_tablets;
    std::ostringstream oss;
    PrintTableSummaries({ table_summary }, "table", oss);
    return oss.str();
  }

  void CreateDefaultAssignmentPlan(int tablets_count) {
    SCOPED_CLEANUP({
        // This isn't necessary for correctness, but the tests were all
        // written to expect a reversed order and doing that here is more
        // convenient than rewriting many ASSERTs.
        std::reverse(assignment_plan_.begin(), assignment_plan_.end());
      });
    while (tablets_count > 0) {
      for (const auto& entry : cluster_->tablet_servers_) {
        if (tablets_count-- == 0) return;
        assignment_plan_.push_back(entry.second->uuid());
      }
    }
  }

  void CreateOneTableOneTablet(bool create_txn_status_table = false) {
    NO_FATALS(CreateDefaultAssignmentPlan(create_txn_status_table ? 2 : 1));

    auto table = CreateAndAddTable("test", 1);
    auto tablet(make_shared<KsckTablet>(
        table.get(), "tablet-id-1", Partition{}));
    NO_FATALS(CreateAndFillTablet(tablet, 1, true, true));
    table->set_tablets({ tablet });

    if (create_txn_status_table) {
      auto sys_table = CreateAndAddTxnStatusTable(1);
      auto sys_tablet(make_shared<KsckTablet>(
          sys_table.get(), "sys-tablet-id-1", Partition{}));
      NO_FATALS(CreateAndFillTablet(sys_tablet, 1, true, true));
      sys_table->set_tablets({ sys_tablet });
    }
  }

  void CreateOneSmallReplicatedTable(const string& table_name = "test",
                                     const string& tablet_id_prefix = "") {
    int num_replicas = 3;
    int num_tablets = 3;
    CreateDefaultAssignmentPlan(num_replicas * num_tablets);
    auto table = CreateAndAddTable(table_name, num_replicas);

    vector<shared_ptr<KsckTablet>> tablets;
    for (int i = 0; i < num_tablets; i++) {
      auto tablet(make_shared<KsckTablet>(
          table.get(),
          Substitute("$0tablet-id-$1", tablet_id_prefix, i),
          Partition{}));
      CreateAndFillTablet(tablet, num_replicas, true, true);
      tablets.push_back(std::move(tablet));
    }
    table->set_tablets(tablets);
  }

  void CreateOneSmallReplicatedTableWithTabletNotRunning() {
    int num_replicas = 3;
    int num_tablets = 3;
    CreateDefaultAssignmentPlan(num_replicas * num_tablets);
    auto table = CreateAndAddTable("test", num_replicas);

    vector<shared_ptr<KsckTablet>> tablets;
    for (int i = 0; i < num_tablets; i++) {
      auto tablet(make_shared<KsckTablet>(
          table.get(), Substitute("tablet-id-$0", i), Partition{}));
      CreateAndFillTablet(tablet, num_replicas, true, i != 0);
      tablets.push_back(std::move(tablet));
    }
    table->set_tablets(tablets);
  }

  void CreateOneOneTabletReplicatedBrokenTable() {
    // We're placing only two tablets, the 3rd goes nowhere.
    CreateDefaultAssignmentPlan(2);

    auto table = CreateAndAddTable("test", 3);

    auto tablet(make_shared<KsckTablet>(table.get(), "tablet-id-1", Partition{}));
    CreateAndFillTablet(tablet, 2, false, true);
    table->set_tablets({ tablet });
  }

  shared_ptr<KsckTable> CreateAndAddTxnStatusTable(int num_replicas) {
    auto table(make_shared<KsckTable>(
        TxnStatusTablet::kTxnStatusTableName, TxnStatusTablet::kTxnStatusTableName,
        TxnStatusTablet::GetSchema(), num_replicas));
    cluster_->txn_sys_table_ = table;
    return table;
  }

  shared_ptr<KsckTable> CreateAndAddTable(const string& id_and_name, int num_replicas) {
    auto table(make_shared<KsckTable>(
        id_and_name, id_and_name, Schema(), num_replicas));
    cluster_->tables_.push_back(table);
    return table;
  }

  void CreateAndFillTablet(shared_ptr<KsckTablet>& tablet, int num_replicas,
                           bool has_leader, bool is_running) {
    {
      vector<shared_ptr<KsckTabletReplica>> replicas;
      if (has_leader) {
        NO_FATALS(CreateReplicaAndAdd(&replicas, tablet->id(), true, is_running));
        num_replicas--;
      }
      for (int i = 0; i < num_replicas; i++) {
        NO_FATALS(CreateReplicaAndAdd(&replicas, tablet->id(), false, is_running));
      }
      tablet->set_replicas(std::move(replicas));
    }

    // Set up the consensus state on each tablet server.
    consensus::ConsensusStatePB cstate;
    cstate.set_current_term(0);
    for (const auto& replica : tablet->replicas()) {
      if (replica->is_leader()) {
        cstate.set_leader_uuid(replica->ts_uuid());
      }
      auto* peer = cstate.mutable_committed_config()->add_peers();
      peer->set_member_type(consensus::RaftPeerPB::VOTER);
      peer->set_permanent_uuid(replica->ts_uuid());
    }
    for (const auto& replica : tablet->replicas()) {
      shared_ptr<MockKsckTabletServer> ts =
        static_pointer_cast<MockKsckTabletServer>(cluster_->tablet_servers_.at(replica->ts_uuid()));
      InsertIfNotPresent(&ts->tablet_consensus_state_map_,
                         std::make_pair(replica->ts_uuid(), tablet->id()),
                         cstate);
    }
  }

  void CreateReplicaAndAdd(vector<shared_ptr<KsckTabletReplica>>* replicas,
                           const string& tablet_id,
                           bool is_leader,
                           bool is_running) {
    shared_ptr<KsckTabletReplica> replica(
        new KsckTabletReplica(assignment_plan_.back(), is_leader, true));
    shared_ptr<MockKsckTabletServer> ts = static_pointer_cast<MockKsckTabletServer>(
            cluster_->tablet_servers_.at(assignment_plan_.back()));

    assignment_plan_.pop_back();
    replicas->push_back(replica);

    // Add the equivalent replica on the tablet server.
    tablet::TabletStatusPB pb;
    pb.set_tablet_id(tablet_id);
    pb.set_table_name("fake-table");
    pb.set_state(is_running ? tablet::RUNNING : tablet::FAILED);
    pb.set_tablet_data_state(TabletDataState::TABLET_DATA_UNKNOWN);
    InsertOrDie(&ts->tablet_status_map_, tablet_id, pb);
  }

  Status RunKsck() {
    auto c = MakeScopedCleanup([this]() {
        LOG(INFO) << "Ksck output:\n" << err_stream_.str();
      });
    return ksck_->RunAndPrintResults();
  }

  const string KsckResultsToJsonString(int sections = PrintSections::ALL_SECTIONS) {
    ostringstream json_stream;
    ksck_->results().PrintJsonTo(PrintMode::JSON_COMPACT,
                                 sections,
                                 json_stream);
    return json_stream.str();
  }

  virtual bool IsGetFlagsAvailable() const {
    return true;
  }

  shared_ptr<MockKsckCluster> cluster_;
  shared_ptr<Ksck> ksck_;
  // This is used as a stack. First the unit test is responsible to create a plan to follow, that
  // is the order in which each replica of each tablet will be assigned, starting from the end.
  // So if you have 2 tablets with num_replicas=3 and 3 tablet servers, then to distribute evenly
  // you should have a list that looks like ts1,ts2,ts3,ts3,ts2,ts1 so that the two LEADERS, which
  // are assigned first, end up on ts1 and ts3.
  vector<string> assignment_plan_;

  std::ostringstream err_stream_;
};

class GetFlagsUnavailableKsckTest : public KsckTest {
 protected:
  bool IsGetFlagsAvailable() const override {
    return false;
  }
};

// Helpful macros for checking JSON fields vs. expected values.
// In all cases, the meaning of the parameters are as follows:
// 'reader' is the JsonReader that owns the parsed JSON data.
// 'value' is the rapidjson::Value* containing the field, or, if 'field'
// is nullptr, the field itself.
// 'field' is a const char* naming the field of 'value' to check.
// If it is null, the field value is extracted from 'value' directly.
// 'expected' is the expected value.
#define EXPECT_JSON_STRING_FIELD(reader, value, field, expected) do { \
  string actual; \
  ASSERT_OK((reader).ExtractString((value), (field), &actual)); \
  EXPECT_EQ((expected), actual); \
} while (0)

#define EXPECT_JSON_INT_FIELD(reader, value, field, expected) do { \
  int64_t actual; \
  ASSERT_OK((reader).ExtractInt64((value), (field), &actual)); \
  EXPECT_EQ((expected), actual); \
} while (0)

#define EXPECT_JSON_BOOL_FIELD(reader, value, field, expected) do { \
  bool actual; \
  ASSERT_OK((reader).ExtractBool((value), (field), &actual)); \
  EXPECT_EQ((expected), actual); \
} while (0)

#define EXPECT_JSON_FIELD_NOT_PRESENT(reader, value, field) do { \
  int64_t unused; \
  ASSERT_TRUE((reader).ExtractInt64((value), (field), &unused).IsNotFound()); \
} while (0)

// 'array' is a vector<const rapidjson::Value*> into which the array elements
// will be extracted.
// 'exp_size' is the expected size of the vector after extraction.
#define EXTRACT_ARRAY_CHECK_SIZE(reader, value, field, array, exp_size) do { \
  ASSERT_OK((reader).ExtractObjectArray((value), (field), &(array))); \
  ASSERT_EQ(exp_size, (array).size()); \
} while (0)

void CheckJsonVsServerHealthSummaries(
    const JsonReader& r,
    const string& key,
    const boost::optional<vector<ServerHealthSummary>>& summaries) {
  if (!summaries || summaries->empty()) {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, r.root(), key.c_str());
    return;
  }
  vector<const rapidjson::Value*> health;
  EXTRACT_ARRAY_CHECK_SIZE(r, r.root(), key.c_str(), health, summaries->size());
  for (int i = 0; i < summaries->size(); i++) {
    const auto& summary = (*summaries)[i];
    const auto* server = health[i];
    EXPECT_JSON_STRING_FIELD(r, server, "uuid", summary.uuid);
    EXPECT_JSON_STRING_FIELD(r, server, "address", summary.address);
    EXPECT_JSON_STRING_FIELD(r, server, "health", ServerHealthToString(summary.health));
    EXPECT_JSON_STRING_FIELD(r, server, "status", summary.status.ToString());
    if (!summary.ts_location.empty()) {
      EXPECT_JSON_STRING_FIELD(r, server, "location", summary.ts_location);
    }
  }
}

const string ConsensusConfigTypeToString(ConsensusConfigType t) {
  switch (t) {
    case ConsensusConfigType::COMMITTED:
      return "COMMITTED";
    case ConsensusConfigType::PENDING:
      return "PENDING";
    case ConsensusConfigType::MASTER:
      return "MASTER";
    default:
      LOG(FATAL) << "unknown ConsensusConfigType";
  }
}

void CheckJsonVsConsensusState(const JsonReader& r,
                               const rapidjson::Value* cstate,
                               const ConsensusState& ref_cstate) {
  EXPECT_JSON_STRING_FIELD(r, cstate, "type",
                           ConsensusConfigTypeToString(ref_cstate.type));
  if (ref_cstate.leader_uuid) {
    EXPECT_JSON_STRING_FIELD(r, cstate, "leader_uuid", ref_cstate.leader_uuid);
  } else {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, cstate, "leader_uuid");
  }
  if (ref_cstate.term) {
    EXPECT_JSON_INT_FIELD(r, cstate, "term", ref_cstate.term);
  } else {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, cstate, "term");
  }
  if (ref_cstate.opid_index) {
    EXPECT_JSON_INT_FIELD(r, cstate, "opid_index", ref_cstate.opid_index);
  } else {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, cstate, "opid_index");
  }
  // Check voters.
  if (ref_cstate.voter_uuids.empty()) {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, cstate, "voter_uuids");
  } else {
    const vector<string> ref_voter_uuids(ref_cstate.voter_uuids.begin(),
                                         ref_cstate.voter_uuids.end());
    vector<const rapidjson::Value*> voter_uuids;
    EXTRACT_ARRAY_CHECK_SIZE(r, cstate, "voter_uuids",
                             voter_uuids, ref_voter_uuids.size());
    for (int j = 0; j < voter_uuids.size(); j++) {
      EXPECT_JSON_STRING_FIELD(r, voter_uuids[j], nullptr, ref_voter_uuids[j]);
    }
  }
  // Check non-voters.
  if (ref_cstate.non_voter_uuids.empty()) {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, cstate, "non_voter_uuids");
  } else {
    const vector<string> ref_non_voter_uuids(ref_cstate.non_voter_uuids.begin(),
                                             ref_cstate.non_voter_uuids.end());
    vector<const rapidjson::Value*> non_voter_uuids;
    EXTRACT_ARRAY_CHECK_SIZE(r, cstate, "nonvoter_uuids",
                             non_voter_uuids, ref_non_voter_uuids.size());
    for (int j = 0; j < non_voter_uuids.size(); j++) {
      EXPECT_JSON_STRING_FIELD(r, non_voter_uuids[j], nullptr, ref_non_voter_uuids[j]);
    }
  }
}

void CheckJsonVsReplicaSummary(const JsonReader& r,
                               const rapidjson::Value* replica,
                               const ReplicaSummary& ref_replica) {
  EXPECT_JSON_STRING_FIELD(r, replica, "ts_uuid", ref_replica.ts_uuid);
  if (ref_replica.ts_address) {
    EXPECT_JSON_STRING_FIELD(r, replica, "ts_address", ref_replica.ts_address);
  } else {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, replica, "ts_address");
  }
  EXPECT_JSON_BOOL_FIELD(r, replica, "is_leader", ref_replica.is_leader);
  EXPECT_JSON_BOOL_FIELD(r, replica, "is_voter", ref_replica.is_voter);
  EXPECT_JSON_BOOL_FIELD(r, replica, "ts_healthy", ref_replica.ts_healthy);
  EXPECT_JSON_STRING_FIELD(r, replica, "state", tablet::TabletStatePB_Name(ref_replica.state));
  // The only thing ksck expects from the status_pb is the data state,
  // so it's all we check (even though the other info is nice to have).
  if (ref_replica.status_pb) {
    const rapidjson::Value* status_pb;
    ASSERT_OK(r.ExtractObject(replica, "status_pb", &status_pb));
    EXPECT_JSON_STRING_FIELD(
        r,
        status_pb,
        "tablet_data_state",
        tablet::TabletDataState_Name(ref_replica.status_pb->tablet_data_state()));
  } else {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, replica, "status_pb");
  }
  if (ref_replica.consensus_state) {
    const rapidjson::Value* cstate;
    ASSERT_OK(r.ExtractObject(replica, "consensus_state", &cstate));
    CheckJsonVsConsensusState(r, cstate, *ref_replica.consensus_state);
  } else {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, replica, "consensus_state");
  }
}

void CheckJsonVsMasterConsensus(const JsonReader& r,
                                bool ref_conflict,
                                const boost::optional<ConsensusStateMap>& ref_cstates) {
  if (!ref_cstates || ref_cstates->empty()) {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, r.root(), "master_consensus_states");
    return;
  }
  EXPECT_JSON_BOOL_FIELD(r, r.root(), "master_consensus_conflict", ref_conflict);
  vector<const rapidjson::Value*> cstates;
  EXTRACT_ARRAY_CHECK_SIZE(r, r.root(), "master_consensus_states",
                           cstates, ref_cstates->size());
  int i = 0;
  for (const auto& entry : *ref_cstates) {
    CheckJsonVsConsensusState(r, cstates[i++], entry.second);
  }
}

void CheckJsonVsTableSummaries(const JsonReader& r,
                               const string& key,
                               const boost::optional<vector<TableSummary>>& ref_tables) {
  if (!ref_tables || ref_tables->empty()) {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, r.root(), key.c_str());
    return;
  }
  vector<const rapidjson::Value*> tables;
  EXTRACT_ARRAY_CHECK_SIZE(r, r.root(), key.c_str(), tables, ref_tables->size());
  for (int i = 0; i < ref_tables->size(); i++) {
    const auto& ref_table = (*ref_tables)[i];
    const auto* table = tables[i];
    EXPECT_JSON_STRING_FIELD(r, table, "id", ref_table.id);
    EXPECT_JSON_STRING_FIELD(r, table, "name", ref_table.name);
    EXPECT_JSON_STRING_FIELD(r, table,
                             "health", HealthCheckResultToString(ref_table.TableStatus()));
    EXPECT_JSON_INT_FIELD(r, table,
                          "replication_factor", ref_table.replication_factor);
    EXPECT_JSON_INT_FIELD(r, table,
                          "total_tablets", ref_table.TotalTablets());
    EXPECT_JSON_INT_FIELD(r, table,
                          "healthy_tablets", ref_table.healthy_tablets);
    EXPECT_JSON_INT_FIELD(r, table,
                          "recovering_tablets", ref_table.recovering_tablets);
    EXPECT_JSON_INT_FIELD(r, table,
                          "underreplicated_tablets", ref_table.underreplicated_tablets);
    EXPECT_JSON_INT_FIELD(r, table,
                          "unavailable_tablets", ref_table.unavailable_tablets);
    EXPECT_JSON_INT_FIELD(r, table,
                          "consensus_mismatch_tablets", ref_table.consensus_mismatch_tablets);
  }
}

void CheckJsonVsTabletSummaries(const JsonReader& r,
                                const string& key,
                                const boost::optional<vector<TabletSummary>>& ref_tablets) {
  if (!ref_tablets || ref_tablets->empty()) {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, r.root(), key.c_str());
    return;
  }
  vector<const rapidjson::Value*> tablets;
  EXTRACT_ARRAY_CHECK_SIZE(r, r.root(), key.c_str(), tablets, ref_tablets->size());
  for (int i = 0; i < ref_tablets->size(); i++) {
    const auto& ref_tablet = (*ref_tablets)[i];
    const auto& tablet = tablets[i];
    EXPECT_JSON_STRING_FIELD(r, tablet, "id", ref_tablet.id);
    EXPECT_JSON_STRING_FIELD(r, tablet, "table_id", ref_tablet.table_id);
    EXPECT_JSON_STRING_FIELD(r, tablet, "table_name", ref_tablet.table_name);
    EXPECT_JSON_STRING_FIELD(r, tablet,
                             "health", HealthCheckResultToString(ref_tablet.result));
    EXPECT_JSON_STRING_FIELD(r, tablet, "status", ref_tablet.status);
    const rapidjson::Value* master_cstate;
    ASSERT_OK(r.ExtractObject(tablet, "master_cstate", &master_cstate));
    CheckJsonVsConsensusState(r, master_cstate, ref_tablet.master_cstate);
    if (ref_tablet.replicas.empty()) {
      EXPECT_JSON_FIELD_NOT_PRESENT(r, tablet, "replicas");
      continue;
    }
    vector<const rapidjson::Value*> replicas;
    EXTRACT_ARRAY_CHECK_SIZE(r, tablet,
                             "replicas", replicas, ref_tablet.replicas.size());
    for (int j = 0; j < replicas.size(); j++) {
      const auto& ref_replica = ref_tablet.replicas[j];
      const auto* replica = replicas[j];
      CheckJsonVsReplicaSummary(r, replica, ref_replica);
    }
    EXPECT_JSON_STRING_FIELD(r, tablet, "range_key_begin", ref_tablet.range_key_begin);
  }
}

void CheckJsonVsChecksumResults(const JsonReader& r,
                                const string& key,
                                const boost::optional<KsckChecksumResults>& ref_checksum_results) {
  if (!ref_checksum_results || ref_checksum_results->tables.empty()) {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, r.root(), key.c_str());
    return;
  }
  const rapidjson::Value* checksum_results;
  ASSERT_OK(r.ExtractObject(r.root(), key.c_str(), &checksum_results));
  if (ref_checksum_results->snapshot_timestamp) {
    EXPECT_JSON_INT_FIELD(r, checksum_results,
                          "snapshot_timestamp", *ref_checksum_results->snapshot_timestamp);
  } else {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, checksum_results, "snapshot_timestamp");
  }
  vector<const rapidjson::Value*> tables;
  EXTRACT_ARRAY_CHECK_SIZE(r, checksum_results, "tables",
                           tables, ref_checksum_results->tables.size());
  int i = 0;
  for (const auto& table_entry : ref_checksum_results->tables) {
    const auto& ref_table = table_entry.second;
    const auto* table = tables[i++];
    EXPECT_JSON_STRING_FIELD(r, table, "name", table_entry.first);
    vector<const rapidjson::Value*> tablets;
    EXTRACT_ARRAY_CHECK_SIZE(r, table, "tablets", tablets, ref_table.size());
    int j = 0;
    for (const auto& tablet_entry : ref_table) {
      const auto& ref_tablet = tablet_entry.second;
      const auto* tablet = tablets[j++];
      EXPECT_JSON_STRING_FIELD(r, tablet, "tablet_id", tablet_entry.first);
      EXPECT_JSON_BOOL_FIELD(r, tablet, "mismatch", ref_tablet.mismatch);
      vector<const rapidjson::Value*> checksums;
      EXTRACT_ARRAY_CHECK_SIZE(r, tablet, "replica_checksums",
                               checksums, ref_tablet.replica_checksums.size());
      int k = 0;
      for (const auto& replica_entry : ref_tablet.replica_checksums) {
        const auto& ref_replica = replica_entry.second;
        const auto* replica = checksums[k++];
        EXPECT_JSON_STRING_FIELD(r, replica, "ts_uuid", ref_replica.ts_uuid);
        EXPECT_JSON_STRING_FIELD(r, replica, "ts_address", ref_replica.ts_address);
        EXPECT_JSON_STRING_FIELD(r, replica, "status", ref_replica.status.ToString());
        // Checksum is a uint64_t and might plausibly be larger than int64_t's max,
        // so we're handling it special.
        int64_t signed_checksum;
        ASSERT_OK(r.ExtractInt64(replica, "checksum", &signed_checksum));
        ASSERT_EQ(ref_replica.checksum, static_cast<uint64_t>(signed_checksum));
      }
    }
  }
}

void CheckJsonVsVersionSummaries(const JsonReader& r,
                                 const string& key,
                                 const boost::optional<KsckVersionToServersMap>& ref_result) {
  if (!ref_result || ref_result->empty()) {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, r.root(), key.c_str());
    return;
  }

  vector<const rapidjson::Value*> version_servers_map;
  EXTRACT_ARRAY_CHECK_SIZE(r, r.root(), key.c_str(), version_servers_map, ref_result->size());
  auto version_servers = version_servers_map.begin();
  for (const auto& ref_version_servers : *ref_result) {
    ASSERT_NE(version_servers, version_servers_map.end());
    EXPECT_JSON_STRING_FIELD(r, *version_servers, "version", ref_version_servers.first);
    vector<const rapidjson::Value *> servers;
    EXTRACT_ARRAY_CHECK_SIZE(r, *version_servers, "servers", servers,
                             ref_version_servers.second.size());
    auto server = servers.begin();
    for (const auto& ref_server : ref_version_servers.second) {
      ASSERT_NE(server, servers.end());
      EXPECT_EQ((*server)->GetString(), ref_server);
      ++server;
    }
    ++version_servers;
  }
}

void CheckJsonVsCountSummaries(const JsonReader& r,
                               const string& key,
                               const boost::optional<KsckResults>& ref_result) {
  if (!ref_result) {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, r.root(), key.c_str());
    return;
  }
  vector<const rapidjson::Value*> count_results;
  EXTRACT_ARRAY_CHECK_SIZE(r, r.root(), key.c_str(), count_results, 1);

  EXPECT_JSON_INT_FIELD(r, count_results[0], "masters",
                        ref_result->cluster_status.master_summaries.size());
  EXPECT_JSON_INT_FIELD(r, count_results[0], "tservers",
                        ref_result->cluster_status.tserver_summaries.size());
  EXPECT_JSON_INT_FIELD(r, count_results[0], "tables",
                        ref_result->cluster_status.table_summaries.size());
  EXPECT_JSON_INT_FIELD(r, count_results[0], "tablets",
                        ref_result->cluster_status.tablet_summaries.size());
  int replica_count = std::accumulate(ref_result->cluster_status.tablet_summaries.begin(),
                                      ref_result->cluster_status.tablet_summaries.end(),
                                      0,
                                      [](int acc, const TabletSummary& ts) {
                                        return acc + ts.replicas.size();
                                      });
  EXPECT_JSON_INT_FIELD(r, count_results[0], "replicas", replica_count);
}

void CheckJsonVsErrors(const JsonReader& r,
                       const string& key,
                       const vector<Status>& ref_errors) {
  if (ref_errors.empty()) {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, r.root(), key.c_str());
    return;
  }
  vector<const rapidjson::Value*> errors;
  EXTRACT_ARRAY_CHECK_SIZE(r, r.root(), "errors", errors, ref_errors.size());
  for (int i = 0; i < ref_errors.size(); i++) {
    EXPECT_JSON_STRING_FIELD(r, errors[i], nullptr, ref_errors[i].ToString());
  }
}

void CheckPlainStringSection(const string& plain, const string& header, bool present) {
  if (present) {
    ASSERT_STR_CONTAINS(plain, header);
  } else {
    ASSERT_STR_NOT_CONTAINS(plain, header);
  }
}

void CheckPlainStringSections(const string& plain, int sections) {
  NO_FATALS(CheckPlainStringSection(plain,
                                    "Master Summary\n",
                                    sections & PrintSections::Values::MASTER_SUMMARIES));
  NO_FATALS(CheckPlainStringSection(plain,
                                    "Tablet Server Summary\n",
                                    sections & PrintSections::Values::TSERVER_SUMMARIES));
  NO_FATALS(CheckPlainStringSection(plain,
                                    "Version Summary\n",
                                    sections & PrintSections::Values::VERSION_SUMMARIES));
  NO_FATALS(CheckPlainStringSection(plain,
                                    "Tablet Summary\n",
                                    sections & PrintSections::Values::TABLET_SUMMARIES));
  NO_FATALS(CheckPlainStringSection(plain,
                                    "Summary by table\n",
                                    sections & PrintSections::Values::TABLE_SUMMARIES));
  NO_FATALS(CheckPlainStringSection(plain, "Summary by system table\n",
                                    sections & PrintSections::Values::SYSTEM_TABLE_SUMMARIES));
  NO_FATALS(CheckPlainStringSection(plain,
                                    "Checksum Summary\n",
                                    sections & PrintSections::Values::CHECKSUM_RESULTS));
  NO_FATALS(CheckPlainStringSection(plain,
                                    "Total Count Summary\n",
                                    sections & PrintSections::Values::TOTAL_COUNT));
}

void CheckJsonStringVsKsckResults(const string& json,
                                  const KsckResults& results,
                                  int sections = PrintSections::ALL_SECTIONS) {
  JsonReader r(json);
  ASSERT_OK(r.Init());

  NO_FATALS(CheckJsonVsServerHealthSummaries(
      r,
      "master_summaries",
      sections & PrintSections::Values::MASTER_SUMMARIES ?
      boost::optional<vector<ServerHealthSummary>>
        (results.cluster_status.master_summaries) : boost::none));
  NO_FATALS(CheckJsonVsMasterConsensus(
      r,
      results.cluster_status.master_consensus_conflict,
      sections & PrintSections::Values::MASTER_SUMMARIES ?
      boost::optional<ConsensusStateMap>
        (results.cluster_status.master_consensus_state_map) : boost::none));
  NO_FATALS(CheckJsonVsServerHealthSummaries(
      r,
      "tserver_summaries",
      sections & PrintSections::Values::TSERVER_SUMMARIES ?
      boost::optional<vector<ServerHealthSummary>>
        (results.cluster_status.tserver_summaries) : boost::none));
  NO_FATALS(CheckJsonVsVersionSummaries(
      r,
      "version_summaries",
      sections & PrintSections::Values::VERSION_SUMMARIES ?
      boost::optional<KsckVersionToServersMap>
        (results.version_summaries) : boost::none));
  NO_FATALS(CheckJsonVsTabletSummaries(
      r,
      "tablet_summaries",
      sections & PrintSections::Values::TABLET_SUMMARIES ?
      boost::optional<vector<TabletSummary>>
        (results.cluster_status.tablet_summaries) : boost::none));
  NO_FATALS(CheckJsonVsTableSummaries(
      r,
      "table_summaries",
      sections & PrintSections::Values::TABLE_SUMMARIES ?
      boost::optional<vector<TableSummary>>
        (results.cluster_status.table_summaries) : boost::none));
  NO_FATALS(CheckJsonVsTableSummaries(
      r,
      "system_table_summaries",
      sections & PrintSections::Values::SYSTEM_TABLE_SUMMARIES ?
      boost::optional<vector<TableSummary>>
        (results.cluster_status.system_table_summaries) : boost::none));
  NO_FATALS(CheckJsonVsChecksumResults(
      r,
      "checksum_results",
      sections & PrintSections::Values::CHECKSUM_RESULTS ?
      boost::optional<KsckChecksumResults>(results.checksum_results) : boost::none));
  NO_FATALS(CheckJsonVsCountSummaries(
      r,
      "count_summaries",
      sections & PrintSections::Values::TOTAL_COUNT ?
      boost::optional<KsckResults>(results) : boost::none));
  NO_FATALS(CheckJsonVsErrors(r, "errors", results.error_messages));
}

void CheckMessageNotPresent(const vector<Status>& messages, const string& msg) {
  for (const auto& status : messages) {
    ASSERT_STR_NOT_CONTAINS(status.ToString(), msg);
  }
}

TEST_F(KsckTest, TestServersOk) {
  ASSERT_OK(RunKsck());
  const string err_string = err_stream_.str();
  // Master health.
  ASSERT_STR_CONTAINS(err_string,
    "Master Summary\n"
    "    UUID     | Address  | Status\n"
    "-------------+----------+---------\n"
    " master-id-0 | master-0 | HEALTHY\n"
    " master-id-1 | master-1 | HEALTHY\n"
    " master-id-2 | master-2 | HEALTHY\n");
  // Tablet server health.
  ASSERT_STR_CONTAINS(err_string,
    "Tablet Server Summary\n"
    "  UUID   | Address | Status  | Location\n"
    "---------+---------+---------+----------\n"
    " ts-id-0 | <mock>  | HEALTHY | <none>\n"
    " ts-id-1 | <mock>  | HEALTHY | <none>\n"
    " ts-id-2 | <mock>  | HEALTHY | <none>\n");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestMasterUnavailable) {
  shared_ptr<MockKsckMaster> master =
      std::static_pointer_cast<MockKsckMaster>(cluster_->masters_.at(1));
  master->fetch_info_status_ = Status::NetworkError("gremlins");
  master->cstate_ = boost::none;
  ASSERT_TRUE(ksck_->CheckMasterHealth().IsNetworkError());
  ASSERT_TRUE(ksck_->CheckMasterConsensus().IsCorruption());
  ASSERT_OK(ksck_->PrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(),
    "Master Summary\n"
    "    UUID     | Address  |   Status\n"
    "-------------+----------+-------------\n"
    " master-id-0 | master-0 | HEALTHY\n"
    " master-id-2 | master-2 | HEALTHY\n"
    " master-id-1 | master-1 | UNAVAILABLE\n");
  ASSERT_STR_CONTAINS(err_stream_.str(),
    "All reported replicas are:\n"
    "  A = master-id-0\n"
    "  B = master-id-1\n"
    "  C = master-id-2\n"
    "The consensus matrix is:\n"
    " Config source |        Replicas        | Current term | Config index | Committed?\n"
    "---------------+------------------------+--------------+--------------+------------\n"
    " A             | A*  B   C              | 0            |              | Yes\n"
    " B             | [config not available] |              |              | \n"
    " C             | A*  B   C              | 0            |              | Yes\n");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestUnauthorized) {
  Status noauth = Status::RemoteError("Not authorized: unauthorized access to method");
  shared_ptr<MockKsckMaster> master =
      std::static_pointer_cast<MockKsckMaster>(cluster_->masters_.at(1));
  master->fetch_info_status_ = noauth;
  shared_ptr<MockKsckTabletServer> tserver =
      std::static_pointer_cast<MockKsckTabletServer>(
          cluster_->tablet_servers().begin()->second);
  tserver->fetch_info_status_ = noauth;
  Status s = RunKsck();
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "re-run ksck with administrator privileges");
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "failed to gather info from 1 of 3 "
                      "masters due to lack of admin privileges");
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "failed to gather info from 1 of 3 "
                      "tablet servers due to lack of admin privileges");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

// A wrong-master-uuid situation can happen if a master that is part of, e.g.,
// a 3-peer config fails permanently and is wiped and reborn on the same address
// with a new uuid.
TEST_F(KsckTest, TestWrongMasterUuid) {
  shared_ptr<MockKsckMaster> master =
      std::static_pointer_cast<MockKsckMaster>(cluster_->masters_.at(2));
  const string imposter_uuid = "master-id-imposter";
  master->uuid_ = imposter_uuid;
  master->cstate_->set_leader_uuid(imposter_uuid);
  auto* config = master->cstate_->mutable_committed_config();
  config->clear_peers();
  config->add_peers()->set_permanent_uuid(imposter_uuid);

  ASSERT_OK(ksck_->CheckMasterHealth());
  ASSERT_TRUE(ksck_->CheckMasterConsensus().IsCorruption());
  ASSERT_OK(ksck_->PrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(),
    "Master Summary\n"
    "        UUID        | Address  | Status\n"
    "--------------------+----------+---------\n"
    " master-id-0        | master-0 | HEALTHY\n"
    " master-id-1        | master-1 | HEALTHY\n"
    " master-id-imposter | master-2 | HEALTHY\n");
  ASSERT_STR_CONTAINS(err_stream_.str(),
    "All reported replicas are:\n"
    "  A = master-id-0\n"
    "  B = master-id-1\n"
    "  C = master-id-imposter\n"
    "  D = master-id-2\n"
    "The consensus matrix is:\n"
    " Config source |     Replicas     | Current term | Config index | Committed?\n"
    "---------------+------------------+--------------+--------------+------------\n"
    " A             | A*  B       D    | 0            |              | Yes\n"
    " B             | A*  B       D    | 0            |              | Yes\n"
    " C             |         C*       | 0            |              | Yes\n");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestTwoLeaderMasters) {
  shared_ptr<MockKsckMaster> master =
      std::static_pointer_cast<MockKsckMaster>(cluster_->masters_.at(1));
  master->cstate_->set_leader_uuid(master->uuid_);

  ASSERT_OK(ksck_->CheckMasterHealth());
  ASSERT_TRUE(ksck_->CheckMasterConsensus().IsCorruption());
  ASSERT_OK(ksck_->PrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(),
    "All reported replicas are:\n"
    "  A = master-id-0\n"
    "  B = master-id-1\n"
    "  C = master-id-2\n"
    "The consensus matrix is:\n"
    " Config source |   Replicas   | Current term | Config index | Committed?\n"
    "---------------+--------------+--------------+--------------+------------\n"
    " A             | A*  B   C    | 0            |              | Yes\n"
    " B             | A   B*  C    | 0            |              | Yes\n"
    " C             | A*  B   C    | 0            |              | Yes\n");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestLeaderMasterUnavailable) {
  Status error = Status::NetworkError("Network failure");
  cluster_->fetch_info_status_ = error;
  ASSERT_TRUE(ksck_->CheckClusterRunning().IsNetworkError());
  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestMasterFlagCheck) {
  // Check for the differences in the 'unusual' flags category.
  FLAGS_flags_categories_to_check = "unusual";

  // Setup flags for each mock master.
  for (int i = 0; i < cluster_->masters().size(); i++) {
    server::GetFlagsResponsePB flags;
    {
      // Add an experimental flag with the same value for each master.
      auto* flag = flags.add_flags();
      flag->set_name("experimental_flag");
      flag->set_value("x");
      flag->mutable_tags()->Add("experimental");
    }
    {
      // Add a hidden flag with a different value for each master.
      auto* flag = flags.add_flags();
      flag->set_name("hidden_flag");
      flag->set_value(std::to_string(i));
      flag->mutable_tags()->Add("hidden");
    }
    {
      // Add a hidden and unsafe flag with one master having a different value
      // than the other two.
      auto* flag = flags.add_flags();
      flag->set_name("hidden_unsafe_flag");
      flag->set_value(std::to_string(i % 2));
      flag->mutable_tags()->Add("hidden");
      flag->mutable_tags()->Add("unsafe");
    }
    shared_ptr<MockKsckMaster> master =
        std::static_pointer_cast<MockKsckMaster>(cluster_->masters_.at(i));
    master->flags_by_category_[FlagsCategory::UNUSUAL].flags = std::move(flags);
  }
  ASSERT_OK(ksck_->CheckMasterHealth());
  ASSERT_OK(ksck_->CheckMasterUnusualFlags());
  ASSERT_OK(ksck_->CheckMasterDivergedFlags());
  ASSERT_OK(ksck_->PrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "Unusual flags for Master:\n"
      "        Flag        | Value |     Tags      |         Master\n"
      "--------------------+-------+---------------+-------------------------\n"
      " experimental_flag  | x     | experimental  | all 3 server(s) checked\n"
      " hidden_flag        | 0     | hidden        | master-0\n"
      " hidden_flag        | 1     | hidden        | master-1\n"
      " hidden_flag        | 2     | hidden        | master-2\n"
      " hidden_unsafe_flag | 0     | hidden,unsafe | master-0, master-2\n"
      " hidden_unsafe_flag | 1     | hidden,unsafe | master-1");
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "Some masters have unsafe, experimental, or hidden flags set");

  ASSERT_STR_CONTAINS(err_stream_.str(),
      "Flags of checked categories for Master:\n"
      "        Flag        | Value |         Master\n"
      "--------------------+-------+-------------------------\n"
      " experimental_flag  | x     | all 3 server(s) checked\n"
      " hidden_flag        | 0     | master-0\n"
      " hidden_flag        | 1     | master-1\n"
      " hidden_flag        | 2     | master-2\n"
      " hidden_unsafe_flag | 0     | master-0, master-2\n"
      " hidden_unsafe_flag | 1     | master-1");
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "Different masters have different settings for same flags "
      "of checked category 'unusual'");
}

TEST_F(GetFlagsUnavailableKsckTest, TestMasterFlagsUnavailable) {
  ASSERT_OK(ksck_->CheckMasterHealth());
  ASSERT_TRUE(ksck_->CheckMasterUnusualFlags().IsIncomplete());

  static const string flags_msg = "unable to get flag information for master";
  CheckMessageNotPresent(ksck_->results().warning_messages, flags_msg);
}

TEST_F(KsckTest, TestWrongUUIDTabletServer) {
  CreateOneTableOneTablet();

  Status error = Status::RemoteError("ID reported by tablet server "
                                     "doesn't match the expected ID");
  static_pointer_cast<MockKsckTabletServer>(cluster_->tablet_servers_["ts-id-1"])
    ->fetch_info_status_ = error;
  static_pointer_cast<MockKsckTabletServer>(cluster_->tablet_servers_["ts-id-1"])
    ->fetch_info_health_ = ServerHealth::WRONG_SERVER_UUID;

  ASSERT_OK(ksck_->CheckClusterRunning());
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  ASSERT_TRUE(ksck_->FetchInfoFromTabletServers().IsNetworkError());
  ASSERT_OK(ksck_->PrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(),
    "Tablet Server Summary\n"
    "  UUID   | Address |      Status       | Location\n"
    "---------+---------+-------------------+----------\n"
    " ts-id-0 | <mock>  | HEALTHY           | <none>\n"
    " ts-id-2 | <mock>  | HEALTHY           | <none>\n"
    " ts-id-1 | <mock>  | WRONG_SERVER_UUID | <none>\n");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestBadTabletServer) {
  CreateOneSmallReplicatedTable();

  // Mock a failure to connect to one of the tablet servers.
  Status error = Status::NetworkError("Network failure");
  static_pointer_cast<MockKsckTabletServer>(cluster_->tablet_servers_["ts-id-1"])
      ->fetch_info_status_ = error;
  static_pointer_cast<MockKsckTabletServer>(cluster_->tablet_servers_["ts-id-1"])
      ->fetch_info_health_ = ServerHealth::UNAVAILABLE;

  ASSERT_OK(ksck_->CheckClusterRunning());
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  Status s = ksck_->FetchInfoFromTabletServers();
  ASSERT_TRUE(s.IsNetworkError()) << "Status returned: " << s.ToString();

  s = ksck_->CheckTablesConsistency();
  EXPECT_EQ("Corruption: 1 out of 1 table(s) are not healthy", s.ToString());
  ASSERT_OK(ksck_->PrintResults());
  ASSERT_STR_CONTAINS(
      err_stream_.str(),
      "Tablet Server Summary\n"
      "  UUID   | Address |   Status    | Location\n"
      "---------+---------+-------------+----------\n"
      " ts-id-0 | <mock>  | HEALTHY     | <none>\n"
      " ts-id-2 | <mock>  | HEALTHY     | <none>\n"
      " ts-id-1 | <mock>  | UNAVAILABLE | <none>\n");
  ASSERT_STR_CONTAINS(
      err_stream_.str(),
      "Error from <mock>: Network error: Network failure (UNAVAILABLE)\n");
  ASSERT_STR_CONTAINS(
      err_stream_.str(),
      "Tablet tablet-id-0 of table 'test' is under-replicated: 1 replica(s) not RUNNING\n"
      "  ts-id-0 (<mock>): RUNNING [LEADER]\n"
      "  ts-id-1 (<mock>): TS unavailable\n"
      "  ts-id-2 (<mock>): RUNNING\n");
  ASSERT_STR_CONTAINS(
      err_stream_.str(),
      "Tablet tablet-id-1 of table 'test' is under-replicated: 1 replica(s) not RUNNING\n"
      "  ts-id-0 (<mock>): RUNNING [LEADER]\n"
      "  ts-id-1 (<mock>): TS unavailable\n"
      "  ts-id-2 (<mock>): RUNNING\n");
  ASSERT_STR_CONTAINS(
      err_stream_.str(),
      "Tablet tablet-id-2 of table 'test' is under-replicated: 1 replica(s) not RUNNING\n"
      "  ts-id-0 (<mock>): RUNNING [LEADER]\n"
      "  ts-id-1 (<mock>): TS unavailable\n"
      "  ts-id-2 (<mock>): RUNNING\n");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestTserverFlagCheck) {
  // Lower the truncation threshold to test truncation.
  FLAGS_truncate_server_csv_length = 1;

  // Check for the differences in the 'unusual' flags category.
  FLAGS_flags_categories_to_check = "unusual";

  // Setup flags for each mock tablet server.
  int i = 0;
  for (const auto& entry : cluster_->tablet_servers()) {
    server::GetFlagsResponsePB flags;
    {
      // Add an experimental flag with the same value for each tablet server.
      auto* flag = flags.add_flags();
      flag->set_name("experimental_flag");
      flag->set_value("x");
      flag->mutable_tags()->Add("experimental");
    }
    {
      // Add a hidden flag with a different value for each tablet server.
      auto* flag = flags.add_flags();
      flag->set_name("hidden_flag");
      flag->set_value(std::to_string(i));
      flag->mutable_tags()->Add("hidden");
    }
    {
      // Add a hidden and unsafe flag with one tablet server having a different value
      // than the other two.
      auto* flag = flags.add_flags();
      flag->set_name("hidden_unsafe_flag");
      flag->set_value(std::to_string(i % 2));
      flag->mutable_tags()->Add("hidden");
      flag->mutable_tags()->Add("unsafe");
    }
    shared_ptr<MockKsckTabletServer> ts =
        std::static_pointer_cast<MockKsckTabletServer>(entry.second);
    ts->flags_by_category_[FlagsCategory::UNUSUAL].flags = std::move(flags);
    i++;
  }
  ASSERT_OK(ksck_->FetchInfoFromTabletServers());
  ASSERT_OK(ksck_->CheckTabletServerUnusualFlags());
  ASSERT_OK(ksck_->CheckTabletServerDivergedFlags());
  ASSERT_OK(ksck_->PrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "Unusual flags for Tablet Server:\n"
      "        Flag        | Value |     Tags      |         Tablet Server\n"
      "--------------------+-------+---------------+-------------------------------\n"
      " experimental_flag  | x     | experimental  | all 3 server(s) checked\n"
      " hidden_flag        | 0     | hidden        | <mock>\n"
      " hidden_flag        | 1     | hidden        | <mock>\n"
      " hidden_flag        | 2     | hidden        | <mock>\n"
      " hidden_unsafe_flag | 0     | hidden,unsafe | <mock>, and 1 other server(s)\n"
      " hidden_unsafe_flag | 1     | hidden,unsafe | <mock>");
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "Some tablet servers have unsafe, experimental, or hidden flags set");

  ASSERT_STR_CONTAINS(err_stream_.str(),
      "Flags of checked categories for Tablet Server:\n"
      "        Flag        | Value |         Tablet Server\n"
      "--------------------+-------+-------------------------------\n"
      " experimental_flag  | x     | all 3 server(s) checked\n"
      " hidden_flag        | 0     | <mock>\n"
      " hidden_flag        | 1     | <mock>\n"
      " hidden_flag        | 2     | <mock>\n"
      " hidden_unsafe_flag | 0     | <mock>, and 1 other server(s)\n"
      " hidden_unsafe_flag | 1     | <mock>");
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "Different tablet servers have different settings for same flags "
      "of checked category 'unusual'");
}

TEST_F(KsckTest, FlagsCategoriesDifferenceBetweenMastersAndTabletServers) {
  // Check for the differences in the 'time_source' flags category.
  FLAGS_flags_categories_to_check = "time_source";

  // Setup flags for mock masters.
  for (const auto& master : cluster_->masters()) {
    shared_ptr<MockKsckMaster> m =
        std::static_pointer_cast<MockKsckMaster>(master);
    // Set two flags in the 'time_source' category.
    {
      server::GetFlagsResponsePB flags;
      {
        auto* flag = flags.add_flags();
        flag->set_name("time_source");
        flag->set_value("builtin");
      }
      {
        auto* flag = flags.add_flags();
        flag->set_name("builtin_ntp_servers");
        flag->set_value("mega.turbo.ntp");
      }
      m->flags_by_category_[FlagsCategory::TIME_SOURCE].flags = std::move(flags);
    }

    // Set a flag unrelated to the checked category.
    {
      server::GetFlagsResponsePB flags;
      {
        auto* flag = flags.add_flags();
        flag->set_name("giga");
        flag->set_value("hertz");
      }
      m->flags_by_category_[FlagsCategory::UNUSUAL].flags = std::move(flags);
    }
  }

  // Setup flags for mock tablet servers.
  for (const auto& entry : cluster_->tablet_servers()) {
    shared_ptr<MockKsckTabletServer> ts =
        std::static_pointer_cast<MockKsckTabletServer>(entry.second);
    // Set one flag in the 'time_source' category.
    {
      server::GetFlagsResponsePB flags;
      {
        auto* flag = flags.add_flags();
        flag->set_name("time_source");
        flag->set_value("system");
      }
      ts->flags_by_category_[FlagsCategory::TIME_SOURCE].flags = std::move(flags);
    }

    // Set a flag unrelated to the 'time_source' category.
    {
      server::GetFlagsResponsePB flags;
      {
        auto* flag = flags.add_flags();
        flag->set_name("foo");
        flag->set_value("bar");
      }
      ts->flags_by_category_[FlagsCategory::UNUSUAL].flags = std::move(flags);
    }
  }

  // Calling CheckMasterHealth() is a prerequisite for calling
  // CheckMasterUnusualFlags().
  ASSERT_OK(ksck_->CheckMasterHealth());
  ASSERT_OK(ksck_->CheckMasterDivergedFlags());
  // Calling FetchInfoFromTabletServers() is a prerequisite for calling
  // CheckTabletServerDivergedFlags().
  ASSERT_OK(ksck_->FetchInfoFromTabletServers());
  ASSERT_OK(ksck_->CheckTabletServerDivergedFlags());
  ASSERT_OK(ksck_->CheckDivergedFlags());
  ASSERT_OK(ksck_->PrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "Flags of checked categories for Master:\n"
      "        Flag         |     Value      |         Master\n"
      "---------------------+----------------+-------------------------\n"
      " builtin_ntp_servers | mega.turbo.ntp | all 3 server(s) checked\n"
      " time_source         | builtin        | all 3 server(s) checked\n");
  ASSERT_STR_NOT_CONTAINS(err_stream_.str(),
      "Different masters have different settings for same flags "
      "of checked category 'time_source'");
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "Flags of checked categories for Master diverging from Tablet Server flags:\n"
      "        Flag         |     Value      |         Master\n"
      "---------------------+----------------+-------------------------\n"
      " builtin_ntp_servers | mega.turbo.ntp | all 3 server(s) checked\n"
      " time_source         | builtin        | all 3 server(s) checked");

  ASSERT_STR_CONTAINS(err_stream_.str(),
      "Flags of checked categories for Tablet Server:\n"
      "    Flag     | Value  |      Tablet Server\n"
      "-------------+--------+-------------------------\n"
      " time_source | system | all 3 server(s) checked\n");
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "Flags of checked categories for Tablet Server diverging from Master flags:\n"
      "    Flag     | Value  |      Tablet Server\n"
      "-------------+--------+-------------------------\n"
      " time_source | system | all 3 server(s) checked");
  ASSERT_STR_NOT_CONTAINS(err_stream_.str(),
      "Different tablet servers have different settings for same flags "
      "of checked category 'time_source'");

  ASSERT_STR_CONTAINS(err_stream_.str(),
        "Same flags have different values between masters and tablet servers "
        "for at least one checked flag category");
}

TEST_F(GetFlagsUnavailableKsckTest, TestTserverFlagsUnavailable) {
  ASSERT_OK(ksck_->FetchInfoFromTabletServers());
  ASSERT_TRUE(ksck_->CheckTabletServerUnusualFlags().IsIncomplete());

  static const string flags_msg = "unable to get flag information for tablet server";
  CheckMessageNotPresent(ksck_->results().warning_messages, flags_msg);
}

TEST_F(KsckTest, TestOneTableCheck) {
  CreateOneTableOneTablet();
  FLAGS_checksum_scan = true;
  ASSERT_OK(RunKsck());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "0/1 replicas remaining (20B from disk, 10 rows summed)");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestOneSmallReplicatedTable) {
  CreateOneSmallReplicatedTable();
  FLAGS_checksum_scan = true;
  ASSERT_OK(RunKsck());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "0/9 replicas remaining (180B from disk, 90 rows summed)");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestOneSmallReplicatedTableWithConsensusState) {
  CreateOneSmallReplicatedTable();
  ASSERT_OK(RunKsck());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      ExpectedTableSummary("test",
                                           /*replication_factor=*/ 3,
                                           /*healthy_tablets=*/ 3,
                                           /*recovering_tablets=*/ 0,
                                           /*underreplicated_tablets=*/ 0,
                                           /*consensus_mismatch_tablets=*/ 0,
                                           /*unavailable_tablets=*/ 0));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestConsensusConflictExtraPeer) {
  CreateOneSmallReplicatedTable();

  shared_ptr<KsckTabletServer> ts = FindOrDie(cluster_->tablet_servers_, "ts-id-0");
  auto& cstate = FindOrDieNoPrint(ts->tablet_consensus_state_map_,
                                  std::make_pair("ts-id-0", "tablet-id-0"));
  cstate.mutable_committed_config()->add_peers()->set_permanent_uuid("ts-id-fake");

  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  ASSERT_EQ("Corruption: table consistency check error: 1 out of 1 table(s) are not healthy",
            error_messages[0].ToString());
  const string err_str = err_stream_.str();
  ASSERT_STR_CONTAINS(err_str, "Tablet tablet-id-0 of table 'test' is conflicted: "
                               "1 replicas' active configs disagree with the leader master's");
  ASSERT_STR_CONTAINS(err_str,
      "The consensus matrix is:\n"
      " Config source |     Replicas     | Current term | Config index | Committed?\n"
      "---------------+------------------+--------------+--------------+------------\n"
      " master        | A*  B   C        |              |              | Yes\n"
      " A             | A*  B   C   D    | 0            |              | Yes\n"
      " B             | A*  B   C        | 0            |              | Yes\n"
      " C             | A*  B   C        | 0            |              | Yes");
  ASSERT_STR_CONTAINS(err_str,
                      ExpectedTableSummary("test",
                                           /*replication_factor=*/ 3,
                                           /*healthy_tablets=*/ 2,
                                           /*recovering_tablets=*/ 0,
                                           /*underreplicated_tablets=*/ 0,
                                           /*consensus_mismatch_tablets=*/ 1,
                                           /*unavailable_tablets=*/ 0));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestConsensusConflictMissingPeer) {
  CreateOneSmallReplicatedTable();

  shared_ptr<KsckTabletServer> ts = FindOrDie(cluster_->tablet_servers_, "ts-id-0");
  auto& cstate = FindOrDieNoPrint(ts->tablet_consensus_state_map_,
                                  std::make_pair("ts-id-0", "tablet-id-0"));
  cstate.mutable_committed_config()->mutable_peers()->RemoveLast();

  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  ASSERT_EQ("Corruption: table consistency check error: 1 out of 1 table(s) are not healthy",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "The consensus matrix is:\n"
      " Config source |   Replicas   | Current term | Config index | Committed?\n"
      "---------------+--------------+--------------+--------------+------------\n"
      " master        | A*  B   C    |              |              | Yes\n"
      " A             | A*  B        | 0            |              | Yes\n"
      " B             | A*  B   C    | 0            |              | Yes\n"
      " C             | A*  B   C    | 0            |              | Yes");
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      ExpectedTableSummary("test",
                                           /*replication_factor=*/ 3,
                                           /*healthy_tablets=*/ 2,
                                           /*recovering_tablets=*/ 0,
                                           /*underreplicated_tablets=*/ 0,
                                           /*consensus_mismatch_tablets=*/ 1,
                                           /*unavailable_tablets=*/ 0));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestConsensusConflictDifferentLeader) {
  CreateOneSmallReplicatedTable();

  const shared_ptr<KsckTabletServer>& ts = FindOrDie(cluster_->tablet_servers_, "ts-id-0");
  auto& cstate = FindOrDieNoPrint(ts->tablet_consensus_state_map_,
                                  std::make_pair("ts-id-0", "tablet-id-0"));
  cstate.set_leader_uuid("ts-id-1");

  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  ASSERT_EQ("Corruption: table consistency check error: 1 out of 1 table(s) are not healthy",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "The consensus matrix is:\n"
      " Config source |   Replicas   | Current term | Config index | Committed?\n"
      "---------------+--------------+--------------+--------------+------------\n"
      " master        | A*  B   C    |              |              | Yes\n"
      " A             | A   B*  C    | 0            |              | Yes\n"
      " B             | A*  B   C    | 0            |              | Yes\n"
      " C             | A*  B   C    | 0            |              | Yes");
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      ExpectedTableSummary("test",
                                           /*replication_factor=*/ 3,
                                           /*healthy_tablets=*/ 2,
                                           /*recovering_tablets=*/ 0,
                                           /*underreplicated_tablets=*/ 0,
                                           /*consensus_mismatch_tablets=*/ 1,
                                           /*unavailable_tablets=*/ 0));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestOneOneTabletBrokenTable) {
  CreateOneOneTabletReplicatedBrokenTable();
  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  ASSERT_EQ("Corruption: table consistency check error: 1 out of 1 table(s) are not healthy",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "Tablet tablet-id-1 of table 'test' is under-replicated: "
                      "configuration has 2 replicas vs desired 3");
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      ExpectedTableSummary("test",
                                           /*replication_factor=*/ 3,
                                           /*healthy_tablets=*/ 0,
                                           /*recovering_tablets=*/ 0,
                                           /*underreplicated_tablets=*/ 1,
                                           /*consensus_mismatch_tablets=*/ 0,
                                           /*unavailable_tablets=*/ 0));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestMismatchedAssignments) {
  CreateOneSmallReplicatedTable();
  shared_ptr<MockKsckTabletServer> ts = static_pointer_cast<MockKsckTabletServer>(
      cluster_->tablet_servers_.at(Substitute("ts-id-$0", 0)));
  ASSERT_EQ(1, ts->tablet_status_map_.erase("tablet-id-2"));

  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  ASSERT_EQ("Corruption: table consistency check error: 1 out of 1 table(s) are not healthy",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "Tablet tablet-id-2 of table 'test' is under-replicated: "
                      "1 replica(s) not RUNNING\n"
                      "  ts-id-0 (<mock>): missing [LEADER]\n"
                      "  ts-id-1 (<mock>): RUNNING\n"
                      "  ts-id-2 (<mock>): RUNNING\n");
  ASSERT_STR_CONTAINS(err_stream_.str(),
                     ExpectedTableSummary("test",
                                          /*replication_factor=*/ 3,
                                          /*healthy_tablets=*/ 2,
                                          /*recovering_tablets=*/ 0,
                                          /*underreplicated_tablets=*/ 1,
                                          /*consensus_mismatch_tablets=*/ 0,
                                          /*unavailable_tablets=*/ 0));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestTabletNotRunning) {
  CreateOneSmallReplicatedTableWithTabletNotRunning();

  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  ASSERT_EQ("Corruption: table consistency check error: 1 out of 1 table(s) are not healthy",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(
      err_stream_.str(),
      "Tablet tablet-id-0 of table 'test' is unavailable: 3 replica(s) not RUNNING\n"
      "  ts-id-0 (<mock>): not running [LEADER]\n"
      "    State:       FAILED\n"
      "    Data state:  TABLET_DATA_UNKNOWN\n"
      "    Last status: \n"
      "  ts-id-1 (<mock>): not running\n"
      "    State:       FAILED\n"
      "    Data state:  TABLET_DATA_UNKNOWN\n"
      "    Last status: \n"
      "  ts-id-2 (<mock>): not running\n"
      "    State:       FAILED\n"
      "    Data state:  TABLET_DATA_UNKNOWN\n"
      "    Last status: \n");
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      ExpectedTableSummary("test",
                                           /*replication_factor=*/ 3,
                                           /*healthy_tablets=*/ 2,
                                           /*recovering_tablets=*/ 0,
                                           /*underreplicated_tablets=*/ 0,
                                           /*consensus_mismatch_tablets=*/ 0,
                                           /*unavailable_tablets=*/ 1));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestTabletCopying) {
  CreateOneSmallReplicatedTableWithTabletNotRunning();
  CreateDefaultAssignmentPlan(1);

  // Mark one of the tablet replicas as copying.
  auto not_running_ts = static_pointer_cast<MockKsckTabletServer>(
          cluster_->tablet_servers_.at(assignment_plan_.back()));
  auto& pb = FindOrDie(not_running_ts->tablet_status_map_, "tablet-id-0");
  pb.set_tablet_data_state(TabletDataState::TABLET_DATA_COPYING);
  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  ASSERT_EQ("Corruption: table consistency check error: 1 out of 1 table(s) are not healthy",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      ExpectedTableSummary("test",
                                           /*replication_factor=*/ 3,
                                           /*healthy_tablets=*/ 2,
                                           /*recovering_tablets=*/ 1,
                                           /*underreplicated_tablets=*/ 0,
                                           /*consensus_mismatch_tablets=*/ 0,
                                           /*unavailable_tablets=*/ 0));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

// Test for a bug where we weren't properly handling a tserver not reported by the master.
TEST_F(KsckTest, TestMasterNotReportingTabletServer) {
  CreateOneSmallReplicatedTable();

  // Delete a tablet server from the master's list. This simulates a situation
  // where the master is starting and doesn't list all tablet servers yet, but
  // tablets from other tablet servers are listing a missing tablet server as a peer.
  EraseKeyReturnValuePtr(&cluster_->tablet_servers_, "ts-id-0");
  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  ASSERT_EQ("Corruption: table consistency check error: 1 out of 1 table(s) are not healthy",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      ExpectedTableSummary("test",
                                           /*replication_factor=*/ 3,
                                           /*healthy_tablets=*/ 0,
                                           /*recovering_tablets=*/ 0,
                                           /*underreplicated_tablets=*/ 3,
                                           /*consensus_mismatch_tablets=*/ 0,
                                           /*unavailable_tablets=*/ 0));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

// KUDU-2113: Test for a bug where we weren't properly handling a tserver not
// reported by the master when there was also a consensus conflict.
TEST_F(KsckTest, TestMasterNotReportingTabletServerWithConsensusConflict) {
  CreateOneSmallReplicatedTable();

  // Delete a tablet server from the cluster's list as in TestMasterNotReportingTabletServer.
  EraseKeyReturnValuePtr(&cluster_->tablet_servers_, "ts-id-0");

  // Now engineer a consensus conflict.
  const shared_ptr<KsckTabletServer>& ts = FindOrDie(cluster_->tablet_servers_, "ts-id-1");
  auto& cstate = FindOrDieNoPrint(ts->tablet_consensus_state_map_,
                                  std::make_pair("ts-id-1", "tablet-id-1"));
  cstate.set_leader_uuid("ts-id-1");

  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  ASSERT_EQ("Corruption: table consistency check error: 1 out of 1 table(s) are not healthy",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "The consensus matrix is:\n"
      " Config source |        Replicas        | Current term | Config index | Committed?\n"
      "---------------+------------------------+--------------+--------------+------------\n"
      " master        | A*  B   C              |              |              | Yes\n"
      " A             | [config not available] |              |              | \n"
      " B             | A   B*  C              | 0            |              | Yes\n"
      " C             | A*  B   C              | 0            |              | Yes");
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      ExpectedTableSummary("test",
                                           /*replication_factor=*/ 3,
                                           /*healthy_tablets=*/ 0,
                                           /*recovering_tablets=*/ 0,
                                           /*underreplicated_tablets=*/ 3,
                                           /*consensus_mismatch_tablets=*/ 0,
                                           /*unavailable_tablets=*/ 0));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestVersionCheck) {
  for (int i : {1, 2}) {
    shared_ptr<MockKsckMaster> master =
        static_pointer_cast<MockKsckMaster>(cluster_->masters_[i]);
    master->version_ = Substitute("v$0", i);
  }

  ASSERT_OK(RunKsck());
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "Version Summary\n"
      "   Version    |                                Servers\n"
      "--------------+------------------------------------------------------------------------\n"
      " mock-version | master@master-0, tserver@<mock>, tserver@<mock>, and 1 other server(s)\n"
      " v1           | master@master-1\n"
      " v2           | master@master-2");
  ASSERT_STR_CONTAINS(err_stream_.str(), "version check error: not all servers "
                                         "are running the same version: "
                                         "3 different versions were seen");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestChecksumScanJson) {
  CreateOneTableOneTablet();
  FLAGS_checksum_scan = true;
  FLAGS_ksck_format = "json_compact";
  ASSERT_OK(RunKsck());
  JsonReader r(err_stream_.str());
  ASSERT_OK(r.Init());
}

TEST_F(KsckTest, TestChecksumScanMismatch) {
  CreateOneSmallReplicatedTable();
  FLAGS_checksum_scan = true;

  // Set one tablet server to return a non-zero checksum for its replicas.
  // This will not match the checksums of replicas from other servers because
  // they are zero by default.
  auto ts = static_pointer_cast<MockKsckTabletServer>(
      cluster_->tablet_servers_.begin()->second);
  ts->checksum_ = 1;

  ASSERT_TRUE(RunKsck().IsRuntimeError());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "Corruption: checksum scan error: 3 tablet(s) had "
                      "checksum mismatches");
}

TEST_F(KsckTest, TestChecksumScanIdleTimeout) {
  CreateOneTableOneTablet();
  FLAGS_checksum_scan = true;

  // Set an impossibly low idle timeout and tweak one of the servers to always
  // report no progress on the checksum.
  FLAGS_checksum_idle_timeout_sec = 0;
  auto ts = static_pointer_cast<MockKsckTabletServer>(
      cluster_->tablet_servers_.begin()->second);
  ts->checksum_progress_ = 0;

  // Make the progress report happen frequently so this test is fast.
  FLAGS_max_progress_report_wait_ms = 10;
  ASSERT_TRUE(RunKsck().IsRuntimeError());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "Timed out: checksum scan error: Checksum scan did not "
                      "make progress within the idle timeout of 0.000s");
}

TEST_F(KsckTest, TestChecksumWithAllUnhealthyTabletServers) {
  CreateOneTableOneTablet();
  FLAGS_checksum_scan = true;

  // Make all tablet servers unhealthy.
  for (const auto& entry : cluster_->tablet_servers_) {
    auto ts = static_pointer_cast<MockKsckTabletServer>(entry.second);
    ts->fetch_info_status_ = Status::NetworkError("gremlins");
    ts->fetch_info_health_ = ServerHealth::UNAVAILABLE;
  }

  // The checksum should short-circuit and fail because no tablet servers are
  // available.
  ASSERT_TRUE(RunKsck().IsRuntimeError());
  ASSERT_STR_CONTAINS(err_stream_.str(), "no tablet servers are available");
}

TEST_F(KsckTest, TestChecksumWithAllPeersUnhealthy) {
  CreateOneTableOneTablet();
  FLAGS_checksum_scan = true;

  // Make all tablet servers unhealthy except an extra one with no replica of
  // the tablet.
  for (const auto& entry : cluster_->tablet_servers_) {
    auto ts = static_pointer_cast<MockKsckTabletServer>(entry.second);
    ts->fetch_info_status_ = Status::NetworkError("gremlins");
    ts->fetch_info_health_ = ServerHealth::UNAVAILABLE;
  }
  const char* const new_uuid = "new";
  EmplaceOrDie(&cluster_->tablet_servers_,
               new_uuid,
               make_shared<MockKsckTabletServer>(new_uuid, IsGetFlagsAvailable()));

  // The checksum should fail for tablet because none of its replicas are
  // available to provide a timestamp.
  ASSERT_TRUE(RunKsck().IsRuntimeError());
  ASSERT_STR_CONTAINS(
      err_stream_.str(),
      "T tablet-id-1 P ts-id-0 (<mock>): Error: Aborted: "
      "no healthy peer was available to provide a timestamp");
}

TEST_F(KsckTest, TestTabletServerLocation) {
  CreateOneTableOneTablet();
  shared_ptr<MockKsckTabletServer> ts =
        static_pointer_cast<MockKsckTabletServer>(cluster_->tablet_servers_["ts-id-1"]);
  ts->location_ = "/foo";

  ASSERT_OK(ksck_->CheckClusterRunning());
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  ASSERT_OK(ksck_->FetchInfoFromTabletServers());
  ASSERT_OK(ksck_->PrintResults());

  ASSERT_STR_CONTAINS(err_stream_.str(),
    "Tablet Server Summary\n"
    "  UUID   | Address | Status  | Location\n"
    "---------+---------+---------+----------\n"
    " ts-id-0 | <mock>  | HEALTHY | <none>\n"
    " ts-id-1 | <mock>  | HEALTHY | /foo\n"
    " ts-id-2 | <mock>  | HEALTHY | <none>\n");

  NO_FATALS(CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results()));
}

TEST_F(KsckTest, TestSectionFilter) {
  std::map<int, std::string> sections = {
          {PrintSections::Values::MASTER_SUMMARIES, "MASTER_SUMMARIES"},
          {PrintSections::Values::TSERVER_SUMMARIES, "TSERVER_SUMMARIES"},
          {PrintSections::Values::VERSION_SUMMARIES, "VERSION_SUMMARIES"},
          {PrintSections::Values::TABLET_SUMMARIES, "TABLET_SUMMARIES"},
          {PrintSections::Values::TABLE_SUMMARIES, "TABLE_SUMMARIES"},
          {PrintSections::Values::SYSTEM_TABLE_SUMMARIES, "SYSTEM_TABLE_SUMMARIES"},
          {PrintSections::Values::CHECKSUM_RESULTS, "CHECKSUM_RESULTS"},
          {PrintSections::Values::TOTAL_COUNT, "TOTAL_COUNT"}};
  NO_FATALS(CreateOneTableOneTablet(/*create_txn_status_table=*/true));
  for (const auto& [s_enum, s_str] : sections) {
    if (s_enum == PrintSections::Values::CHECKSUM_RESULTS) {
      FLAGS_checksum_scan = true;
    }
    ksck_->set_print_sections({s_str});
    err_stream_.str("");
    err_stream_.clear();
    ASSERT_OK(RunKsck());

    // Check plain string output.
    NO_FATALS(CheckPlainStringSections(err_stream_.str(), s_enum));

    // Check json string output.
    const string& json_output = KsckResultsToJsonString(s_enum);
    NO_FATALS(CheckJsonStringVsKsckResults(json_output, ksck_->results(), s_enum));
  }
}

} // namespace tools
} // namespace kudu
