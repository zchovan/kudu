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

#include "kudu/mini-cluster/external_mini_cluster.h"

#include <algorithm>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iterator>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_set>
#include <utility>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/master_rpc.h"
#include "kudu/fs/default_key_provider.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/key_provider.h"
#include "kudu/postgres/mini_postgres.h"
#include "kudu/ranger-kms/mini_ranger_kms.h"
#include "kudu/rpc/rpc_header.pb.h"
#if !defined(NO_CHRONY)
#include "kudu/clock/test/mini_chronyd.h"
#endif
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/ranger/mini_ranger.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/rpc/user_credentials.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/server/server_base.proxy.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/async_util.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/jwt-util.h"
#include "kudu/util/mini_oidc.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"

namespace kudu {
class JwtVerifier;
}  // namespace kudu

using kudu::client::internal::ConnectToClusterRpc;
#if !defined(NO_CHRONY)
using kudu::clock::MiniChronyd;
#endif
using kudu::master::ListTablesRequestPB;
using kudu::master::ListTablesResponsePB;
using kudu::master::MasterServiceProxy;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::RpcController;
using kudu::security::DefaultKeyProvider;
using kudu::server::ServerStatusPB;
using kudu::tserver::ListTabletsRequestPB;
using kudu::tserver::ListTabletsResponsePB;
using kudu::tserver::TabletServerAdminServiceProxy;
using kudu::tserver::TabletServerServiceProxy;
using std::back_inserter;
using std::copy;
using std::map;
using std::pair;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::a2b_hex;
using strings::Substitute;

typedef ListTabletsResponsePB::StatusAndSchemaPB StatusAndSchemaPB;

DECLARE_bool(encrypt_data_at_rest);
DECLARE_string(block_manager);
DECLARE_string(dns_addr_resolution_override);

DEFINE_bool(perf_record, false,
            "Whether to run \"perf record --call-graph fp\" on each daemon in the cluster");

namespace kudu {
namespace cluster {

static const char* const kKuduBinaryName = "kudu";
static double kTabletServerRegistrationTimeoutSeconds = 15.0;
static double kMasterCatalogManagerTimeoutSeconds = 60.0;

ExternalMiniClusterOptions::ExternalMiniClusterOptions()
    : num_masters(1),
      supply_single_master_addr(true),
      num_tablet_servers(1),
      bind_mode(kDefaultBindMode),
      num_data_dirs(1),
      enable_kerberos(false),
      principal("kudu"),
      hms_mode(HmsMode::NONE),
      enable_ranger(false),
      enable_ranger_kms(false),
      ranger_cluster_key("kuduclusterkey"),
      enable_encryption(FLAGS_encrypt_data_at_rest),
      logtostderr(true),
      start_process_timeout(MonoDelta::FromSeconds(70)),
      rpc_negotiation_timeout(MonoDelta::FromSeconds(3)),
#if !defined(NO_CHRONY)
      num_ntp_servers(1),
      ntp_config_mode(BuiltinNtpConfigMode::ALL_SERVERS),
#endif // #if !defined(NO_CHRONY) ...
      enable_client_jwt(false) {}

ExternalMiniCluster::ExternalMiniCluster()
  : opts_(ExternalMiniClusterOptions()) {
}

ExternalMiniCluster::ExternalMiniCluster(ExternalMiniClusterOptions opts)
  : opts_(std::move(opts)) {
}

ExternalMiniCluster::~ExternalMiniCluster() {
  Shutdown();
}

Env* ExternalMiniCluster::env() const {
  return Env::Default();
}

Env* ExternalMiniCluster::ts_env(int ts_idx) const {
  return tablet_server(ts_idx)->env();
}

Env* ExternalMiniCluster::master_env(int master_idx) const {
  return master(master_idx)->env();
}

Status ExternalMiniCluster::DeduceBinRoot(std::string* ret) {
  string exe;
  RETURN_NOT_OK(env()->GetExecutablePath(&exe));
  *ret = DirName(exe);
  return Status::OK();
}

Status ExternalMiniCluster::HandleOptions() {
  if (opts_.daemon_bin_path.empty()) {
    RETURN_NOT_OK(DeduceBinRoot(&opts_.daemon_bin_path));
  }

  if (opts_.cluster_root.empty()) {
    // If they don't specify a cluster root, use the current gtest directory.
    opts_.cluster_root = JoinPathSegments(GetTestDataDirectory(), "minicluster-data");
  }

  if (opts_.block_manager_type.empty()) {
    opts_.block_manager_type = FLAGS_block_manager;
  }

  vector<string> host_mappings;
  if (!opts_.tserver_alias_prefix.empty()) {
    for (int i = 0; i < opts_.num_tablet_servers; i++) {
      host_mappings.emplace_back(Substitute("$0.$1=$2", opts_.tserver_alias_prefix, i,
                                            GetBindIpForTabletServer(i)));
    }
  }
  if (!opts_.master_alias_prefix.empty()) {
    for (int i = 0; i < opts_.num_masters; i++) {
      host_mappings.emplace_back(Substitute("$0.$1=$2", opts_.master_alias_prefix, i,
                                            GetBindIpForMaster(i)));
    }
  }
  if (!host_mappings.empty()) {
    dns_overrides_ = JoinStrings(host_mappings, ",");
    opts_.extra_master_flags.emplace_back(
        Substitute("--dns_addr_resolution_override=$0", dns_overrides_));
    opts_.extra_tserver_flags.emplace_back(
        Substitute("--dns_addr_resolution_override=$0", dns_overrides_));
  }

  return Status::OK();
}

Status ExternalMiniCluster::AddTimeSourceFlags(
    int idx, std::vector<std::string>* flags) {
  DCHECK_LE(0, idx);
  DCHECK(flags);
#if defined(NO_CHRONY)
  flags->emplace_back("--time_source=system_unsync");
#else
  CHECK_LE(0, opts_.num_ntp_servers);
  if (opts_.num_ntp_servers == 0) {
    flags->emplace_back("--time_source=system_unsync");
  } else {
    vector<string> ntp_endpoints;
    CHECK_EQ(opts_.num_ntp_servers, ntp_servers_.size());
    // Point the built-in NTP client to the test NTP servers.
    switch (opts_.ntp_config_mode) {
      case BuiltinNtpConfigMode::ALL_SERVERS:
        for (const auto& server : ntp_servers_) {
          ntp_endpoints.emplace_back(server->address().ToString());
        }
        break;
      case BuiltinNtpConfigMode::ROUND_ROBIN_SINGLE_SERVER:
        ntp_endpoints.emplace_back(
            ntp_servers_[idx % opts_.num_ntp_servers]->address().ToString());
        break;
    }
    flags->emplace_back(Substitute("--builtin_ntp_servers=$0",
                                   JoinStrings(ntp_endpoints, ",")));
    // The chronyd server supports very short polling interval: let's use this
    // feature for faster clock synchronisation at startup and to keep the
    // estimated clock error of the built-in NTP client smaller.
    flags->emplace_back(Substitute("--builtin_ntp_poll_interval_ms=100"));
    // Wait up to 10 seconds to let the built-in NTP client to synchronize its
    // time with the test NTP server.
    flags->emplace_back(Substitute("--ntp_initial_sync_wait_secs=10"));
    // Switch the clock to use the built-in NTP client which clock is
    // synchronized with the test NTP server.
    flags->emplace_back("--time_source=builtin");
  }
#endif // #if defined(NO_CHRONY) ... else ...
  return Status::OK();
}

Status ExternalMiniCluster::Start() {
  CHECK(masters_.empty()) << "Masters are not empty (size: " << masters_.size()
      << "). Maybe you meant Restart()?";
  CHECK(tablet_servers_.empty()) << "Tablet servers are not empty (size: "
      << tablet_servers_.size() << "). Maybe you meant Restart()?";
  RETURN_NOT_OK(HandleOptions());
  gflags::FlagSaver saver;
  FLAGS_dns_addr_resolution_override = dns_overrides_;

  std::shared_ptr<JwtVerifier> jwt_verifier = nullptr;
  if (opts_.enable_client_jwt) {
    oidc_.reset(new MiniOidc(opts_.mini_oidc_options));
    RETURN_NOT_OK_PREPEND(oidc_->Start(), "Failed to start OIDC endpoints");
    jwt_verifier = std::make_shared<PerAccountKeyBasedJwtVerifier>(oidc_->url());
  }

  RETURN_NOT_OK_PREPEND(
      rpc::MessengerBuilder("minicluster-messenger")
          .set_num_reactors(1)
          .set_max_negotiation_threads(1)
          .set_rpc_negotiation_timeout_ms(opts_.rpc_negotiation_timeout.ToMilliseconds())
          .set_sasl_proto_name(opts_.principal)
          .set_jwt_verifier(std::move(jwt_verifier))
          .Build(&messenger_),
      "Failed to start Messenger for minicluster");

  Status s = env()->CreateDir(opts_.cluster_root);
  if (!s.ok() && !s.IsAlreadyPresent()) {
    RETURN_NOT_OK_PREPEND(s, "Could not create root dir " + opts_.cluster_root);
  }

  if (opts_.enable_kerberos) {
    kdc_.reset(new MiniKdc(opts_.mini_kdc_options));
    RETURN_NOT_OK(kdc_->Start());
    RETURN_NOT_OK_PREPEND(kdc_->CreateUserPrincipal("test-admin"),
                          "could not create admin principal");
    RETURN_NOT_OK_PREPEND(kdc_->CreateUserPrincipal("test-user"),
                          "could not create user principal");
    RETURN_NOT_OK_PREPEND(kdc_->CreateUserPrincipal("joe-interloper"),
                          "could not create unauthorized principal");

    RETURN_NOT_OK_PREPEND(kdc_->CreateKeytabForExistingPrincipal("test-user"),
                         "could not create client keytab");

    RETURN_NOT_OK_PREPEND(kdc_->Kinit("test-admin"),
                          "could not kinit as admin");

    RETURN_NOT_OK_PREPEND(kdc_->SetKrb5Environment(),
                          "could not set krb5 client env");
  }

#if !defined(NO_CHRONY)
  // Start NTP servers, if requested.
  if (opts_.num_ntp_servers > 0) {
    // Collect and keep alive the set of sockets bound with SO_REUSEPORT option
    // until all chronyd proccesses are started. This allows to avoid port
    // conflicts: chronyd doesn't support binding to wildcard addresses and
    // it's necessary to make sure chronyd is able to bind to the port specified
    // in its configuration. So, the mini-cluster reserves a set of ports up
    // front, then starts the set of chronyd processes, each bound to one
    // of the reserved ports.
    vector<unique_ptr<Socket>> reserved_sockets;
    for (auto i = 0; i < opts_.num_ntp_servers; ++i) {
      unique_ptr<Socket> reserved_socket;
      RETURN_NOT_OK_PREPEND(ReserveDaemonSocket(
          DaemonType::EXTERNAL_SERVER, i, opts_.bind_mode, &reserved_socket),
          "failed to reserve chronyd socket address");
      Sockaddr addr;
      RETURN_NOT_OK(reserved_socket->GetSocketAddress(&addr));
      reserved_sockets.emplace_back(std::move(reserved_socket));

      RETURN_NOT_OK_PREPEND(AddNtpServer(addr),
                            Substitute("failed to start NTP server $0", i));
    }
  }
#endif // #if !defined(NO_CHRONY) ...

  if (opts_.enable_ranger || opts_.enable_ranger_kms) {
    if (!postgres_ || !postgres_->IsStarted()) {
      postgres_.reset(new postgres::MiniPostgres(cluster_root(), GetBindIpForExternalServer(0)));
    }
    string host = GetBindIpForExternalServer(0);
    ranger_.reset(new ranger::MiniRanger(cluster_root(), host, postgres_));
    if (opts_.enable_kerberos) {

      // The SPNs match the ones defined in mini_ranger_configs.h.
      string admin_keytab;
      RETURN_NOT_OK_PREPEND(kdc_->CreateServiceKeytab(
            Substitute("rangeradmin/$0@KRBTEST.COM", host),
            &admin_keytab),
          "could not create rangeradmin keytab");

      string lookup_keytab;
      RETURN_NOT_OK_PREPEND(kdc_->CreateServiceKeytab(
            Substitute("rangerlookup/$0@KRBTEST.COM", host),
            &lookup_keytab),
          "could not create rangerlookup keytab");

      string spnego_keytab;
      RETURN_NOT_OK_PREPEND(kdc_->CreateServiceKeytab(
            Substitute("HTTP/$0@KRBTEST.COM", host),
            &spnego_keytab),
          "could not create ranger HTTP keytab");

      ranger_->EnableKerberos(kdc_->GetEnvVars()["KRB5_CONFIG"], admin_keytab,
                              lookup_keytab, spnego_keytab);
    }

    RETURN_NOT_OK_PREPEND(ranger_->Start(), "Failed to start the Ranger service");
    RETURN_NOT_OK_PREPEND(ranger_->CreateClientConfig(JoinPathSegments(cluster_root(),
                                                                       "ranger-client")),
                          "Failed to write Ranger client config");
  }

  if (opts_.enable_ranger_kms) {
    string host = GetBindIpForExternalServer(0);
    ranger_kms_.reset(new rangerkms::MiniRangerKMS(cluster_root(), host, postgres_, ranger_));
    if (opts_.enable_kerberos) {
      string keytab;
      RETURN_NOT_OK_PREPEND(kdc_->CreateServiceKeytab(
          Substitute("rangerkms/$0@KRBTEST.COM", host),
          &keytab),
        "could not create ranger kms keytab");
      string spnego_keytab;
      RETURN_NOT_OK_PREPEND(kdc_->CreateServiceKeytab(
          Substitute("HTTP/$0@KRBTEST.COM", host),
          &spnego_keytab),
        "could not create ranger kms keytab");

      ranger_kms_->EnableKerberos(kdc_->GetEnvVars()["KRB5_CONFIG"], keytab, spnego_keytab);
    }
    RETURN_NOT_OK(kdc_->CreateUserPrincipal("keyadmin"));
    RETURN_NOT_OK(kdc_->Kinit("keyadmin"));
    RETURN_NOT_OK_PREPEND(ranger_kms_->Start(), "Failed to start the Ranger KMS service");
    RETURN_NOT_OK_PREPEND(ranger_kms_->CreateClusterKey(opts_.ranger_cluster_key,
                                                        &opts_.ranger_cluster_key_version),
                          "Failed to create cluster key");;
    RETURN_NOT_OK(kdc_->Kinit("test-admin"));
  }

  // Start the HMS.
  if (opts_.hms_mode == HmsMode::DISABLE_HIVE_METASTORE ||
      opts_.hms_mode == HmsMode::ENABLE_HIVE_METASTORE ||
      opts_.hms_mode == HmsMode::ENABLE_METASTORE_INTEGRATION) {
    hms_.reset(new hms::MiniHms());
    hms_->SetDataRoot(opts_.cluster_root);

    if (opts_.hms_mode == HmsMode::DISABLE_HIVE_METASTORE) {
      hms_->EnableKuduPlugin(false);
    }

    if (opts_.enable_kerberos) {
      string spn = Substitute("hive/$0", hms_->address().host());
      string ktpath;
      RETURN_NOT_OK_PREPEND(kdc_->CreateServiceKeytab(spn, &ktpath),
                            "could not create keytab");
      hms_->EnableKerberos(kdc_->GetEnvVars()["KRB5_CONFIG"], spn, ktpath,
                           rpc::SaslProtection::kAuthentication);

      // Set the protocol name in the environment so that the KuduMetastorePlugin
      // can communicate with Kudu when a custom name is used.
      hms_->AddEnvVar("KUDU_SASL_PROTOCOL_NAME", opts_.principal);
    }

    RETURN_NOT_OK_PREPEND(hms_->Start(),
                          "Failed to start the Hive Metastore");
  }

  RETURN_NOT_OK_PREPEND(StartMasters(), "failed to start masters");

  for (int i = 1; i <= opts_.num_tablet_servers; i++) {
    RETURN_NOT_OK_PREPEND(AddTabletServer(), Substitute("failed to start tablet server $0", i));
  }
  RETURN_NOT_OK(WaitForTabletServerCount(
                  opts_.num_tablet_servers,
                  MonoDelta::FromSeconds(kTabletServerRegistrationTimeoutSeconds)));

  return Status::OK();
}

void ExternalMiniCluster::ShutdownNodes(ClusterNodes nodes) {
  if (nodes == ClusterNodes::ALL || nodes == ClusterNodes::TS_ONLY) {
    for (const scoped_refptr<ExternalTabletServer>& ts : tablet_servers_) {
      ts->Shutdown();
    }
  }
  if (nodes == ClusterNodes::ALL || nodes == ClusterNodes::MASTERS_ONLY) {
    for (const scoped_refptr<ExternalMaster>& master : masters_) {
      if (master) {
        master->Shutdown();
      }
    }
  }
}

Status ExternalMiniCluster::Restart() {
  for (const scoped_refptr<ExternalMaster>& master : masters_) {
    if (master && master->IsShutdown()) {
      if (opts_.hms_mode == HmsMode::ENABLE_METASTORE_INTEGRATION) {
        master->SetMetastoreIntegration(hms_->uris(), opts_.enable_kerberos);
      }
      RETURN_NOT_OK_PREPEND(master->Restart(), "Cannot restart master bound at: " +
                                               master->bound_rpc_hostport().ToString());
    }
  }

  for (const scoped_refptr<ExternalTabletServer>& ts : tablet_servers_) {
    if (ts->IsShutdown()) {
      RETURN_NOT_OK_PREPEND(ts->Restart(), "Cannot restart tablet server bound at: " +
                                           ts->bound_rpc_hostport().ToString());
    }
  }

  RETURN_NOT_OK(WaitForTabletServerCount(
      tablet_servers_.size(),
      MonoDelta::FromSeconds(kTabletServerRegistrationTimeoutSeconds)));

  return Status::OK();
}

void ExternalMiniCluster::EnableMetastoreIntegration() {
  opts_.hms_mode = HmsMode::ENABLE_METASTORE_INTEGRATION;
}

void ExternalMiniCluster::DisableMetastoreIntegration() {
  for (const auto& master : masters_) {
    CHECK(master->IsShutdown()) << "Call Shutdown() before changing the HMS mode";
    master->mutable_flags()->erase(
        std::remove_if(
          master->mutable_flags()->begin(), master->mutable_flags()->end(),
          [] (const string& flag) {
            return StringPiece(flag).starts_with("--hive_metastore");
          }),
        master->mutable_flags()->end());
  }
  opts_.hms_mode = HmsMode::ENABLE_HIVE_METASTORE;
}

void ExternalMiniCluster::SetDaemonBinPath(string daemon_bin_path) {
  opts_.daemon_bin_path = std::move(daemon_bin_path);
  for (auto& master : masters_) {
    master->SetExePath(GetBinaryPath(kKuduBinaryName));
  }
  for (auto& ts : tablet_servers_) {
    ts->SetExePath(GetBinaryPath(kKuduBinaryName));
  }
}

string ExternalMiniCluster::GetBinaryPath(const string& binary) const {
  CHECK(!opts_.daemon_bin_path.empty());
  return JoinPathSegments(opts_.daemon_bin_path, binary);
}

string ExternalMiniCluster::GetLogPath(const string& daemon_id) const {
  CHECK(!opts_.cluster_root.empty());
  return JoinPathSegments(JoinPathSegments(opts_.cluster_root, daemon_id), "logs");
}

string ExternalMiniCluster::GetDataPath(const string& daemon_id,
                                        std::optional<uint32_t> dir_index) const {
  CHECK(!opts_.cluster_root.empty());
  string data_path = "data";
  if (dir_index) {
    CHECK_LT(*dir_index, opts_.num_data_dirs);
    data_path = Substitute("$0-$1", data_path, *dir_index);
  } else {
    CHECK_EQ(1, opts_.num_data_dirs);
  }
  return JoinPathSegments(JoinPathSegments(opts_.cluster_root, daemon_id), data_path);
}

vector<string> ExternalMiniCluster::GetDataPaths(const string& daemon_id) const {
  if (opts_.num_data_dirs == 1) {
    return { GetDataPath(daemon_id) };
  }
  vector<string> paths;
  for (uint32_t dir_index = 0; dir_index < opts_.num_data_dirs; dir_index++) {
    paths.emplace_back(GetDataPath(daemon_id, dir_index));
  }
  return paths;
}

string ExternalMiniCluster::GetWalPath(const string& daemon_id) const {
  CHECK(!opts_.cluster_root.empty());
  return JoinPathSegments(JoinPathSegments(opts_.cluster_root, daemon_id), "wal");
}

namespace {
vector<string> SubstituteInFlags(const vector<string>& orig_flags, int index) {
  string str_index = strings::Substitute("$0", index);
  vector<string> ret;
  for (const string& orig : orig_flags) {
    ret.push_back(StringReplace(orig, "${index}", str_index, true));
  }
  return ret;
}
} // anonymous namespace

Status ExternalMiniCluster::StartMasters() {
  int num_masters = opts_.num_masters;

  // Collect and keep alive the set of master sockets bound with SO_REUSEPORT
  // until all master proccesses are started. This allows the mini-cluster to
  // reserve a set of ports up front, then later start the set of masters, each
  // configured with the full set of ports.
  //
  // TODO(dan): re-bind the ports between node restarts in order to prevent other
  // processess from binding to them in the interim.
  vector<unique_ptr<Socket>> reserved_sockets;
  vector<HostPort> master_rpc_addrs;

  if (!opts_.master_rpc_addresses.empty()) {
    CHECK_EQ(opts_.master_rpc_addresses.size(), num_masters);
    master_rpc_addrs = opts_.master_rpc_addresses;
  } else {
    for (int i = 0; i < num_masters; i++) {
      unique_ptr<Socket> reserved_socket;
      RETURN_NOT_OK_PREPEND(ReserveDaemonSocket(DaemonType::MASTER, i, opts_.bind_mode,
                                                &reserved_socket),
                            "failed to reserve master socket address");
      Sockaddr addr;
      RETURN_NOT_OK(reserved_socket->GetSocketAddress(&addr));
      master_rpc_addrs.emplace_back(
          opts_.master_alias_prefix.empty() ?
              addr.host() : Substitute("$0.$1", opts_.master_alias_prefix, i),
          addr.port());
      reserved_sockets.emplace_back(std::move(reserved_socket));
    }
  }
  // Start the masters.
  for (int i = 0; i < num_masters; i++) {
    scoped_refptr<ExternalMaster> peer;
    RETURN_NOT_OK(CreateMaster(master_rpc_addrs, i, &peer));
    RETURN_NOT_OK_PREPEND(peer->Start(), Substitute("Unable to start Master at index $0", i));
    RETURN_NOT_OK(peer->SetServerKey());
    masters_.emplace_back(std::move(peer));
  }
  return Status::OK();
}

string ExternalMiniCluster::GetBindIpForTabletServer(int index) const {
  return MiniCluster::GetBindIpForDaemonWithType(MiniCluster::TSERVER, index,
                                                 opts_.bind_mode);
}

string ExternalMiniCluster::GetBindIpForMaster(int index) const {
  return MiniCluster::GetBindIpForDaemonWithType(MiniCluster::MASTER, index,
                                                 opts_.bind_mode);
}

string ExternalMiniCluster::GetBindIpForExternalServer(int index) const {
  return MiniCluster::GetBindIpForDaemonWithType(MiniCluster::EXTERNAL_SERVER,
                                                 index, opts_.bind_mode);
}

Status ExternalMiniCluster::AddTabletServer() {
  CHECK(leader_master() != nullptr)
      << "Must have started at least 1 master before adding tablet servers";

  const int idx = tablet_servers_.size();
  const string daemon_id = Substitute("ts-$0", idx);
  const string bind_host = GetBindIpForTabletServer(idx);

  ExternalDaemonOptions opts;
  opts.messenger = messenger_;
  opts.enable_encryption = opts_.enable_encryption;
  opts.enable_ranger_kms = opts_.enable_ranger_kms;
  opts.ranger_cluster_key = opts_.ranger_cluster_key;
  if (opts.enable_ranger_kms) {
    opts.ranger_kms_url = ranger_kms_->url();
  }
  opts.block_manager_type = opts_.block_manager_type;
  opts.exe = GetBinaryPath(kKuduBinaryName);
  opts.wal_dir = GetWalPath(daemon_id);
  opts.data_dirs = GetDataPaths(daemon_id);
  opts.log_dir = GetLogPath(daemon_id);
  if (FLAGS_perf_record) {
    opts.perf_record_filename =
        Substitute("$0/perf-$1.data", opts.log_dir, daemon_id);
  }
  vector<string> extra_flags;
  RETURN_NOT_OK(AddTimeSourceFlags(idx, &extra_flags));
  auto flags = SubstituteInFlags(opts_.extra_tserver_flags, idx);
  copy(flags.begin(), flags.end(), back_inserter(extra_flags));

  // Add custom tablet server flags, specific to this particular tablet server
  // instance.
  if (!opts_.t_custom_flags.empty()) {
    CHECK_EQ(opts_.num_tablet_servers, opts_.t_custom_flags.size());
    const auto& custom_flags = opts_.t_custom_flags[idx];
    copy(custom_flags.begin(), custom_flags.end(), back_inserter(extra_flags));
  }

  opts.extra_flags = extra_flags;
  if (!opts_.tserver_alias_prefix.empty()) {
    opts.extra_flags.emplace_back(
        Substitute("--host_for_tests=$0.$1",
                   opts_.tserver_alias_prefix, tablet_servers_.size()));
  }
  opts.start_process_timeout = opts_.start_process_timeout;
  opts.rpc_bind_address = HostPort(bind_host, 0);
  opts.logtostderr = opts_.logtostderr;

  vector<HostPort> master_hostports = master_rpc_addrs();
  scoped_refptr<ExternalTabletServer> ts = new ExternalTabletServer(opts, master_hostports);
  if (opts_.enable_kerberos) {
    RETURN_NOT_OK_PREPEND(ts->EnableKerberos(kdc_.get(), opts_.principal, bind_host),
                          "could not enable Kerberos");
  }

  RETURN_NOT_OK(ts->Start());
  RETURN_NOT_OK(ts->SetServerKey());
  tablet_servers_.push_back(ts);
  return Status::OK();
}

Status ExternalMiniCluster::CreateMaster(const vector<HostPort>& master_rpc_addrs,
                                         size_t idx,
                                         scoped_refptr<ExternalMaster>* master) {
  DCHECK_LT(idx, master_rpc_addrs.size());
  vector<string> flags;
  // We expect that callers have reserved a socket for the master we're
  // creating, and we'll thus have to have the daemon reuse the bound port.
  flags.emplace_back("--rpc_reuseport=true");
  // Setting --master_addresses flag for a single master configuration is now
  // supported but not mandatory. Not setting the flag helps test existing kudu
  // deployments that don't specify the --master_addresses flag for single
  // master configuration.
  if (master_rpc_addrs.size() > 1 || opts_.supply_single_master_addr) {
    flags.emplace_back(Substitute("--master_addresses=$0",
                                  HostPort::ToCommaSeparatedString(master_rpc_addrs)));
  }
  if (!opts_.location_info.empty()) {
    string bin_path;
    RETURN_NOT_OK(DeduceBinRoot(&bin_path));
    const auto mapping_script_path =
        JoinPathSegments(bin_path, "testdata/assign-location.py");
    const auto state_store_fpath =
        JoinPathSegments(opts_.cluster_root, "location-assignment.state");
    auto location_cmd = Substitute("$0 --state_store=$1",
                                   mapping_script_path, state_store_fpath);
    for (const auto& elem : opts_.location_info) {
      // Per-location mapping rule specified as a pair 'location:num_servers',
      // where 'location' is the location string and 'num_servers' is the number
      // of tablet servers to be assigned the location.
      location_cmd += Substitute(" --map $0:$1", elem.first, elem.second);
    }
    flags.emplace_back(Substitute("--location_mapping_cmd=$0", location_cmd));
#   if defined(__APPLE__)
    // On macOS, it's not possible to have unique loopback interfaces. To make
    // location mapping working, a tablet server is identified by its UUID
    // instead of IP address of its RPC end-point.
    flags.emplace_back("--location_mapping_by_uuid");
#   endif
  }
  if (opts_.hms_mode == HmsMode::ENABLE_METASTORE_INTEGRATION) {
    flags.emplace_back(Substitute("--hive_metastore_uris=$0", hms_->uris()));
    if (opts_.enable_kerberos) {
      flags.emplace_back("--hive_metastore_sasl_enabled=true");
    }
  }
  if (opts_.enable_ranger) {
    flags.emplace_back(Substitute("--ranger_config_path=$0",
                                  JoinPathSegments(cluster_root(),
                                                   "ranger-client")));
    flags.emplace_back("--trusted_user_acl=test-admin");
  }
  if (opts_.enable_client_jwt) {
    flags.emplace_back("--enable_jwt_token_auth=true");
    flags.emplace_back(Substitute("--jwks_url=$0", oidc_->url()));
    flags.emplace_back(Substitute("--jwks_discovery_endpoint_base=$0", oidc_->url()));
  }
  if (!opts_.master_alias_prefix.empty()) {
    flags.emplace_back(Substitute("--host_for_tests=$0.$1",
                                  opts_.master_alias_prefix, idx));
  }

  if (opts_.enable_encryption) {
    flags.emplace_back("--encrypt_data_at_rest=true");
    if (opts_.enable_ranger_kms) {
      flags.emplace_back("--encryption_key_provider=ranger-kms");
      flags.emplace_back(Substitute("--encryption_cluster_key_name=$0", opts_.ranger_cluster_key));
      flags.emplace_back(Substitute("--ranger_kms_url=$0", ranger_kms_->url()));
    }
  }
  // Add extra master flags, common for all masters in the cluster.
  copy(opts_.extra_master_flags.begin(), opts_.extra_master_flags.end(),
       back_inserter(flags));

  // Add custom master flags, specific to this particular master instance.
  if (!opts_.m_custom_flags.empty()) {
    CHECK_EQ(opts_.num_masters, opts_.m_custom_flags.size());
    const auto& custom_flags = opts_.m_custom_flags[idx];
    copy(custom_flags.begin(), custom_flags.end(), back_inserter(flags));
  }

  string daemon_id = Substitute("master-$0", idx);

  ExternalDaemonOptions opts;
  opts.messenger = messenger_;
  opts.block_manager_type = opts_.block_manager_type;
  opts.exe = GetBinaryPath(kKuduBinaryName);
  opts.wal_dir = GetWalPath(daemon_id);
  opts.data_dirs = GetDataPaths(daemon_id);
  opts.log_dir = GetLogPath(daemon_id);
  if (FLAGS_perf_record) {
    opts.perf_record_filename =
        Substitute("$0/perf-$1.data", opts.log_dir, daemon_id);
  }

  vector<string> time_source_flags;
  RETURN_NOT_OK(AddTimeSourceFlags(idx, &time_source_flags));
  // Custom flags set above come last because they can contain overrides.
  flags.insert(flags.begin(), time_source_flags.begin(), time_source_flags.end());

  opts.extra_flags = SubstituteInFlags(flags, idx);
  opts.start_process_timeout = opts_.start_process_timeout;
  opts.rpc_bind_address = master_rpc_addrs[idx];
  opts.logtostderr = opts_.logtostderr;

  scoped_refptr<ExternalMaster> peer(new ExternalMaster(opts));
  if (opts_.enable_kerberos) {
    RETURN_NOT_OK_PREPEND(
        peer->EnableKerberos(kdc_.get(), opts_.principal, master_rpc_addrs[idx].host()),
        "could not enable Kerberos");
  }
  *master = std::move(peer);
  return Status::OK();
}

Status ExternalMiniCluster::AddMaster(const vector<string>& extra_flags) {
  const int idx = masters_.size();
  const string daemon_id = Substitute("master-$0", idx);

  unique_ptr<Socket> reserved_socket;
  RETURN_NOT_OK_PREPEND(ReserveDaemonSocket(DaemonType::MASTER, idx, opts_.bind_mode,
                                            &reserved_socket),
                        "failed to reserve master socket address");
  Sockaddr addr;
  RETURN_NOT_OK(reserved_socket->GetSocketAddress(&addr));
  vector<HostPort> master_rpc_addrs = this->master_rpc_addrs();
  master_rpc_addrs.emplace_back(addr.host(), addr.port());
  scoped_refptr<ExternalMaster> peer;
  RETURN_NOT_OK(CreateMaster(master_rpc_addrs, idx, &peer));
  peer->mutable_flags()->insert(peer->mutable_flags()->end(),
                                extra_flags.begin(), extra_flags.end());
  RETURN_NOT_OK_PREPEND(peer->Start(),
                        Substitute("unable to start master at index $0", idx));
  // Update the existing servers' gflags so the new master is accounted for the
  // next time they restart.
  // NOTE: the new master already has the correct list set for this flag, from
  // the call to CreateMaster().
  const auto& new_master_addrs_list = HostPort::ToCommaSeparatedString(master_rpc_addrs);
  for (auto& master : masters_) {
    master->mutable_flags()->emplace_back(
        Substitute("--master_addresses=$0", new_master_addrs_list));
  }
  for (auto& ts : tablet_servers_) {
    ts->mutable_flags()->emplace_back(
        Substitute("--tserver_master_addrs=$0", new_master_addrs_list));
  }
  masters_.emplace_back(std::move(peer));
  ++opts_.num_masters;
  return Status::OK();
}

#if !defined(NO_CHRONY)
Status ExternalMiniCluster::AddNtpServer(const Sockaddr& addr) {
  clock::MiniChronydOptions options;
  options.index = ntp_servers_.size();
  options.data_root = JoinPathSegments(cluster_root(),
                                       Substitute("chrony.$0", options.index));
  options.bindaddress = addr.host();
  options.port = static_cast<uint16_t>(addr.port());
  unique_ptr<MiniChronyd> chrony(new MiniChronyd(std::move(options)));
  RETURN_NOT_OK(chrony->Start());
  ntp_servers_.emplace_back(std::move(chrony));
  return Status::OK();
}
#endif // #if !defined(NO_CHRONY) ...

Status ExternalMiniCluster::WaitForTabletServerCount(int count, const MonoDelta& timeout,
                                                     int master_idx) {
  CHECK_LT(master_idx, opts_.num_masters);
  MonoTime deadline = MonoTime::Now() + timeout;

  unordered_set<int> masters_to_search;
  if (master_idx == -1) {
    for (int i = 0; i < masters_.size(); i++) {
      if (!masters_[i]->IsShutdown()) {
        masters_to_search.insert(i);
      }
    }
  } else {
    if (!masters_[master_idx]->IsShutdown()) {
      masters_to_search.insert(master_idx);
    }
  }

  while (true) {
    MonoDelta remaining = deadline - MonoTime::Now();
    if (remaining.ToSeconds() < 0) {
      return Status::TimedOut(Substitute(
          "Timed out waiting for $0 TS(s) to register with all masters", count));
    }
    bool all_masters_reachable = true;
    for (const auto& master_idx : masters_to_search) {
      master::PingRequestPB req;
      master::PingResponsePB resp;
      rpc::RpcController rpc;
      rpc.set_timeout(remaining);
      Status s = master_proxy(master_idx)->Ping(req, &resp, &rpc);
      if (!s.ok()) {
        all_masters_reachable = false;
        break;
      }
    }
    if (!all_masters_reachable) {
      SleepFor(MonoDelta::FromMilliseconds(10));
      continue;
    }

    for (auto iter = masters_to_search.begin(); iter != masters_to_search.end();) {
      master::ListTabletServersRequestPB req;
      master::ListTabletServersResponsePB resp;
      rpc::RpcController rpc;
      rpc.set_timeout(remaining);
      RETURN_NOT_OK_PREPEND(master_proxy(*iter)->ListTabletServers(req, &resp, &rpc),
                            "ListTabletServers RPC failed");
      // ListTabletServers() may return servers that are no longer online.
      // Do a second step of verification to verify that the descs that we got
      // are aligned (same uuid/seqno) with the TSs that we have in the cluster.
      int match_count = 0;
      for (const master::ListTabletServersResponsePB_Entry& e : resp.servers()) {
        for (const scoped_refptr<ExternalTabletServer>& ets : tablet_servers_) {
          if (ets->instance_id().permanent_uuid() == e.instance_id().permanent_uuid() &&
              ets->instance_id().instance_seqno() == e.instance_id().instance_seqno()) {
            match_count++;
            break;
          }
        }
      }
      if (match_count == count) {
        // This master has returned the correct set of tservers.
        iter = masters_to_search.erase(iter);
      } else {
        iter++;
      }
    }

    if (masters_to_search.empty()) {
      // All masters have returned the correct set of tservers.
      LOG(INFO) << count << " TS(s) registered with all masters";
      return Status::OK();
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
}

void ExternalMiniCluster::AssertNoCrashes() {
  vector<ExternalDaemon*> daemons = this->daemons();
  int num_crashes = 0;
  for (ExternalDaemon* d : daemons) {
    if (d->IsShutdown()) continue;
    if (!d->IsProcessAlive()) {
      LOG(ERROR) << "Process with UUID " << d->uuid() << " has crashed";
      num_crashes++;
    }
  }
  ASSERT_EQ(0, num_crashes) << "At least one process crashed";
}

Status ExternalMiniCluster::WaitForTabletsRunning(
    ExternalTabletServer* ts,
    int min_tablet_count,
    const MonoDelta& timeout,
    vector<TabletIdAndTableName>* tablets_info) {
  TabletServerServiceProxy proxy(messenger_, ts->bound_rpc_addr(), ts->bound_rpc_addr().host());
  ListTabletsRequestPB req;
  ListTabletsResponsePB resp;

  MonoTime deadline = MonoTime::Now() + timeout;
  while (MonoTime::Now() < deadline) {
    rpc::RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy.ListTablets(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }

    bool all_running = true;
    for (const StatusAndSchemaPB& status : resp.status_and_schema()) {
      if (status.tablet_status().state() != tablet::RUNNING) {
        all_running = false;
      }
    }

    // We're done if:
    // 1. All the tablets are running, and
    // 2. We've observed as many tablets as we had expected or more.
    if (all_running && resp.status_and_schema_size() >= min_tablet_count) {
      if (tablets_info) {
        tablets_info->clear();
        const auto num_elems = resp.status_and_schema_size();
        tablets_info->reserve(num_elems);
        for (auto i = 0; i < num_elems; ++i) {
          const auto& elem = resp.status_and_schema(i);
          tablets_info->emplace_back(
              TabletIdAndTableName{elem.tablet_status().tablet_id(),
                                   elem.tablet_status().table_name()});
        }
      }
      return Status::OK();
    }

    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  return Status::TimedOut(SecureDebugString(resp));
}

Status ExternalMiniCluster::GetLeaderMasterIndex(int* idx) {
  scoped_refptr<ConnectToClusterRpc> rpc;
  Synchronizer sync;
  vector<pair<Sockaddr, string>> addrs_with_names;
  Sockaddr leader_master_addr;
  string leader_master_hostname;
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(5);

  for (const scoped_refptr<ExternalMaster>& master : masters_) {
    addrs_with_names.emplace_back(master->bound_rpc_addr(), master->bound_rpc_addr().host());
  }
  const auto& cb = [&](const Status& status,
                       const pair<Sockaddr, string>& leader_master,
                       const master::ConnectToMasterResponsePB& /*resp*/) {
    if (status.ok()) {
      leader_master_addr = leader_master.first;
      leader_master_hostname = leader_master.second;
    }
    sync.StatusCB(status);
  };
  rpc::UserCredentials user_credentials;
  RETURN_NOT_OK(user_credentials.SetLoggedInRealUser());
  rpc.reset(new ConnectToClusterRpc(cb,
                                    std::move(addrs_with_names),
                                    deadline,
                                    MonoDelta::FromSeconds(5),
                                    messenger_,
                                    user_credentials));
  rpc->SendRpc();
  RETURN_NOT_OK(sync.Wait());
  bool found = false;
  for (int i = 0; i < masters_.size(); i++) {
    const auto& bound_hp = masters_[i]->bound_rpc_hostport();
    // If using BindMode::UNIQUE_LOOPBACK mode, in rare cases different masters
    // might bind to different local IP addresses but use same port numbers.
    // So, it's necessary to check both the returned hostnames and IP addresses
    // to point to leader master.
    if (bound_hp.port() == leader_master_addr.port() &&
        bound_hp.host() == leader_master_hostname) {
      found = true;
      *idx = i;
      break;
    }
  }
  if (!found) {
    // There is never a situation where this should happen, so it's
    // better to exit with a FATAL log message right away vs. return a
    // Status::IllegalState().
    LOG(FATAL) << "Leader master is not in masters_";
  }
  return Status::OK();
}

ExternalTabletServer* ExternalMiniCluster::tablet_server_by_uuid(const std::string& uuid) const {
  for (const scoped_refptr<ExternalTabletServer>& ts : tablet_servers_) {
    if (ts->instance_id().permanent_uuid() == uuid) {
      return ts.get();
    }
  }
  return nullptr;
}

int ExternalMiniCluster::tablet_server_index_by_uuid(const std::string& uuid) const {
  for (int i = 0; i < tablet_servers_.size(); i++) {
    if (tablet_servers_[i]->uuid() == uuid) {
      return i;
    }
  }
  return -1;
}

vector<ExternalDaemon*> ExternalMiniCluster::daemons() const {
  vector<ExternalDaemon*> results;
  for (const scoped_refptr<ExternalTabletServer>& ts : tablet_servers_) {
    results.push_back(ts.get());
  }
  for (const scoped_refptr<ExternalMaster>& master : masters_) {
    results.push_back(master.get());
  }
  return results;
}

#if !defined(NO_CHRONY)
vector<MiniChronyd*> ExternalMiniCluster::ntp_servers() const {
  vector<MiniChronyd*> servers;
  servers.reserve(ntp_servers_.size());
  for (const auto& server : ntp_servers_) {
    DCHECK(server);
    servers.emplace_back(server.get());
  }
  return servers;
}
#endif // #if !defined(NO_CHRONY) ...

vector<HostPort> ExternalMiniCluster::master_rpc_addrs() const {
  vector<HostPort> master_hostports;
  for (const auto& master : masters_) {
    master_hostports.emplace_back(master->bound_rpc_hostport());
  }
  return master_hostports;
}

std::shared_ptr<rpc::Messenger> ExternalMiniCluster::messenger() const {
  return messenger_;
}

std::shared_ptr<MasterServiceProxy> ExternalMiniCluster::master_proxy() const {
  CHECK_EQ(masters_.size(), 1);
  return master_proxy(0);
}

std::shared_ptr<MasterServiceProxy> ExternalMiniCluster::master_proxy(int idx) const {
  CHECK_LT(idx, masters_.size());
  const auto& addr = CHECK_NOTNULL(master(idx))->bound_rpc_addr();
  return std::make_shared<MasterServiceProxy>(messenger_, addr, addr.host());
}

std::shared_ptr<TabletServerServiceProxy> ExternalMiniCluster::tserver_proxy(int idx) const {
  CHECK_LT(idx, tablet_servers_.size());
  const auto& addr = CHECK_NOTNULL(tablet_server(idx))->bound_rpc_addr();
  return std::make_shared<TabletServerServiceProxy>(messenger_, addr, addr.host());
}

std::shared_ptr<TabletServerAdminServiceProxy> ExternalMiniCluster::tserver_admin_proxy(
    int idx) const {
  CHECK_LT(idx, tablet_servers_.size());
  const auto& addr = CHECK_NOTNULL(tablet_server(idx))->bound_rpc_addr();
  return std::make_shared<TabletServerAdminServiceProxy>(messenger_, addr, addr.host());
}

Status ExternalMiniCluster::CreateClient(client::KuduClientBuilder* builder,
                                         client::sp::shared_ptr<client::KuduClient>* client) const {
  client::KuduClientBuilder defaults;
  if (builder == nullptr) {
    builder = &defaults;
  }

  CHECK(!masters_.empty());
  builder->clear_master_server_addrs();
  for (const scoped_refptr<ExternalMaster>& master : masters_) {
    builder->add_master_server_addr(master->bound_rpc_hostport().ToString());
  }
  builder->sasl_protocol_name(opts_.principal);
  return builder->Build(client);
}

Status ExternalMiniCluster::SetFlag(ExternalDaemon* daemon,
                                    const string& flag,
                                    const string& value) {
  const auto& addr = daemon->bound_rpc_addr();
  server::GenericServiceProxy proxy(messenger_, addr, addr.host());

  rpc::RpcController controller;
  controller.set_timeout(MonoDelta::FromSeconds(30));
  server::SetFlagRequestPB req;
  server::SetFlagResponsePB resp;
  req.set_flag(flag);
  req.set_value(value);
  req.set_force(true);
  RETURN_NOT_OK_PREPEND(proxy.SetFlag(req, &resp, &controller),
                        "rpc failed");
  if (resp.result() != server::SetFlagResponsePB::SUCCESS) {
    return Status::RemoteError("failed to set flag",
                               SecureShortDebugString(resp));
  }
  return Status::OK();
}

string ExternalMiniCluster::WalRootForTS(int ts_idx) const {
  return tablet_server(ts_idx)->wal_dir();
}

string ExternalMiniCluster::UuidForTS(int ts_idx) const {
  return tablet_server(ts_idx)->uuid();
}

Status ExternalMiniCluster::RemoveMaster(const HostPort& hp) {
  for (auto it = masters_.begin(); it != masters_.end(); ++it) {
    if ((*it)->bound_rpc_hostport() == hp) {
      masters_.erase(it);
      return Status::OK();
    }
  }
  return Status::NotFound(Substitute("Master $0 not found in ExternalMiniCluster", hp.ToString()));
}

//------------------------------------------------------------
// ExternalDaemon
//------------------------------------------------------------

ExternalDaemon::ExternalDaemon(ExternalDaemonOptions opts)
    : opts_(std::move(opts)),
      key_provider_(new DefaultKeyProvider()),
      parent_tid_(std::this_thread::get_id()) {
  CHECK(rpc_bind_address().Initialized());
}

ExternalDaemon::~ExternalDaemon() {
}

std::vector<std::string> ExternalDaemon::GetDaemonFlags(const ExternalDaemonOptions& opts) {
  const string info_path = JoinPathSegments(opts.data_dirs[0], "info.pb");
  vector<string> flags = {
      // Basic flags for a Kudu server.
      "--fs_wal_dir=" + opts.wal_dir,
      "--fs_data_dirs=" + JoinStrings(opts.data_dirs, ","),
      "--block_manager=" + opts.block_manager_type,
      "--webserver_interface=localhost",

      // Disable fsync to dramatically speed up runtime. This is safe as no tests
      // rely on forcefully cutting power to a machine or equivalent.
      "--never_fsync",

      // Disable minidumps by default since many tests purposely inject faults.
      "--enable_minidumps=false",

      // Disable redaction of the information in logs and Web UI.
      "--redact=none",

      // Enable metrics logging.
      "--metrics_log_interval_ms=1000",

      // Even if we are logging to stderr, metrics logs and minidumps end up being
      // written based on -log_dir. So, we have to set that too.
      "--log_dir=" + opts.log_dir,

      // Tell the server to dump its port information so we can pick it up.
      "--server_dump_info_path=" + info_path,
      "--server_dump_info_format=pb",

      // We use ephemeral ports in many tests. They don't work for production,
      // but are OK in unit tests.
      "--rpc_server_allow_ephemeral_ports",

      // Allow unsafe and experimental flags from tests, since we often use
      // fault injection, etc.
      "--unlock_experimental_flags",
      "--unlock_unsafe_flags",
  };

  if (opts.logtostderr) {
    // Ensure that logging goes to the test output and doesn't get buffered.
    flags.emplace_back("--logtostderr");
    flags.emplace_back("--logbuflevel=-1");
  }

  if (opts.enable_encryption) {
    flags.emplace_back("--encrypt_data_at_rest=true");
    if (opts.enable_ranger_kms) {
      flags.emplace_back("--encryption_key_provider=ranger-kms");
      flags.emplace_back(Substitute("--encryption_cluster_key_name=$0", opts.ranger_cluster_key));
      flags.emplace_back(Substitute("--ranger_kms_url=$0", opts.ranger_kms_url));
    }
  }

  // If large keys are not enabled.
  if (!UseLargeKeys()) {
    // Generate smaller RSA keys -- generating a 768-bit key is faster
    // than generating the default 2048-bit key, and we don't care about
    // strong encryption in tests. Setting it lower (e.g. 512 bits) results
    // in OpenSSL errors RSA_sign:digest too big for rsa key:rsa_sign.c:122
    // since we are using strong/high TLS v1.2 cipher suites, so the minimum
    // size of TLS-related RSA key is 768 bits (due to the usage of
    // the ECDHE-RSA-AES256-GCM-SHA384 suite).
    flags.emplace_back("--ipki_server_key_size=768");

    // The RSA key of 768 bits is too short if OpenSSL security level is set to
    // 1 or higher (applicable for OpenSSL 1.1.0 and newer). Lowering the
    // security level to 0 makes possible ot use shorter keys in such cases.
    flags.emplace_back("--openssl_security_level_override=0");
  }

  return flags;
}

Status ExternalDaemon::StartProcess(const vector<string>& user_flags) {
  CHECK(!process_);
  const auto this_tid = std::this_thread::get_id();
  CHECK_EQ(parent_tid_, this_tid)
    << "Process being started from thread " << this_tid << " which is different"
    << " from the instantiating thread " << parent_tid_;

  RETURN_NOT_OK(env_util::CreateDirsRecursively(Env::Default(), log_dir()));
  const string info_path = JoinPathSegments(data_dirs()[0], "info.pb");

  vector<string> argv = { /* First the exe for argv[0] */ opts_.exe };
  vector<string> flags = GetDaemonFlags(opts_);
  argv.insert(argv.end(), std::make_move_iterator(flags.begin()),
              std::make_move_iterator(flags.end()));

  // Add all the flags coming from the minicluster framework.
  argv.insert(argv.end(), user_flags.begin(), user_flags.end());

  // Then the "extra flags" passed into the ctor (from the ExternalMiniCluster
  // options struct). These come at the end so they can override things like
  // web port or RPC bind address if necessary.
  argv.insert(argv.end(), opts_.extra_flags.begin(), opts_.extra_flags.end());

  // A previous instance of the daemon may have run in the same directory. So, remove
  // the previous info file if it's there.
  ignore_result(Env::Default()->DeleteFile(info_path));

  // Start the daemon.
  unique_ptr<Subprocess> p(new Subprocess(argv));
  p->SetEnvVars(extra_env_);
  string env_str;
  JoinMapKeysAndValues(extra_env_, "=", ",", &env_str);
  LOG(INFO) << "Running " << opts_.exe << "\n" << JoinStrings(argv, "\n")
            << " with env {" << env_str << "}";
  RETURN_NOT_OK_PREPEND(p->Start(),
                        Substitute("Failed to start subprocess $0", opts_.exe));

  // If requested, start a monitoring subprocess.
  unique_ptr<Subprocess> perf_record;
  if (!opts_.perf_record_filename.empty()) {
    perf_record.reset(new Subprocess({
      "perf",
      "record",
      "--call-graph",
      "fp",
      "-o",
      opts_.perf_record_filename,
      Substitute("--pid=$0", p->pid())
    }, SIGINT));
    RETURN_NOT_OK_PREPEND(perf_record->Start(),
                          "Could not start perf record subprocess");
  }

  // The process is now starting -- wait for the bound port info to show up.
  Stopwatch sw;
  sw.start();
  bool success = false;
  while (sw.elapsed().wall_seconds() < opts_.start_process_timeout.ToSeconds()) {
    if (Env::Default()->FileExists(info_path)) {
      success = true;
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
    int wait_status;
    Status s = p->WaitNoBlock(&wait_status);
    if (s.IsTimedOut()) {
      // The process is still running.
      continue;
    }

    // If the process exited with expected exit status we need to still swap() the process
    // and exit as if it had succeeded.
    if (WIFEXITED(wait_status) && WEXITSTATUS(wait_status) == fault_injection::kExitStatus) {
      process_.swap(p);
      perf_record_process_.swap(perf_record);
      return Status::OK();
    }

    RETURN_NOT_OK_PREPEND(s, Substitute("Failed waiting on $0", opts_.exe));
    string exit_info;
    RETURN_NOT_OK(p->GetExitStatus(nullptr, &exit_info));
    return Status::RuntimeError(exit_info);
  }

  if (!success) {
    ignore_result(p->Kill(SIGKILL));
    return Status::TimedOut(
        Substitute("Timed out after $0 waiting for process ($1) to write info file ($2)",
                   opts_.start_process_timeout.ToString(), opts_.exe, info_path));
  }

  status_.reset(new ServerStatusPB());
  RETURN_NOT_OK_PREPEND(pb_util::ReadPBFromPath(Env::Default(), info_path, status_.get()),
                        "Failed to read info file from " + info_path);
  LOG(INFO) << "Started " << opts_.exe << " as pid " << p->pid();
  VLOG(1) << opts_.exe << " instance information:\n" << SecureDebugString(*status_);

  process_.swap(p);
  perf_record_process_.swap(perf_record);
  return Status::OK();
}

Env* ExternalDaemon::env() const {
  return Env::Default();
}

Status ExternalDaemon::SetServerKey() {
  string path = JoinPathSegments(this->wal_dir(), "instance");;
  LOG(INFO) << "Reading " << path;
  InstanceMetadataPB instance;
  RETURN_NOT_OK(pb_util::ReadPBContainerFromPath(env(), path, &instance, pb_util::NOT_SENSITIVE));
  if (!instance.server_key().empty()) {
    string key;
    RETURN_NOT_OK(key_provider_->DecryptServerKey(instance.server_key(),
                                                  instance.server_key_iv(),
                                                  instance.server_key_version(),
                                                  &key));
    LOG(INFO) << "Setting key " << key;
    env()->SetEncryptionKey(reinterpret_cast<const uint8_t*>(a2b_hex(key).c_str()), key.size() * 4);
  }
  return Status::OK();
}

void ExternalDaemon::SetExePath(string exe) {
  CHECK(IsShutdown()) << "Call Shutdown() before changing the executable path";
  opts_.exe = std::move(exe);
}

void ExternalDaemon::SetMetastoreIntegration(const string& hms_uris,
                                             bool enable_kerberos) {
  opts_.extra_flags.emplace_back(Substitute("--hive_metastore_uris=$0", hms_uris));
  opts_.extra_flags.emplace_back(Substitute("--hive_metastore_sasl_enabled=$0", enable_kerberos));
}

Status ExternalDaemon::CreateKerberosConfig(MiniKdc* kdc,
                                            const string& principal_base,
                                            const string& bind_host,
                                            vector<string>* flags,
                                            map<string, string>* env_vars) {
  string spn = principal_base + "/" + bind_host;
  string ktpath;
  RETURN_NOT_OK_PREPEND(kdc->CreateServiceKeytab(spn, &ktpath),
                        "could not create keytab");
  *env_vars = kdc->GetEnvVars();
  *flags =  {
      Substitute("--keytab_file=$0", ktpath),
      Substitute("--principal=$0", spn),
      "--rpc_authentication=required",
      "--superuser_acl=test-admin",
      "--user_acl=test-user",
  };

  return Status::OK();
}

Status ExternalDaemon::EnableKerberos(MiniKdc* kdc, const string& principal_base,
                                      const string& bind_host) {
  vector<string> flags;
  RETURN_NOT_OK(CreateKerberosConfig(kdc, principal_base, bind_host, &flags, &extra_env_));
  // Insert Kerberos flags at the front of extra_flags, so that user specified
  // flags will override them.
  opts_.extra_flags.insert(opts_.extra_flags.begin(), std::make_move_iterator(flags.begin()),
                           std::make_move_iterator(flags.end()));
  return Status::OK();
}

Status ExternalDaemon::Pause() {
  if (!process_) {
    return Status::IllegalState(Substitute(
        "Request to pause '$0' but the process is not there", opts_.exe));
  }
  VLOG(1) << "Pausing " << opts_.exe << " with pid " << process_->pid();
  const Status s = process_->Kill(SIGSTOP);
  RETURN_NOT_OK(s);
  paused_ = true;
  return s;
}

Status ExternalDaemon::Resume() {
  if (!process_) {
    return Status::IllegalState(Substitute(
        "Request to resume '$0' but the process is not there", opts_.exe));
  }
  VLOG(1) << "Resuming " << opts_.exe << " with pid " << process_->pid();
  const Status s = process_->Kill(SIGCONT);
  RETURN_NOT_OK(s);
  paused_ = false;
  return s;
}

bool ExternalDaemon::IsShutdown() const {
  return !process_;
}

bool ExternalDaemon::IsProcessAlive() const {
  if (IsShutdown()) {
    return false;
  }
  Status s = process_->WaitNoBlock();
  // If the non-blocking Wait "times out", that means the process
  // is running.
  return s.IsTimedOut();
}

Status ExternalDaemon::WaitForInjectedCrash(const MonoDelta& timeout) const {
  return WaitForCrash(timeout, [](int status) {
      return WIFEXITED(status) && WEXITSTATUS(status) == fault_injection::kExitStatus;
    }, "fault injection");
}

Status ExternalDaemon::WaitForFatal(const MonoDelta& timeout) const {
  return WaitForCrash(timeout, [](int status) {
      return WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT;
    }, "FATAL crash");
}


Status ExternalDaemon::WaitForCrash(const MonoDelta& timeout,
                                    const std::function<bool(int)>& wait_status_predicate,
                                    const char* crash_type_str) const {
  CHECK(process_) << "process not started";
  MonoTime deadline = MonoTime::Now() + timeout;

  int i = 1;
  while (IsProcessAlive() && MonoTime::Now() < deadline) {
    int sleep_ms = std::min(i++ * 10, 200);
    SleepFor(MonoDelta::FromMilliseconds(sleep_ms));
  }

  if (IsProcessAlive()) {
    return Status::TimedOut(Substitute("Process did not crash within $0",
                                       timeout.ToString()));
  }

  // If the process has exited, make sure it exited with the expected status.
  int wait_status;
  RETURN_NOT_OK_PREPEND(process_->WaitNoBlock(&wait_status),
                        "could not get wait status");

  if (!wait_status_predicate(wait_status)) {
    string info_str;
    RETURN_NOT_OK_PREPEND(process_->GetExitStatus(nullptr, &info_str),
                          "could not get description of exit");
    return Status::Aborted(
        Substitute("process exited, but not due to a $0: $1", crash_type_str, info_str));
  }
  return Status::OK();
}

pid_t ExternalDaemon::pid() const {
  return process_->pid();
}

Subprocess* ExternalDaemon::process() const {
  return process_.get();
}

void ExternalDaemon::Shutdown() {
  if (!process_) return;

  // Before we kill the process, store the addresses. If we're told to
  // start again we'll reuse these. Store only the port if the
  // daemons were using wildcard address for binding.
  if (rpc_bind_address().host() != kWildcardIpAddr) {
    bound_rpc_ = bound_rpc_hostport();
    bound_http_ = bound_http_hostport();
  } else {
    bound_rpc_.set_host(kWildcardIpAddr);
    bound_rpc_.set_port(bound_rpc_hostport().port());
    bound_http_.set_host(kWildcardIpAddr);
    bound_http_.set_port(bound_http_hostport().port());
  }

  if (IsProcessAlive()) {
    if (!paused_) {
      // In coverage builds, ask the process nicely to flush coverage info
      // before we kill -9 it. Otherwise, we never get any coverage from
      // external clusters.
      FlushCoverage();
      // Similarly, check for leaks in LSAN builds before killing.
      CheckForLeaks();
    }

    LOG(INFO) << "Killing " << opts_.exe << " with pid " << process_->pid();
    ignore_result(process_->Kill(SIGKILL));
  }
  WARN_NOT_OK(process_->Wait(), "Waiting on " + opts_.exe);
  paused_ = false;
  process_.reset();
  perf_record_process_.reset();
}

Status ExternalDaemon::DeleteFromDisk() const {
  for (const string& data_dir : data_dirs()) {
    RETURN_NOT_OK(Env::Default()->DeleteRecursively(data_dir));
  }
  RETURN_NOT_OK(Env::Default()->DeleteRecursively(wal_dir()));
  return Status::OK();
}

void ExternalDaemon::FlushCoverage() {
#ifndef COVERAGE_BUILD
  return; // NOLINT(*)
#else
  LOG(INFO) << "Attempting to flush coverage for " << opts_.exe << " pid " << process_->pid();
  server::GenericServiceProxy proxy(
      opts_.messenger, bound_rpc_addr(), bound_rpc_addr().host());

  server::FlushCoverageRequestPB req;
  server::FlushCoverageResponsePB resp;
  rpc::RpcController rpc;

  rpc.set_timeout(MonoDelta::FromMilliseconds(1000));
  Status s = proxy.FlushCoverage(req, &resp, &rpc);
  if (s.ok() && !resp.success()) {
    s = Status::RemoteError("Server does not appear to be running a coverage build");
  }
  WARN_NOT_OK(s, Substitute("Unable to flush coverage on $0 pid $1", opts_.exe, process_->pid()));
#endif
}

void ExternalDaemon::CheckForLeaks() {
#if defined(__has_feature)
#  if __has_feature(address_sanitizer)
  LOG(INFO) << "Attempting to check leaks for " << opts_.exe << " pid " << process_->pid();
  server::GenericServiceProxy proxy(opts_.messenger, bound_rpc_addr(), bound_rpc_addr().host());

  server::CheckLeaksRequestPB req;
  server::CheckLeaksResponsePB resp;
  rpc::RpcController rpc;

  rpc.set_timeout(MonoDelta::FromMilliseconds(1000));
  Status s = proxy.CheckLeaks(req, &resp, &rpc);
  if (s.ok()) {
    if (!resp.success()) {
      s = Status::RemoteError("Server does not appear to be running an LSAN build");
    } else {
      CHECK(!resp.found_leaks()) << "Found leaks in " << opts_.exe << " pid " << process_->pid();
    }
  }
  WARN_NOT_OK(s, Substitute("Unable to check leaks on $0 pid $1", opts_.exe, process_->pid()));
#  endif
#endif
}

HostPort ExternalDaemon::bound_rpc_hostport() const {
  CHECK(status_);
  CHECK_GE(status_->bound_rpc_addresses_size(), 1);
  return HostPortFromPB(status_->bound_rpc_addresses(0));
}

Sockaddr ExternalDaemon::bound_rpc_addr() const {
  HostPort hp = bound_rpc_hostport();
  vector<Sockaddr> addrs;
  CHECK_OK(hp.ResolveAddresses(&addrs));
  CHECK(!addrs.empty());
  return addrs[0];
}

HostPort ExternalDaemon::bound_http_hostport() const {
  CHECK(status_);
  if (status_->bound_http_addresses_size() == 0) {
    return HostPort();
  }
  return HostPortFromPB(status_->bound_http_addresses(0));
}

const NodeInstancePB& ExternalDaemon::instance_id() const {
  CHECK(status_);
  return status_->node_instance();
}

const string& ExternalDaemon::uuid() const {
  CHECK(status_);
  return status_->node_instance().permanent_uuid();
}

//------------------------------------------------------------
// ScopedResumeExternalDaemon
//------------------------------------------------------------

ScopedResumeExternalDaemon::ScopedResumeExternalDaemon(ExternalDaemon* daemon)
    : daemon_(CHECK_NOTNULL(daemon)) {
}

ScopedResumeExternalDaemon::~ScopedResumeExternalDaemon() {
  WARN_NOT_OK(daemon_->Resume(), "Could not resume external daemon");
}

//------------------------------------------------------------
// ExternalMaster
//------------------------------------------------------------

ExternalMaster::ExternalMaster(ExternalDaemonOptions opts)
    : ExternalDaemon(std::move(opts)),
      env_(Env::NewEnv()) {
}

ExternalMaster::~ExternalMaster() {
}

Status ExternalMaster::Start() {
  vector<string> flags { "master", "run" };
  auto common_flags = GetCommonFlags(rpc_bind_address());
  flags.insert(flags.end(), std::make_move_iterator(common_flags.begin()),
               std::make_move_iterator(common_flags.end()));
  return StartProcess(flags);
}

Status ExternalMaster::Restart() {
  // We store the addresses on shutdown so make sure we did that first.
  if (bound_rpc_.port() == 0) {
    return Status::IllegalState("Master cannot be restarted. Must call Shutdown() first.");
  }

  vector<string> flags { "master", "run" };
  auto common_flags = GetCommonFlags(bound_rpc_, bound_http_);
  flags.insert(flags.end(), std::make_move_iterator(common_flags.begin()),
               std::make_move_iterator(common_flags.end()));
  return StartProcess(flags);
}

Status ExternalMaster::WaitForCatalogManager(WaitMode wait_mode) {
  unique_ptr<MasterServiceProxy> proxy(new MasterServiceProxy(
      opts_.messenger, bound_rpc_addr(), bound_rpc_addr().host()));
  Stopwatch sw;
  sw.start();
  while (sw.elapsed().wall_seconds() < kMasterCatalogManagerTimeoutSeconds) {
    ListTablesRequestPB req;
    ListTablesResponsePB resp;
    RpcController rpc;
    Status s = proxy->ListTables(req, &resp, &rpc);
    if (s.IsRemoteError()) {
      auto* err = rpc.error_response();
      if (err && err->has_code()) {
        switch (err->code()) {
          case rpc::ErrorStatusPB::ERROR_SERVER_TOO_BUSY:
          case rpc::ErrorStatusPB::ERROR_UNAVAILABLE:
            continue;
          default:
            return s;
        }
      }
      return s;
    }
    if (s.ok()) {
      if (!resp.has_error()) {
        // This master is the leader and is up and running.
        break;
      }
      s = StatusFromPB(resp.error().status());
      if (s.IsIllegalState()) {
        if (wait_mode == DONT_WAIT_FOR_LEADERSHIP) {
          // This master is not the leader but is otherwise up and running.
          break;
        }
        DCHECK_EQ(wait_mode, WAIT_FOR_LEADERSHIP);
        // Continue to the sleep below.
      } else if (!s.IsServiceUnavailable()) {
        // Unexpected error from master.
        return s;
      }
    } else if (!s.IsTimedOut() && !s.IsNetworkError()) {
      // Unexpected error from proxy.
      return s;
    }

    // There was some kind of transient network error or the master isn't yet
    // ready. Sleep and retry.
    SleepFor(MonoDelta::FromMilliseconds(50));
  }
  if (sw.elapsed().wall_seconds() > kMasterCatalogManagerTimeoutSeconds) {
    return Status::TimedOut(
        Substitute("Timed out after $0s waiting for master ($1) startup",
                   kMasterCatalogManagerTimeoutSeconds,
                   bound_rpc_addr().ToString()));
  }
  return Status::OK();
}

vector<string> ExternalMaster::GetCommonFlags(const HostPort& rpc_bind_addr,
                                              const HostPort& http_addr) {
  vector<string> flags;
  if (!UseLargeKeys()) {
    // See the in-line comment for "--ipki_server_key_size" flag in
    // ExternalDaemon::StartProcess() method.
    flags.emplace_back("--ipki_ca_key_size=768");
    // As for the TSK keys, 512 bits is the minimum since we are using
    // SHA256 digest for token signing/verification.
    flags.emplace_back("--tsk_num_rsa_bits=512");
  }

  flags.emplace_back(Substitute("--rpc_bind_addresses=$0", rpc_bind_addr.ToString()));

  if (http_addr.Initialized()) {
    flags.emplace_back(Substitute("--webserver_interface=$0", http_addr.host()));
    flags.emplace_back(Substitute("--webserver_port=$0", http_addr.port()));
  } else {
    flags.emplace_back(Substitute("--webserver_interface=$0", rpc_bind_addr.host()));
    flags.emplace_back("--webserver_port=0");
  }

  return flags;
}

vector<string> ExternalMaster::GetMasterFlags(const ExternalDaemonOptions& opts) {
  vector<string> flags(ExternalDaemon::GetDaemonFlags(opts));
  auto common_flags = GetCommonFlags(opts.rpc_bind_address);
  flags.insert(flags.end(), std::make_move_iterator(common_flags.begin()),
               std::make_move_iterator(common_flags.end()));
  return flags;
}

//------------------------------------------------------------
// ExternalTabletServer
//------------------------------------------------------------

ExternalTabletServer::ExternalTabletServer(ExternalDaemonOptions opts,
                                           vector<HostPort> master_addrs)
    : ExternalDaemon(std::move(opts)),
      master_addrs_(std::move(master_addrs)),
      env_(Env::NewEnv()) {
  DCHECK(!master_addrs_.empty());
}

ExternalTabletServer::~ExternalTabletServer() {
}

Status ExternalTabletServer::Start() {
  vector<string> flags {
    "tserver", "run",
    Substitute("--rpc_bind_addresses=$0", rpc_bind_address().ToString()),
    Substitute("--local_ip_for_outbound_sockets=$0", rpc_bind_address().host()),
    Substitute("--webserver_interface=$0", rpc_bind_address().host()),
    "--webserver_port=0",
    Substitute("--tserver_master_addrs=$0",
               HostPort::ToCommaSeparatedString(master_addrs_)),
  };
  return StartProcess(flags);
}

Status ExternalTabletServer::Restart() {
  // We store the addresses on shutdown so make sure we did that first.
  if (bound_rpc_.port() == 0) {
    return Status::IllegalState("Tablet server cannot be restarted. Must call Shutdown() first.");
  }
  vector<string> flags {
    "tserver", "run",
    Substitute("--rpc_bind_addresses=$0", bound_rpc_.ToString()),
    Substitute("--local_ip_for_outbound_sockets=$0", rpc_bind_address().host()),
    Substitute("--tserver_master_addrs=$0",
               HostPort::ToCommaSeparatedString(master_addrs_)),
  };
  if (bound_http_.Initialized()) {
    flags.push_back(Substitute("--webserver_port=$0", bound_http_.port()));
    flags.push_back(Substitute("--webserver_interface=$0",
                               bound_http_.host()));
  }
  return StartProcess(flags);
}

} // namespace cluster
} // namespace kudu
