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

#include <sys/types.h>

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
//#include "kudu/ranger-kms/mini_ranger_kms.h"

namespace kudu {

class Env;
class NodeInstancePB;
class Sockaddr;
class Subprocess;

namespace client {
class KuduClient;
class KuduClientBuilder;
} // namespace client

#if !defined(NO_CHRONY)
namespace clock {
class MiniChronyd;
} // namespace clock
#endif

namespace hms {
class MiniHms;
} // namespace hms

namespace master {
class MasterServiceProxy;
} // namespace master

namespace rpc {
class Messenger;
} // namespace rpc

namespace postgres {
class MiniPostgres;
} // namespace postgres

namespace ranger {
class MiniRanger;
} // namespace ranger

namespace server {
class ServerStatusPB;
} // namespace server

namespace tserver {
class TabletServerAdminServiceProxy;
class TabletServerServiceProxy;
} // namespace tserver

namespace cluster {

class ExternalDaemon;
class ExternalMaster;
class ExternalTabletServer;

// Location --> number of tablet servers in location.
typedef std::map<std::string, int> LocationInfo;


struct TabletIdAndTableName {
  const std::string tablet_id;
  const std::string table_name;
};

#if !defined(NO_CHRONY)
// The enumeration below describes the way Kudu's built-in NTP client is
// configured given the set of dedicated NTP servers run by the mini-cluster.
enum class BuiltinNtpConfigMode {
  // Each server (master/tserver) uses all available NTP servers for its
  // built-in NTP client. For example, given 3 tservers and 2 NTP servers:
  //
  // tserver index | NTP server indices
  // ----------------------------------
  //   0           |  0 1
  //   1           |  0 1
  //   2           |  0 1
  ALL_SERVERS,

  // Each server (master/tserver) uses a single NTP server. The assignment runs
  // in a round-robin manner. For example, given 5 tservers and 2 NTP servers:
  //
  // tserver index | NTP server indices
  // ----------------------------------
  //   0           |  0
  //   1           |  1
  //   2           |  0
  //   3           |  1
  //   4           |  0
  //
  ROUND_ROBIN_SINGLE_SERVER,
};
#endif

struct ExternalMiniClusterOptions {
  ExternalMiniClusterOptions();

  // Number of masters to start.
  //
  // Default: 1.
  int num_masters;

  // Whether to supply 'master_addresses' field for single master configuration.
  // Default: True
  bool supply_single_master_addr;

  // Number of TS to start.
  //
  // Default: 1.
  int num_tablet_servers;

  // Directory in which to store the cluster's data.
  //
  // Default: "", which auto-generates a unique path for this cluster.
  std::string cluster_root;

  // Block manager type. Must be either "file" or "log".
  //
  // Default: "", which uses the current value of FLAGS_block_manager.
  std::string block_manager_type;

  BindMode bind_mode;

  // The path where the kudu daemons should be run from.
  //
  // Default: "", which uses the same path as the currently running executable.
  // This works for unit tests, since they all end up in build/latest/bin.
  std::string daemon_bin_path;

  // Number of data directories to be created for each daemon.
  //
  // Default: 1.
  int num_data_dirs;

  // Extra flags for tablet servers and masters respectively.
  //
  // In these flags, you may use the special string '${index}' which will
  // be substituted with the index of the tablet server or master.
  std::vector<std::string> extra_tserver_flags;
  std::vector<std::string> extra_master_flags;

  // List of RPC bind addresses to use for masters.
  //
  // If unset, addresses are assigned automatically.
  std::vector<HostPort> master_rpc_addresses;

  // Options to configure the MiniKdc before starting it up.
  // Only used when 'enable_kerberos' is 'true'.
  MiniKdcOptions mini_kdc_options;

  // If true, set up a KDC as part of this ExternalMiniCluster, generate keytabs for
  // the servers, and require Kerberos authentication from clients.
  //
  // Additionally, when the cluster is started, the environment of the
  // test process will be modified to include Kerberos credentials for
  // a principal named 'testuser'.
  //
  // Default: false.
  bool enable_kerberos;

  // Service principal name for the servers.
  //
  // Default: "kudu".
  std::string principal;

  // Tri state mode flag that indicates whether to set up a Hive Metastore as
  // part of this ExternalMiniCluster and enable Kudu Hive Metastore integration.
  //
  // Default: HmsMode::NONE.
  HmsMode hms_mode;

  // If true, set up a Ranger service as part of this ExternalMiniCluster.
  //
  // Default: false.
  bool enable_ranger;

  // If true, set up a Ranger KMS service as port of this ExternalMiniCluster.
  //
  // Default: false
  bool enable_ranger_kms;

  // If true, enable data at rest encryption.
  //
  // Default: false.
  bool enable_encryption;

  // If true, sends logging output to stderr instead of a log file.
  //
  // Default: true.
  bool logtostderr;

  // Amount of time that may elapse between the creation of a daemon process
  // and the process writing out its info file.
  //
  // Default: 70s (just a bit more than --ntp_initial_wait_secs).
  MonoDelta start_process_timeout;

  // Parameter for the cluster's RPC messenger: timeout interval after which
  // an incomplete connection negotiation will timeout.
  //
  // Default: 3 seconds.
  MonoDelta rpc_negotiation_timeout;

  // Parameter to specify the layout of tablet servers across cluster locations
  // in form of pairs { location, num_tablet_servers }. The empty container
  // means no locations are configured for the cluster.
  //
  // Default: empty
  LocationInfo location_info;

#if !defined(NO_CHRONY)
  // Number of NTP servers to start as part of the cluster. The NTP servers are
  // used as true time references for the NTP client built into masters and
  // tablet servers. Specifying a value greater than 0 automatically enables
  // the built-in NTP client, i.e. switches the clock source from the system
  // wallclock to the wallclock tracked by the built-in NTP client.
  //
  // Default: 0
  int num_ntp_servers;

  // Mapping of NTP servers to built-in NTP client for each Kudu server.
  // This parameter is effective iff num_ntp_servers > 0.
  //
  // Default: BuiltinNtpConfigMode::ALL_SERVERS
  BuiltinNtpConfigMode ntp_config_mode;
#endif // #if !defined(NO_CHRONY) ...

  std::string master_alias_prefix;
  std::string tserver_alias_prefix;
};

// A mini-cluster made up of subprocesses running each of the daemons
// separately. This is useful for black-box or grey-box failure testing
// purposes -- it provides the ability to forcibly kill or stop particular
// cluster participants, which isn't feasible in the normal InternalMiniCluster.
// On the other hand, there is little access to inspect the internal state
// of the daemons.
class ExternalMiniCluster : public MiniCluster {
 public:
  // Constructs a cluster with the default options.
  ExternalMiniCluster();

  // Constructs a cluster with options specified in 'opts'.
  explicit ExternalMiniCluster(ExternalMiniClusterOptions opts);

  // Destroys a cluster.
  virtual ~ExternalMiniCluster();

  // Start the cluster.
  Status Start() override;

  // Restarts the cluster. Requires that it has been Shutdown() first.
  Status Restart();

  // Add a new TS to the cluster. The new TS is started.
  // Requires that the master is already running.
  Status AddTabletServer();

  // Add a new master to the cluster. The new master is started.
  Status AddMaster(const std::vector<std::string>& extra_flags = {});

#if !defined(NO_CHRONY)
  // Add a new NTP server to the cluster. The new NTP server is started upon
  // adding, bind to the address and port specified by 'addr'.
  Status AddNtpServer(const Sockaddr& addr);
#endif

  // Currently, this uses SIGKILL on each daemon for a non-graceful shutdown.
  void ShutdownNodes(ClusterNodes nodes) override;

  // Return the IP address that the tablet server with the given index will bind to.
  // If options.bind_to_unique_loopback_addresses is false, this will be 127.0.0.1
  // Otherwise, it is another IP in the local netblock.
  std::string GetBindIpForTabletServer(int index) const;

  // Same as above but for a master.
  std::string GetBindIpForMaster(int index) const;

  // Same as above but for a external server, e.g. Ranger service or Hive Metastore.
  std::string GetBindIpForExternalServer(int index) const;

  // Return a pointer to the running leader master. This may be NULL
  // if the cluster is not started.
  //
  // TODO(unknown): Use the appropriate RPC here to return the leader master,
  // to allow some of the existing tests (e.g., raft_consensus-itest)
  // to use multiple masters.
  ExternalMaster* leader_master() { return master(0); }

  // Perform an RPC to determine the leader of the external mini
  // cluster.  Set 'index' to the leader master's index (for calls to
  // to master() below).
  //
  // NOTE: if a leader election occurs after this method is executed,
  // the last result may not be valid.
  Status GetLeaderMasterIndex(int* idx);

  // If this cluster is configured for a single non-distributed
  // master, return the single master or NULL if the master is not
  // started. Exits with a CHECK failure if there are multiple
  // masters.
  ExternalMaster* master() const {
    CHECK_EQ(masters_.size(), 1)
        << "master() should not be used with multiple masters, use leader_master() instead.";
    return master(0);
  }

  // Return master at 'idx' or NULL if the master at 'idx' has not
  // been started.
  ExternalMaster* master(int idx) const {
    CHECK_LT(idx, masters_.size());
    return masters_[idx].get();
  }

  ExternalTabletServer* tablet_server(int idx) const {
    CHECK_LT(idx, tablet_servers_.size());
    return tablet_servers_[idx].get();
  }

  // Return ExternalTabletServer given its UUID. If not found, returns NULL.
  ExternalTabletServer* tablet_server_by_uuid(const std::string& uuid) const;

  // Return the index of the tablet server that has the given 'uuid', or
  // -1 if no such UUID can be found.
  int tablet_server_index_by_uuid(const std::string& uuid) const;

  // Return all tablet servers and masters.
  std::vector<ExternalDaemon*> daemons() const;

  // Return all configured NTP servers used for the synchronisation of the
  // built-in NTP client.
#if !defined(NO_CHRONY)
  std::vector<clock::MiniChronyd*> ntp_servers() const;
#endif

  MiniKdc* kdc() const {
    return kdc_.get();
  }

  hms::MiniHms* hms() const {
    return hms_.get();
  }

  postgres::MiniPostgres* postgres() const {
    return postgres_.get();
  }

  ranger::MiniRanger* ranger() const {
    return ranger_.get();
  }

//  ranger_kms::MiniRangerKMS* ranger_kms() const {
//    return ranger_kms_.get();
//  }

  const std::string& cluster_root() const {
    return opts_.cluster_root;
  }

  // Kerberos principal prefix name whose credentials are used to run Kudu
  // servers in the cluster. Matches the SASL protocol name used for connection
  // negotiation.
  const std::string& service_principal() const {
    return opts_.principal;
  }

  int num_tablet_servers() const override {
    return tablet_servers_.size();
  }

  int num_masters() const override {
    return masters_.size();
  }

  // Returns the WALs root directory for the tablet server 'ts_idx'.
  virtual std::string WalRootForTS(int ts_idx) const override;

  // Returns the UUID for the tablet server 'ts_idx'.
  virtual std::string UuidForTS(int ts_idx) const override;

  // Returns the Env on which the cluster operates.
  virtual Env* env() const override;

  BindMode bind_mode() const override {
    return opts_.bind_mode;
  }

  std::vector<HostPort> master_rpc_addrs() const override;

  std::shared_ptr<rpc::Messenger> messenger() const override;
  std::shared_ptr<master::MasterServiceProxy> master_proxy() const override;
  std::shared_ptr<master::MasterServiceProxy> master_proxy(int idx) const override;
  std::shared_ptr<tserver::TabletServerServiceProxy> tserver_proxy(int idx) const override;
  std::shared_ptr<tserver::TabletServerAdminServiceProxy> tserver_admin_proxy(
      int idx) const override;

  std::string block_manager_type() const {
    return opts_.block_manager_type;
  }

  // Wait until the number of registered tablet servers reaches the given count
  // on running masters. Returns Status::TimedOut if the desired count is not
  // achieved or if the masters cannot be reached within the given timeout. If
  // 'master_idx' is specified, only examines the given master if it's running.
  // Otherwise, checks all running masters.
  Status WaitForTabletServerCount(int count, const MonoDelta& timeout,
                                  int master_idx = -1);

  // Runs gtest assertions that no servers have crashed.
  void AssertNoCrashes();

  // Wait until all tablets on the given tablet server are in the RUNNING
  // state. Returns Status::TimedOut if 'timeout' elapses and at least one
  // tablet is not yet RUNNING.
  //
  // If 'min_tablet_count' is not -1, will also wait for at least that many
  // RUNNING tablets to appear before returning (potentially timing out if that
  // number is never reached).
  Status WaitForTabletsRunning(
      ExternalTabletServer* ts,
      int min_tablet_count,
      const MonoDelta& timeout,
      std::vector<TabletIdAndTableName>* tablets_info = nullptr);

  // Create a client configured to talk to this cluster.
  // Builder may contain override options for the client. The master address will
  // be overridden to talk to the running master.
  //
  // REQUIRES: the cluster must have already been Start()ed.
  Status CreateClient(client::KuduClientBuilder* builder,
                      client::sp::shared_ptr<client::KuduClient>* client) const override;

  // Sets the given flag on the given daemon, which must be running.
  //
  // This uses the 'force' flag on the RPC so that, even if the flag
  // is considered unsafe to change at runtime, it is changed.
  Status SetFlag(ExternalDaemon* daemon,
                 const std::string& flag,
                 const std::string& value) WARN_UNUSED_RESULT;

  // Enable Hive Metastore integration.
  // Overrides HMS integration options set by ExternalMiniClusterOptions.
  // The cluster must be shut down before calling this method.
  void EnableMetastoreIntegration();

  // Disable Hive Metastore integration.
  // Overrides HMS integration options set by ExternalMiniClusterOptions.
  // The cluster must be shut down before calling this method.
  void DisableMetastoreIntegration();

  // Set the path where daemon binaries can be found.
  // Overrides 'daemon_bin_path' set by ExternalMiniClusterOptions.
  // The cluster must be shut down before calling this method.
  void SetDaemonBinPath(std::string daemon_bin_path);

  // Returns the path where 'binary' is expected to live, based on
  // ExternalMiniClusterOptions.daemon_bin_path if it was provided, or on the
  // path of the currently running executable otherwise.
  std::string GetBinaryPath(const std::string& binary) const;

  // Returns the path where 'daemon_id' is expected to store its data, based on
  // ExternalMiniClusterOptions.cluster_root if it was provided, or on the
  // standard Kudu test directory otherwise.
  // 'dir_index' is an optional numeric suffix to be added to the default path.
  // If it is not specified, the cluster must be configured to use a single data dir.
  std::string GetDataPath(const std::string& daemon_id,
                          boost::optional<uint32_t> dir_index = boost::none) const;

  // Returns paths where 'daemon_id' is expected to store its data, each with a
  // numeric suffix appropriate for 'opts_.num_data_dirs'
  std::vector<std::string> GetDataPaths(const std::string& daemon_id) const;

  // Returns the path where 'daemon_id' is expected to store its wal, or other
  // files that reside in the wal dir.
  std::string GetWalPath(const std::string& daemon_id) const;

  // Returns the path where 'daemon_id' is expected to store its logs, or other
  // files that reside in the log dir.
  std::string GetLogPath(const std::string& daemon_id) const;

  // Removes any bookkeeping of the master specified by 'hp' from the ExternalMiniCluster
  // after already having run through a successful master Raft change config to remove it.
  // This helps keep the state of the actual cluster in sync with the state in ExternalMiniCluster.
  Status RemoveMaster(const HostPort& hp);

  const std::string& dns_overrides() const {
    return dns_overrides_;
  }
  // Constructs an ExternalMaster based on 'opts_' but with the given set of
  // master addresses, giving the new master the address in the list
  // corresponding to 'idx'. Callers are expected to call Start() with the
  // output 'master'.
  //
  // It's expected that the port for the master at 'idx' is reserved, and that
  // the master can be run with the --rpc_reuseport flag.
  Status CreateMaster(const std::vector<HostPort>& master_rpc_addrs, int idx,
                      scoped_refptr<ExternalMaster>* master);

 private:
  Status StartMasters();

  Status DeduceBinRoot(std::string* ret);
  Status HandleOptions();

  // Add flags related to time source and clock for the daemon at index 'idx'.
  // The time source flags are appended to the 'flags' in-out parameter.
  Status AddTimeSourceFlags(int idx, std::vector<std::string>* flags);

  ExternalMiniClusterOptions opts_;

  std::vector<scoped_refptr<ExternalMaster>> masters_;
  std::vector<scoped_refptr<ExternalTabletServer>> tablet_servers_;
#if !defined(NO_CHRONY)
  std::vector<std::unique_ptr<clock::MiniChronyd>> ntp_servers_;
#endif
  std::unique_ptr<MiniKdc> kdc_;
  std::unique_ptr<hms::MiniHms> hms_;
  std::shared_ptr<postgres::MiniPostgres> postgres_;
  std::unique_ptr<ranger::MiniRanger> ranger_;
//  std::unique_ptr<ranger_kms::MiniRangerKMS> ranger_kms_;

  std::shared_ptr<rpc::Messenger> messenger_;

  std::string dns_overrides_;

  DISALLOW_COPY_AND_ASSIGN(ExternalMiniCluster);
};

struct ExternalDaemonOptions {
  ExternalDaemonOptions()
      : logtostderr(false),
        enable_encryption(false) {
  }

  bool logtostderr;
  bool enable_encryption;
  std::shared_ptr<rpc::Messenger> messenger;
  std::string block_manager_type;
  std::string exe;
  HostPort rpc_bind_address;
  std::string wal_dir;
  std::vector<std::string> data_dirs;
  std::string log_dir;
  std::string perf_record_filename;
  std::vector<std::string> extra_flags;
  MonoDelta start_process_timeout;
};

class ExternalDaemon : public RefCountedThreadSafe<ExternalDaemon> {
 public:
  explicit ExternalDaemon(ExternalDaemonOptions opts);

  HostPort bound_rpc_hostport() const;
  Sockaddr bound_rpc_addr() const;

  // Return the host/port that this daemon is bound to for HTTP.
  // May return an uninitialized HostPort if HTTP is disabled.
  HostPort bound_http_hostport() const;

  const NodeInstancePB& instance_id() const;
  const std::string& uuid() const;

  // Return the pid of the running process.
  // Causes a CHECK failure if the process is not running.
  pid_t pid() const;

  // Return the pointer to the undelying Subprocess if it is set.
  // Otherwise, returns nullptr.
  Subprocess* process() const;

  // Set the path of the executable to run as a daemon.
  // Overrides the exe path specified in the constructor.
  // The daemon must be shut down before calling this method.
  void SetExePath(std::string exe);

  // Set '--hive_metastore_uris' and '--hive_metastore_sasl_enabled'
  // to enable Hive Metastore integration.
  // Overrides the extra flags specified in the constructor.
  void SetMetastoreIntegration(const std::string& hms_uris,
                               bool enable_kerberos);

  // Create a Kerberos principal and keytab using the 'principal_base' and 'bind_host' hostname
  // of the form <principal_base>/<bind_host>.
  // Returns the generated keytab file and service principal as 'flags' and the appropriate
  // environment variables in 'env_vars' output parameters.
  static Status CreateKerberosConfig(MiniKdc* kdc, const std::string& principal_base,
                                     const std::string& bind_host,
                                     std::vector<std::string>* flags,
                                     std::map<std::string, std::string>* env_vars);

  // Enable Kerberos for this daemon. This creates a Kerberos principal
  // and keytab, and sets the appropriate environment variables in the
  // subprocess such that the server will use Kerberos authentication.
  //
  // 'bind_host' is the hostname that will be used to generate the Kerberos
  // service principal, 'principal_base' will be the first part (i.e.
  // <principal_base>/<bind_host>).
  //
  // Must be called before 'StartProcess()'.
  Status EnableKerberos(MiniKdc* kdc,
                        const std::string& principal_base,
                        const std::string& bind_host);

  // Sends a SIGSTOP signal to the daemon.
  Status Pause() WARN_UNUSED_RESULT;

  // Sends a SIGCONT signal to the daemon.
  Status Resume() WARN_UNUSED_RESULT;

  // Return true if we have explicitly shut down the process.
  bool IsShutdown() const;

  // Return true if the process is still running.
  // This may return false if the process crashed, even if we didn't
  // explicitly call Shutdown().
  bool IsProcessAlive() const;

  // Wait for this process to crash due to a configured fault
  // injection, or the given timeout to elapse. If the process
  // crashes for some reason other than an injected fault, returns
  // Status::Aborted.
  //
  // If the process is already crashed, returns immediately.
  Status WaitForInjectedCrash(const MonoDelta& timeout) const;

  // Same as the above, but expects the process to crash due to a
  // LOG(FATAL) or CHECK failure. In other words, waits for it to
  // crash from SIGABRT.
  Status WaitForFatal(const MonoDelta& timeout) const;

  virtual Status Start() = 0;
  virtual Status Restart() = 0;
  virtual void Shutdown();

  // Delete files specified by 'wal_dir_' and 'data_dirs_'.
  Status DeleteFromDisk() const WARN_UNUSED_RESULT;

  const std::string& wal_dir() const { return opts_.wal_dir; }

  const std::string& data_dir() const {
    CHECK_EQ(1, opts_.data_dirs.size());
    return opts_.data_dirs[0];
  }

  const std::vector<std::string>& data_dirs() const { return opts_.data_dirs; }

  // Returns the log dir of the external daemon.
  const std::string& log_dir() const { return opts_.log_dir; }

  // Return a pointer to the flags used for this server on restart.
  // Modifying these flags will only take effect on the next restart.
  std::vector<std::string>* mutable_flags() { return &opts_.extra_flags; }

  // Return the options used to create the daemon.
  ExternalDaemonOptions opts() const { return opts_; }

  void SetRpcBindAddress(HostPort rpc_hostport) {
    DCHECK(!IsProcessAlive());
    bound_rpc_ = std::move(rpc_hostport);
    opts_.rpc_bind_address = bound_rpc_;
  }

 protected:
  friend class RefCountedThreadSafe<ExternalDaemon>;
  virtual ~ExternalDaemon();

  // Starts a process with the given flags.
  Status StartProcess(const std::vector<std::string>& user_flags);

  // Wait for the process to exit, and then call 'wait_status_predicate'
  // on the resulting exit status. NOTE: this is not the return code, but
  // rather the value provided by waitpid(2): use WEXITSTATUS, etc.
  //
  // If the predicate matches, returns OK. Otherwise, returns an error.
  // 'crash_type_str' should be a descriptive name for the type of crash,
  // used in formatting the error message.
  Status WaitForCrash(const MonoDelta& timeout,
                      const std::function<bool(int)>& wait_status_predicate,
                      const char* crash_type_str) const;

  // In a code-coverage build, try to flush the coverage data to disk.
  // In a non-coverage build, this does nothing.
  void FlushCoverage();

  // In an LSAN build, ask the daemon to check for leaked memory, and
  // LOG(FATAL) if there are any leaks.
  void CheckForLeaks();

  // Get flags for an ExternalDaemon from the supplied 'opts'.
  // It does not include executable or any positional arguments.
  static std::vector<std::string> GetDaemonFlags(const ExternalDaemonOptions& opts);

  // Get RPC bind address for daemon.
  const HostPort& rpc_bind_address() const {
    return opts_.rpc_bind_address;
  }

  ExternalDaemonOptions opts_;
  std::map<std::string, std::string> extra_env_;

  std::unique_ptr<Subprocess> process_;
  bool paused_ = false;

  std::unique_ptr<Subprocess> perf_record_process_;

  std::unique_ptr<server::ServerStatusPB> status_;

  // These capture the daemons parameters and running ports and
  // are used to Restart() the daemon with the same parameters.
  HostPort bound_rpc_;
  HostPort bound_http_;

  // ID of the thread that is spawning the child processes. This should not
  // change across restarts of the daemon, as forking from different threads
  // may yield behavior like daemons being killed when the new thread exits.
  const std::thread::id parent_tid_;

  DISALLOW_COPY_AND_ASSIGN(ExternalDaemon);
};

// Resumes a daemon that was stopped with ExternalDaemon::Pause() upon
// exiting a scope.
class ScopedResumeExternalDaemon {
 public:
  // 'daemon' must remain valid for the lifetime of a
  // ScopedResumeExternalDaemon object.
  explicit ScopedResumeExternalDaemon(ExternalDaemon* daemon);

  // Resume 'daemon_'.
  ~ScopedResumeExternalDaemon();

 private:
  ExternalDaemon* daemon_;

  DISALLOW_COPY_AND_ASSIGN(ScopedResumeExternalDaemon);
};

class ExternalMaster : public ExternalDaemon {
 public:
  explicit ExternalMaster(ExternalDaemonOptions opts);

  virtual Status Start() override;

  // Restarts the daemon.
  // Requires that it has previously been shutdown.
  virtual Status Restart() override WARN_UNUSED_RESULT;

  // Blocks until the master's catalog manager is initialized and responding to
  // RPCs. If 'wait_mode' is WAIT_FOR_LEADERSHIP, will further block until the
  // master has been elected leader.
  //
  // WAIT_FOR_LEADERSHIP should only be used in single-master environments; the
  // wait may time out in a multi-master environment as there's no guarantee
  // that this particular master will be elected leader.
  enum WaitMode {
    WAIT_FOR_LEADERSHIP,
    DONT_WAIT_FOR_LEADERSHIP
  };
  Status WaitForCatalogManager(
      WaitMode wait_mode = DONT_WAIT_FOR_LEADERSHIP) WARN_UNUSED_RESULT;

  // Get all flags for a master from the supplied 'opts'.
  // It does not include executable or any positional arguments like "master run".
  static std::vector<std::string> GetMasterFlags(const ExternalDaemonOptions& opts);
 private:
  friend class RefCountedThreadSafe<ExternalMaster>;
  // Get flags specific to ExternalMaster where 'rpc_bind_addr' and 'http_addr' are
  // the RPC and HTTP addresses to bind in case of start or the corresponding bound
  // addresses in case of restart.
  static std::vector<std::string> GetCommonFlags(const HostPort& rpc_bind_addr,
                                                 const HostPort& http_addr = HostPort());
  virtual ~ExternalMaster();
};

class ExternalTabletServer : public ExternalDaemon {
 public:
  ExternalTabletServer(ExternalDaemonOptions opts,
                       std::vector<HostPort> master_addrs);

  virtual Status Start() override;

  // Restarts the daemon.
  // Requires that it has previously been shutdown.
  virtual Status Restart() override WARN_UNUSED_RESULT;

 private:
  const std::vector<HostPort> master_addrs_;

  friend class RefCountedThreadSafe<ExternalTabletServer>;
  virtual ~ExternalTabletServer();
};

} // namespace cluster
} // namespace kudu
