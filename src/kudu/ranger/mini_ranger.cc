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

#include "kudu/ranger/mini_ranger.h"

#include <csignal>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/postgres/mini_postgres.h"
#include "kudu/ranger/mini_ranger_configs.h"
#include "kudu/ranger/ranger.pb.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/easy_json.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;
using strings::Substitute;

static constexpr int kRangerStartTimeoutMs = 90000;

namespace kudu {
namespace ranger {

Status MiniRanger::Start() {
  RETURN_NOT_OK_PREPEND(mini_pg_->Start(), "Failed to start Postgres");
  return StartRanger();
}

MiniRanger::~MiniRanger() {
  WARN_NOT_OK(Stop(), "Failed to stop Ranger");
}

Status MiniRanger::Stop() {
  if (process_) {
    LOG(INFO) << "Stopping Ranger...";
    RETURN_NOT_OK(process_->KillAndWait(SIGTERM));
    LOG(INFO) << "Stopped Ranger";
    process_.reset();
  }
  return mini_pg_->Stop();
}

Status MiniRanger::InitRanger(string admin_home, bool* fresh_install) {
  if (env_->FileExists(admin_home)) {
    *fresh_install = false;
    return Status::OK();
  }
  *fresh_install = true;

  RETURN_NOT_OK(env_->CreateDir(admin_home));

  RETURN_NOT_OK(mini_pg_->AddUser("miniranger", /*super=*/ false));
  LOG(INFO) << "Created miniranger Postgres user";

  RETURN_NOT_OK(mini_pg_->CreateDb("ranger", "miniranger"));
  LOG(INFO) << "Created ranger Postgres database";

  return Status::OK();
}

Status MiniRanger::CreateConfigs() {
  // Ranger listens on 2 ports:
  //
  // - port_ is the RPC port (REST API) that the Ranger subprocess and
  //   EasyCurl can talk to
  // - ranger_shutdown_port is the port which Ranger listens on for a shutdown
  //   command. We're not using this shutdown port as we simply send a SIGTERM,
  //   but it's necessary to set it to a random value to avoid collisions in
  //   parallel testing.
  if (port_ == 0) {
    RETURN_NOT_OK(GetRandomPort(host_, &port_));
  }
  uint16_t ranger_shutdown_port;
  RETURN_NOT_OK(GetRandomPort(host_, &ranger_shutdown_port));
  string admin_home = ranger_admin_home();

  ranger_admin_url_ = Substitute("http://$0:$1", host_, port_);

  // Write config files
  RETURN_NOT_OK(WriteStringToFile(
      env_, GetRangerInstallProperties(bin_dir(), host_, mini_pg_->bound_port()),
      JoinPathSegments(admin_home, "install.properties")));

  RETURN_NOT_OK(WriteStringToFile(
      env_, GetRangerAdminSiteXml(host_, port_, host_, mini_pg_->bound_port(),
                                  admin_ktpath_, lookup_ktpath_,
                                  spnego_ktpath_),
      JoinPathSegments(admin_home, "ranger-admin-site.xml")));

  RETURN_NOT_OK(WriteStringToFile(
      env_, GetRangerAdminDefaultSiteXml(
        JoinPathSegments(bin_dir(), "postgresql.jar"),
        ranger_shutdown_port),
      JoinPathSegments(admin_home, "ranger-admin-default-site.xml")));

  RETURN_NOT_OK(WriteStringToFile(env_, GetRangerCoreSiteXml(kerberos_),
                                  JoinPathSegments(admin_home, "core-site.xml")));

  RETURN_NOT_OK(WriteStringToFile(env_, GetRangerLog4jProperties("info"),
                                  JoinPathSegments(admin_home, "log4j.properties")));

  return Status::OK();
}

Status MiniRanger::DbSetup(const string& admin_home, const string& ews_dir,
                           const string& web_app_dir) {
  RETURN_NOT_OK(env_->CreateDir(ews_dir));
  RETURN_NOT_OK(env_->CreateSymLink(JoinPathSegments(ranger_home_, "ews/webapp").c_str(),
                                   web_app_dir.c_str()));

  // Much of this encapsulates setup.sh from apache/ranger[1], excluding some of
  // the system-level configs.
  //
  // [1] https://github.com/apache/ranger/blob/master/security-admin/scripts/setup.sh
  //
  // TODO(abukor): load a db dump instead as this is very slow
  Subprocess db_setup(
        { "python", JoinPathSegments(ranger_home_, "db_setup.py")});
  db_setup.SetEnvVars({
      { "JAVA_HOME", java_home_ },
      { "RANGER_ADMIN_HOME", ranger_home_ },
      { "RANGER_ADMIN_CONF", admin_home },
      { "XAPOLICYMGR_DIR", admin_home },
      { "RANGER_PID_DIR_PATH", admin_home },
      });
  db_setup.SetCurrentDir(admin_home);
  RETURN_NOT_OK(db_setup.Start());
  return db_setup.WaitAndCheckExitCode();
}

Status MiniRanger::StartRanger() {
  bool fresh_install;
  if (!mini_pg_->isRunning()) {
    return Status::IllegalState("Postgres is not running");
  }

  LOG_TIMING(INFO, "starting Ranger") {
    LOG(INFO) << "Starting Ranger...";
    string exe;
    RETURN_NOT_OK(env_->GetExecutablePath(&exe));
    const string bin_dir = DirName(exe);
    RETURN_NOT_OK(FindHomeDir("hadoop", bin_dir, &hadoop_home_));
    RETURN_NOT_OK(FindHomeDir("ranger", bin_dir, &ranger_home_));
    RETURN_NOT_OK(FindHomeDir("java", bin_dir, &java_home_));

    const string kAdminHome = ranger_admin_home();
    const string kEwsDir = JoinPathSegments(kAdminHome, "ews");
    const string kWebAppDir = JoinPathSegments(kEwsDir, "webapp");
    const string kWebInfDir = JoinPathSegments(kWebAppDir, "WEB-INF");
    const string kClassesDir = JoinPathSegments(kWebInfDir, "classes");
    const string kConfDir = JoinPathSegments(kClassesDir, "conf");

    RETURN_NOT_OK(InitRanger(kAdminHome, &fresh_install));

    LOG(INFO) << "Starting Ranger out of " << kAdminHome;

    RETURN_NOT_OK(CreateConfigs());

    if (fresh_install) {
      RETURN_NOT_OK(DbSetup(kAdminHome, kEwsDir, kWebAppDir));
    }

    // Encapsulates ranger-admin-services.sh[1]
    //
    // 1. https://github.com/apache/ranger/blob/f37f5407eee8d2627a4306a25938b151f8e2ba31/embeddedwebserver/scripts/ranger-admin-services.sh
    string classpath = ranger_classpath();

    LOG(INFO) << "Using Ranger class path: " << classpath;

    LOG(INFO) << "Using host: " << host_;
    std::vector<string> args({
        JoinPathSegments(java_home_, "bin/java"),
        "-Dproc_rangeradmin",
        Substitute("-Dhostname=$0", host_),
        Substitute("-Dlog4j.configuration=file:$0",
                   JoinPathSegments(kAdminHome, "log4j.properties")),
        "-Duser=miniranger",
        Substitute("-Dranger.service.host=$0", host_),
        "-Dservername=miniranger",
        Substitute("-Dcatalina.base=$0", kEwsDir),
        Substitute("-Dlogdir=$0", JoinPathSegments(kAdminHome, "logs")),
        "-Dranger.audit.solr.bootstrap.enabled=false",
    });
    if (kerberos_) {
      args.emplace_back(Substitute("-Djava.security.krb5.conf=$0", krb5_config_));
    }
    args.emplace_back("-cp");
    args.emplace_back(classpath);
    args.emplace_back("org.apache.ranger.server.tomcat.EmbeddedServer");
    process_.reset(new Subprocess(args));
    process_->SetEnvVars({
        { "XAPOLICYMGR_DIR", kAdminHome },
        { "XAPOLICYMGR_EWS_DIR", kEwsDir },
        { "RANGER_JAAS_LIB_DIR", JoinPathSegments(kWebAppDir, "ranger_jaas") },
        { "RANGER_JAAS_CONF_DIR", JoinPathSegments(kConfDir, "ranger_jaas") },
        { "JAVA_HOME", java_home_ },
        { "RANGER_PID_DIR_PATH", JoinPathSegments(data_root_, "tmppid") },
        { "RANGER_ADMIN_PID_NAME", "rangeradmin.pid" },
        { "RANGER_ADMIN_CONF_DIR", kAdminHome },
        { "RANGER_USER", "miniranger" },
    });
    RETURN_NOT_OK(process_->Start());
    uint16_t port;
    RETURN_NOT_OK(WaitForTcpBind(process_->pid(),
                                 &port,
                                 { "0.0.0.0", "127.0.0.1", },
                                 MonoDelta::FromMilliseconds(kRangerStartTimeoutMs)));
    LOG(INFO) << "Ranger bound to " << port;
    LOG(INFO) << "Ranger admin URL: " << ranger_admin_url_;
  }
  if (fresh_install) {
    RETURN_NOT_OK(CreateKuduService());
  }

  return Status::OK();
}

Status MiniRanger::CreateKuduService() {
  EasyJson service;
  service.Set("name", "kudu");
  service.Set("type", "kudu");
  // The below config authorizes "kudu" to download the list of authorized users
  // for policies and tags respectively.
  EasyJson configs = service.Set("configs", EasyJson::kObject);
  configs.Set("policy.download.auth.users", "kudu");
  configs.Set("tag.download.auth.users", "kudu");

  RETURN_NOT_OK_PREPEND(PostToRanger("service/plugins/services", service),
                        "Failed to create Kudu service");
  LOG(INFO) << "Created Kudu service";
  return Status::OK();
}

Status MiniRanger::AddPolicy(AuthorizationPolicy policy) {
  string policy_name = JoinStrings<vector<string>>({JoinStrings(policy.databases, ","),
                                                    JoinStrings(policy.tables, ","),
                                                    JoinStrings(policy.columns, ",")}, ";");
  PolicyKey policy_key(policy.databases, policy.tables, policy.columns);
  vector<PolicyItem>* items = FindOrNull(policies_, policy_key);
  if (!items) {
    policies_.emplace(policy_key, policy.items);
    items = &policy.items;
  } else {
    for (const auto& item : policy.items) {
      items->emplace_back(item);
    }
  }

  EasyJson policy_json;
  policy_json.Set("service", "kudu");
  policy_json.Set("name", policy_name);
  policy_json.Set("isEnabled", true);

  EasyJson resources = policy_json.Set("resources", EasyJson::kObject);

  if (!policy.databases.empty()) {
    EasyJson databases = resources.Set("database", EasyJson::kObject);
    EasyJson database_values = databases.Set("values", EasyJson::kArray);
    for (const string& database : policy.databases) {
      database_values.PushBack(database);
    }
  }

  if (!policy.tables.empty()) {
    EasyJson tables = resources.Set("table", EasyJson::kObject);
    EasyJson table_values = tables.Set("values", EasyJson::kArray);
    for (const string& table : policy.tables) {
      table_values.PushBack(table);
    }
  }

  if (!policy.columns.empty()) {
    EasyJson columns = resources.Set("column", EasyJson::kArray);
    EasyJson column_values = columns.Set("values", EasyJson::kArray);
    for (const string& column : policy.columns) {
      column_values.PushBack(column);
    }
  }

  EasyJson policy_items = policy_json.Set("policyItems", EasyJson::kArray);
  for (const auto& policy_item : *items) {
    EasyJson item = policy_items.PushBack(EasyJson::kObject);

    EasyJson users = item.Set("users", EasyJson::kArray);
    for (const string& user : std::get<0>(policy_item)) {
      users.PushBack(user);
    }

    item.Set("delegateAdmin", std::get<2>(policy_item));
    EasyJson accesses = item.Set("accesses", EasyJson::kArray);
    for (const ActionPB& action : std::get<1>(policy_item)) {
      EasyJson access = accesses.PushBack(EasyJson::kObject);
      access.Set("type", ActionPB_Name(action));
      access.Set("isAllowed", true);
    }
  }

  RETURN_NOT_OK_PREPEND(PostToRanger("service/plugins/policies?deleteIfExists=true",
                                     policy_json), "Failed to add policy");
  return Status::OK();
}

Status MiniRanger::PostToRanger(string url, EasyJson payload) {
  faststring result;
  RETURN_NOT_OK_PREPEND(curl_.PostToURL(JoinPathSegments(ranger_admin_url_, std::move(url)),
                                        std::move(payload.ToString()), &result,
                                        {"Content-Type: application/json"}),
                        Substitute("Error received from Ranger: $0", result.ToString()));
  return Status::OK();
}

Status MiniRanger::CreateClientConfig(const string& client_config_dir) {
  auto policy_cache = JoinPathSegments(client_config_dir, "policy-cache");
  if (!env_->FileExists(client_config_dir)) {
    RETURN_NOT_OK(env_->CreateDir(client_config_dir));
    RETURN_NOT_OK(env_->CreateDir(policy_cache));
  }

  RETURN_NOT_OK(WriteStringToFile(env_, GetRangerCoreSiteXml(kerberos_),
                                  JoinPathSegments(client_config_dir, "core-site.xml")));
  RETURN_NOT_OK(WriteStringToFile(env_, GetRangerKuduSecurityXml(policy_cache, "kudu",
                                                                 ranger_admin_url_,
                                                                 policy_poll_interval_ms_),
                                  JoinPathSegments(client_config_dir,
                                                   "ranger-kudu-security.xml")));
  return Status::OK();
}

} // namespace ranger
} // namespace kudu
