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

#include "kudu/ranger-kms/mini_ranger_kms.h"

#include <csignal>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/util/stopwatch.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/net/net_util.h"
#include "kudu/ranger-kms/mini_ranger_kms_configs.h"

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace ranger_kms {

Status MiniRangerKMS::Start() {
  RETURN_NOT_OK_PREPEND(mini_ranger_.Start(), "Failed to start Ranger");
  return StartRangerKMS(mini_ranger_.mini_pg());
}

MiniRangerKMS::~MiniRangerKMS() {
  WARN_NOT_OK(Stop(), "Failed to stop Ranger KMS");
}

Status MiniRangerKMS::Stop() {
  if (process_) {
    LOG(INFO) << "Stopping Ranger KMS...";
    RETURN_NOT_OK(process_->KillAndWait(SIGTERM));
    LOG(INFO) << "Stopped Ranger KMS";
    process_.reset();
  }
  return mini_ranger_.Stop();
}

Status MiniRangerKMS::InitRangerKMS(std::string kms_home, bool *fresh_install) {
  if (env_->FileExists(kms_home)) {
    *fresh_install = false;
    return Status::OK();
  }
  *fresh_install = true;

  RETURN_NOT_OK(env_->CreateDir(kms_home));

  RETURN_NOT_OK(mini_pg_.AddUser("minirangerkms", /*super=*/false));
  LOG(INFO) << "Created minirangerkms Postgres user";

  RETURN_NOT_OK(mini_pg_.CreateDb("rangerkms", "minirangerkms"));
  LOG(INFO) << "Created rangerkms Postrgres database";

  RETURN_NOT_OK(mini_ranger_.Start());
  LOG(INFO) << "Started Ranger Admin";

  return Status::OK();
}

Status MiniRangerKMS::CreateConfigs() {

  if (port_ == 0) {
      RETURN_NOT_OK(GetRandomPort(host_, &port_));
  }

  string kms_home = ranger_kms_home();

  ranger_kms_url_ = Substitute("http://$0:$1", host_, port_);
  HostPort ranger_admin_url_;
  ranger_admin_url_.ParseString(mini_ranger_.admin_url(), 6080);
  RETURN_NOT_OK(WriteStringToFile(
    env_, GetRangerKMSInstallProperties(
            bin_dir(),
            host_,
            mini_pg_.bound_port(),
            ranger_admin_url_.host(),
            ranger_admin_url_.port()),
    JoinPathSegments(kms_home, "install.properties")));

  RETURN_NOT_OK(WriteStringToFile(
          env_, GetRangerKMSSiteXml(host_, port_),
          JoinPathSegments(kms_home, "ranger-kms-site.xml")));

  RETURN_NOT_OK(WriteStringToFile(
          env_, GetRangerKMSDbksSiteXml(host_, mini_pg_.bound_port(), "postgresql.jar"),
          JoinPathSegments(kms_home, "dbks-site.xml")));


  return Status::OK();
}

Status MiniRangerKMS::SetupKeystores() {

}

Status MiniRangerKMS::DbSetup(const std::string &kms_home, const std::string &ews_dir,
                              const std::string &web_app_dir) {
  RETURN_NOT_OK(env_->CreateDir(ews_dir));
  RETURN_NOT_OK(env_->CreateSymLink(JoinPathSegments(ranger_kms_home_, "ews/webapp").c_str(),
                                    web_app_dir.c_str()));

  Subprocess db_setup(
          { "python", JoinPathSegments(ranger_kms_home_, "db_setup.py")});
}

Status MiniRangerKMS::StartRangerKMS(postgres::MiniPostgres* mini_pg) {
  bool fresh_install;
  LOG_TIMING(INFO, "starting Ranger KMS") {
    LOG(INFO) << "Starting Ranger KMS...";
    string exe;
    RETURN_NOT_OK(env_->GetExecutablePath(&exe));
    const string bin_dir = DirName(exe);
    RETURN_NOT_OK(FindHomeDir("hadoop", bin_dir, &hadoop_home_));
    RETURN_NOT_OK(FindHomeDir("ranger_kms", bin_dir, &ranger_kms_home_));
    RETURN_NOT_OK(FindHomeDir("java", bin_dir, &java_home_));

    const string kKMSHome = ranger_kms_home();
    const string kEwsDir = JoinPathSegments(kKMSHome, "ews"); // kms_home/ews
    const string kWebAppDir = JoinPathSegments(kEwsDir, "webapp"); // kms_home/ews/webapp
    const string kWebInfDir = JoinPathSegments(kWebAppDir, "WEB-INF"); // kms_home/ews/webapp/WEB-INF
    const string kClassesDir = JoinPathSegments(kWebInfDir, "classes"); // kms_home/ews/webapp/WEB-INF/classes
    const string kConfDir = JoinPathSegments(kClassesDir, "conf"); // kms_home/ews/webapp/WEB-INF/classes/conf

    RETURN_NOT_OK(InitRangerKMS(kKMSHome, &fresh_install));

    LOG(INFO) << "Starting Ranger KMS out of " << kKMSHome;
    LOG(INFO) << "Using postgres at " << mini_pg->host() << ":" << mini_pg->bound_port();

    RETURN_NOT_OK(CreateConfigs());

    if (fresh_install) {
        RETURN_NOT_OK(DbSetup(kKMSHome, kEwsDir, kWebAppDir));
    }

  }
}

} // namespace ranger_kms
} // namespace kudu