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

#include <string>

#include <glog/logging.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/postgres/mini_postgres.h";
#include "kudu/util/curl_util.h"
#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"
#include "kudu/ranger/mini_ranger.h"

namespace kudu {
namespace ranger_kms {

class MiniRangerKMS {
  public:
    explicit MiniRangerKMS(std::string host)
      : MiniRangerKMS(GetTestDataDirectory(), std::move(host)) {}

    ~MiniRangerKMS();

    MiniRangerKMS(std::string data_root, std::string host,
                  std::shared_ptr<postgres::MiniPostgres> mini_pg,
                  std::shared_ptr<ranger::MiniRanger> mini_ranger)
      : data_root_(std::move(data_root)),
        host_(std::move(host)),
        env_(Env::Default()) {
          curl_.set_auth(CurlAuthType::BASIC, "admin", "admin");
          mini_pg_ = mini_pg;
          mini_ranger_ = mini_ranger;
        }

    // Starts Ranger and its dependencies.
    Status Start() WARN_UNUSED_RESULT;

    // Stops Ranger and its dependencies.
    Status Stop() WARN_UNUSED_RESULT;

  private:
    // Starts RangerKMS Service
    Status StartRangerKMS(postgres::MiniPostgres* mini_pg) WARN_UNUSED_RESULT;

    // Initializes Ranger KMS within 'kms_home' (home directory of the Ranger KMS
    // admin). Sets 'fresh_install' to true if 'kms_home' didn't exist before
    // calling InitRangerKMS().
    Status InitRangerKMS(std::string kms_home, bool* fresh_install) WARN_UNUSED_RESULT;

    // Creates configuration files.
    // ref: https://docs.cloudera.com/HDPDocuments/HDP2/HDP-2.6.5/bk_security/content/ranger_kms_properties.html
    Status CreateConfigs() WARN_UNUSED_RESULT;

    // creates keystores for the KMS
    Status SetupKeystores() WARN_UNUSED_RESULT;

    // Initializes Ranger KMS' database.
    Status DbSetup(const std::string& kms_home, const std::string& ews_dir,
                   const std::string& web_app_dir) WARN_UNUSED_RESULT;

    // Returns RangerKMS' home directory.
    std::string ranger_kms_home() const {
      return JoinPathSegments(data_root_, "ranger-kms");
    }

    std::string bin_dir() const {
      std::string exe;
              CHECK_OK(env_->GetExecutablePath(&exe));
      return DirName(exe);
    }

    // Returns classpath for Ranger KMS.
    std::string ranger_kms_classpath() const {
      std::string kms_home = ranger_kms_home();
      // ${RANGER_KMS_CONF_DIR}:
      // ${SQL_CONNECTOR_JAR}:
      // ${RANGER_KMS_HOME}/ews/webapp/WEB-INF/classes/lib/:
      // ${RANGER_KMS_HOME}/ews/webapp/lib/:
      // ${JAVA_HOME}/lib/:
      // ${RANGER_KMS_HADOOP_CONF_DIR}/"
      //org.apache.ranger.server.tomcat.EmbeddedServer
      return strings::Substitute(
              "$0:$1/lib/*:$2/lib/*:$3/*:$4:$5",
              kms_home, JoinPathSegments(ranger_kms_home_, "ews"), java_home_,
              hadoop_home_, JoinPathSegments(bin_dir(), "postgresql.jar"),
              JoinPathSegments(ranger_kms_home_, "ews/webapp"));
    }
    // Directory in which to put all our stuff.
    const std::string data_root_;
    const std::string host_;

    std::shared_ptr<postgres::MiniPostgres> mini_pg_;
    std::shared_ptr<ranger::MiniRanger> mini_ranger_;
    std::unique_ptr<Subprocess> process_;

    // URL of the Ranger KMS REST API.
    std::string ranger_kms_url_;

    // Locations in which to find Hadoop, Ranger, and Java.
    // These may be in the thirdparty build, or may be shared across tests. As
    // such, their contents should be treated as read-only.
    std::string hadoop_home_;
    std::string ranger_kms_home_;
    std::string java_home_;

    Env* env_;
    EasyCurl curl_;

    uint16_t port_ = 0;
};


} // namespace ranger_kms
} // namespace kudu
