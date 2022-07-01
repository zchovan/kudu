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

#include <gtest/gtest.h>

#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/mini-cluster/external_mini_cluster.h"

using kudu::postgres::MiniPostgres;
using kudu::ranger::MiniRanger;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;

namespace kudu {
namespace ranger_kms {

class MiniRangerKMSTest : public KuduTest {
  public:
    MiniRangerKMSTest()
      : mini_pg_(std::make_shared<MiniPostgres>("127.0.0.1")),
      mini_ranger_(std::make_shared<MiniRanger>("127.0.0.1", mini_pg_)),
      ranger_kms_("127.0.0.1", mini_pg_, mini_ranger_) {};
    void SetUp() override {
      ASSERT_OK(ranger_kms_.Start());
    }

  protected:
    std::shared_ptr<MiniPostgres> mini_pg_;
    std::shared_ptr<MiniRanger> mini_ranger_;
    MiniRangerKMS ranger_kms_;
};

TEST_F(MiniRangerKMSTest, TestKMSStart) {
  LOG(INFO) << ">>> GETTING KEYS <<<";
  Status status = ranger_kms_.GetKeys();
  KUDU_CHECK_OK(status);
}

class MiniRangerEMCTest : public KuduTest {
  public:
  void SetUp() override {
    cluster_opts_.enable_kerberos = true;
    cluster_opts_.enable_ranger = true;
    cluster_opts_.enable_ranger_kms = true;

    cluster_.reset(new ExternalMiniCluster(std::move(cluster_opts_)));
    ASSERT_OK(cluster_->Start());
  }

  protected:
    ExternalMiniClusterOptions cluster_opts_;
    std::shared_ptr<ExternalMiniCluster> cluster_;
};

TEST_F(MiniRangerEMCTest, TestKMS) {
  LOG(INFO) << ">>> GETTING KEYS <<<";
  Status status = cluster_->ranger_kms()->GetKeys();
  KUDU_CHECK_OK(status);
}


} // namespace ranger_kms
} // namespace kudu
