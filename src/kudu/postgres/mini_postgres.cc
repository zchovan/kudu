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

#include "kudu/postgres/mini_postgres.h"

#include <csignal>
#include <ostream>
#include <string>

#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"

using std::ifstream;
using std::string;
using std::unique_ptr;
using strings::Substitute;

static constexpr int kPgStartTimeoutMs = 60000;

namespace kudu {
namespace postgres {

MiniPostgres::~MiniPostgres() {
  if (process_ && process_->IsStarted()) {
    WARN_NOT_OK(Stop(),"Unable to stop postgres");
  }
}

Status MiniPostgres::Start() {
  if (process_) {
    return Status::IllegalState("Postgres already running");
  }

  VLOG(1) << "Starting Postgres";
  string pgr = pg_root();
  if (firstRun()) {
    // This is our first time running. Set up our directories, config files,
    // and port.
    LOG(INFO) << "Running initdb...";
    Subprocess initdb({
      JoinPathSegments(bin_dir_, "postgres/initdb"),
      "-D", pgr, "-L", JoinPathSegments(bin_dir_, "postgres-share")
    });
    RETURN_NOT_OK_PREPEND(initdb.Start(), "failed to start initdb");
    RETURN_NOT_OK_PREPEND(initdb.Wait(), "failed to wait on initdb");

    // Postgres doesn't support binding to 0 so we need to get a random unused
    // port and persist that to the config file.
    if (port_ == 0) {
      RETURN_NOT_OK(GetRandomPort(host_, &port_));
    }
    RETURN_NOT_OK(CreateConfigs());
  }

  process_.reset(new Subprocess({
      JoinPathSegments(bin_dir_, "postgres/postgres"),
      "-D", pgr}));
  // LIBDIR needs to point to the directory containing the Postgres libraries,
  // otherwise it defaults to /usr/lib/postgres.
  process_->SetEnvVars({
      { "LIBDIR", JoinPathSegments(bin_dir_, "postgres-lib") }
  });
  RETURN_NOT_OK(process_->Start());

  Status wait = WaitForTcpBind(process_->pid(), &port_, { host_ },
                               MonoDelta::FromMilliseconds(kPgStartTimeoutMs));
  if (!wait.ok()) {
    // TODO(abukor): implement retry with a different port if it can't bind
    WARN_NOT_OK(process_->Kill(SIGINT), "failed to send SIGINT to Postgres");
    return wait;
  }

  LOG(INFO) << "Postgres bound to " << port_;
  return WaitForReady();
}

  Status MiniPostgres::Stop() {
  if (process_) {
    RETURN_NOT_OK(process_->KillAndWait(SIGTERM));
    process_.reset();
  }
  return Status::OK();
}

Status MiniPostgres::AddUser(const string& user, bool super) {
  Subprocess add({
    JoinPathSegments(bin_dir_, "postgres/createuser"),
    user,
    Substitute("--$0superuser", super ? "" : "no-"),
    "-p", SimpleItoa(port_),
    "-h", host_,
  });
  RETURN_NOT_OK(add.Start());
  return add.WaitAndCheckExitCode();
}

Status MiniPostgres::CreateDb(const string& db, const string& owner) {
  Subprocess createdb({
    JoinPathSegments(bin_dir_, "postgres/createdb"),
    "-O", owner, db,
    "-p", SimpleItoa(port_),
    "-h", host_,
  });
  RETURN_NOT_OK(createdb.Start());
  return createdb.WaitAndCheckExitCode();
}

Status MiniPostgres::CreateConfigs() {
  Env* env = Env::Default();
  // <pg_root>/postgresql.conf is generated by initdb in a previous step. We
  // append the port to it.
  string config_file = JoinPathSegments(pg_root(), "postgresql.conf");
  faststring config;
  ReadFileToString(env, config_file, &config);
  config.append(Substitute("\nlisten_addresses = '$0'\nport = $1\n", host_, port_));
  unique_ptr<WritableFile> file;
  WritableFileOptions opts;
  opts.is_sensitive = false;
  RETURN_NOT_OK(env->NewWritableFile(opts, config_file, &file));
  RETURN_NOT_OK(file->Append(config));
  return file->Close();
}

Status MiniPostgres::WaitForReady() const {
  Status s;
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(5);
  while (MonoTime::Now() < deadline) {
    Subprocess psql({
        JoinPathSegments(bin_dir_, "postgres/pg_isready"),
        "-p", SimpleItoa(port_),
        "-h", host_,
    });
    RETURN_NOT_OK(psql.Start());
    s = psql.WaitAndCheckExitCode();
    if (s.ok()) {
      return s;
    }
    SleepFor(MonoDelta::FromMilliseconds(100));
  }

  return Status::TimedOut(s.ToString());
}

} // namespace postgres
} // namespace kudu
