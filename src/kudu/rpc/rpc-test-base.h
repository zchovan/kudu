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

#include <algorithm>
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "kudu/gutil/walltime.h"
#include "kudu/rpc/acceptor_pool.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/proxy.h"
#include "kudu/rpc/reactor.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/rpc/rtest.pb.h"
#include "kudu/rpc/rtest.proxy.h"
#include "kudu/rpc/rtest.service.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/security/security-test-util.h"
#include "kudu/util/crc.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/jwt.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/trace.h"

DECLARE_bool(rpc_encrypt_loopback_connections);

using kudu::rpc_test::AddRequestPB;
using kudu::rpc_test::AddResponsePB;
using kudu::rpc_test::CalculatorError;
using kudu::rpc_test::CalculatorServiceIf;
using kudu::rpc_test::CalculatorServiceProxy;
using kudu::rpc_test::EchoRequestPB;
using kudu::rpc_test::EchoResponsePB;
using kudu::rpc_test::ExactlyOnceRequestPB;
using kudu::rpc_test::ExactlyOnceResponsePB;
using kudu::rpc_test::FeatureFlags;
using kudu::rpc_test::PanicRequestPB;
using kudu::rpc_test::PanicResponsePB;
using kudu::rpc_test::PushStringsRequestPB;
using kudu::rpc_test::PushStringsResponsePB;
using kudu::rpc_test::SendTwoStringsRequestPB;
using kudu::rpc_test::SendTwoStringsResponsePB;
using kudu::rpc_test::SleepRequestPB;
using kudu::rpc_test::SleepResponsePB;
using kudu::rpc_test::SleepWithSidecarRequestPB;
using kudu::rpc_test::SleepWithSidecarResponsePB;
using kudu::rpc_test::TestInvalidResponseRequestPB;
using kudu::rpc_test::TestInvalidResponseResponsePB;
using kudu::rpc_test::WhoAmIRequestPB;
using kudu::rpc_test::WhoAmIResponsePB;
using kudu::rpc_test_diff_package::ReqDiffPackagePB;
using kudu::rpc_test_diff_package::RespDiffPackagePB;

namespace kudu {
namespace rpc {

// Implementation of CalculatorService which just implements the generic
// RPC handler (no generated code).
class GenericCalculatorService : public ServiceIf {
 public:
  static const std::string kFullServiceName;
  static const std::string kAddMethodName;
  static const std::string kSleepMethodName;
  static const std::string kSleepWithSidecarMethodName;
  static const std::string kPushStringsMethodName;
  static const std::string kSendTwoStringsMethodName;
  static const std::string kAddExactlyOnce;

  static const char* kFirstString;
  static const char* kSecondString;

  GenericCalculatorService() {
  }

  // To match the argument list of the generated CalculatorService.
  explicit GenericCalculatorService(const scoped_refptr<MetricEntity>& entity,
                                    const scoped_refptr<ResultTracker>& result_tracker) {
    // this test doesn't generate metrics, so we ignore the argument.
  }

  void Handle(InboundCall *incoming) override {
    if (incoming->remote_method().method_name() == kAddMethodName) {
      DoAdd(incoming);
    } else if (incoming->remote_method().method_name() == kSleepMethodName) {
      DoSleep(incoming);
    } else if (incoming->remote_method().method_name() == kSleepWithSidecarMethodName) {
      DoSleepWithSidecar(incoming);
    } else if (incoming->remote_method().method_name() == kSendTwoStringsMethodName) {
      DoSendTwoStrings(incoming);
    } else if (incoming->remote_method().method_name() == kPushStringsMethodName) {
      DoPushStrings(incoming);
    } else {
      incoming->RespondFailure(ErrorStatusPB::ERROR_NO_SUCH_METHOD,
                               Status::InvalidArgument("bad method"));
    }
  }

  const std::string& service_name() const override { return kFullServiceName; }
  static const std::string& static_service_name() { return kFullServiceName; }

 private:
  void DoAdd(InboundCall *incoming) {
    Slice param(incoming->serialized_request());
    AddRequestPB req;
    if (!req.ParseFromArray(param.data(), param.size())) {
      LOG(FATAL) << "couldn't parse: " << param.ToDebugString();
    }

    AddResponsePB resp;
    resp.set_result(req.x() + req.y());
    incoming->RespondSuccess(resp);
  }

  void DoSendTwoStrings(InboundCall* incoming) {
    Slice param(incoming->serialized_request());
    SendTwoStringsRequestPB req;
    if (!req.ParseFromArray(param.data(), param.size())) {
      LOG(FATAL) << "couldn't parse: " << param.ToDebugString();
    }

    faststring first;

    Random r(req.random_seed());
    first.resize(req.size1());
    RandomString(first.data(), req.size1(), &r);

    // The second string gets sent in two separate buffers, which get
    // concatenated on the client side.
    faststring second_data;
    second_data.resize(req.size2());
    RandomString(second_data.data(), second_data.size(), &r);

    std::vector<faststring> second(2);
    second[0].append(second_data.data(), second_data.size() / 3);
    second[1].append(second_data.data() + second[0].size(),
                     second_data.size() - second[0].size());

    SendTwoStringsResponsePB resp;
    int idx1, idx2;
    CHECK_OK(incoming->AddOutboundSidecar(
            RpcSidecar::FromFaststring(std::move(first)), &idx1));

    CHECK_OK(incoming->AddOutboundSidecar(
            RpcSidecar::FromFaststrings(std::move(second)), &idx2));
    resp.set_sidecar1(idx1);
    resp.set_sidecar2(idx2);

    incoming->RespondSuccess(resp);
  }

  static void DoPushStrings(InboundCall* incoming) {
    Slice param(incoming->serialized_request());
    PushStringsRequestPB req;
    if (!req.ParseFromArray(param.data(), param.size())) {
      LOG(FATAL) << "couldn't parse: " << param.ToDebugString();
    }

    // Check that reading non-existant sidecars doesn't work.
    Slice tmp;
    CHECK(!incoming->GetInboundSidecar(req.sidecar_indexes_size() + 2, &tmp).ok());

    PushStringsResponsePB resp;
    for (const auto& sidecar_idx : req.sidecar_indexes()) {
      Slice sidecar;
      CHECK_OK(incoming->GetInboundSidecar(sidecar_idx, &sidecar));

      resp.add_sizes(sidecar.size());
      resp.add_crcs(crc::Crc32c(sidecar.data(), sidecar.size()));
    }

    // Drop the sidecars etc, just to confirm that it's safe to do so.
    CHECK_GT(incoming->GetTransferSize(), 0);
    incoming->DiscardTransfer();
    CHECK_EQ(0, incoming->GetTransferSize());
    incoming->RespondSuccess(resp);
  }

  void DoSleep(InboundCall *incoming) {
    Slice param(incoming->serialized_request());
    SleepRequestPB req;
    if (!req.ParseFromArray(param.data(), param.size())) {
      incoming->RespondFailure(ErrorStatusPB::ERROR_INVALID_REQUEST,
        Status::InvalidArgument("Couldn't parse pb",
                                req.InitializationErrorString()));
      return;
    }

    LOG(INFO) << "got call: " << pb_util::SecureShortDebugString(req);
    SleepFor(MonoDelta::FromMicroseconds(req.sleep_micros()));
    MonoDelta duration(MonoTime::Now() - incoming->GetTimeReceived());
    CHECK_GE(duration.ToMicroseconds(), req.sleep_micros());
    SleepResponsePB resp;
    incoming->RespondSuccess(resp);
  }

  void DoSleepWithSidecar(InboundCall *incoming) {
    Slice param(incoming->serialized_request());
    SleepWithSidecarRequestPB req;
    if (!req.ParseFromArray(param.data(), param.size())) {
      incoming->RespondFailure(ErrorStatusPB::ERROR_INVALID_REQUEST,
        Status::InvalidArgument("Couldn't parse pb",
                                req.InitializationErrorString()));
      return;
    }

    LOG(INFO) << "got call: " << pb_util::SecureShortDebugString(req);
    SleepFor(MonoDelta::FromMicroseconds(req.sleep_micros()));

    uint32_t pattern = req.pattern();
    uint32_t num_repetitions = req.num_repetitions();
    Slice sidecar;
    CHECK_OK(incoming->GetInboundSidecar(req.sidecar_idx(), &sidecar));
    CHECK_EQ(sidecar.size(), sizeof(uint32) * num_repetitions);
    const uint32_t *data = reinterpret_cast<const uint32_t*>(sidecar.data());
    for (int i = 0; i < num_repetitions; ++i) CHECK_EQ(data[i], pattern);

    SleepResponsePB resp;
    incoming->RespondSuccess(resp);
  }
};

class CalculatorService : public CalculatorServiceIf {
 public:
  explicit CalculatorService(const scoped_refptr<MetricEntity>& entity,
                             const scoped_refptr<ResultTracker> result_tracker)
    : CalculatorServiceIf(entity, result_tracker),
      exactly_once_test_val_(0) {
  }

  void Add(const AddRequestPB *req, AddResponsePB *resp, RpcContext *context) override {
    CHECK_GT(context->GetTransferSize(), 0);
    CHECK_GE(context->call_id(), 0);
    resp->set_result(req->x() + req->y());
    context->RespondSuccess();
  }

  void Sleep(const SleepRequestPB *req, SleepResponsePB *resp, RpcContext *context) override {
    if (req->return_app_error()) {
      CalculatorError my_error;
      my_error.set_extra_error_data("some application-specific error data");
      context->RespondApplicationError(CalculatorError::app_error_ext.number(),
                                       "Got some error", my_error);
      return;
    }

    // Respond w/ error if the RPC specifies that the client deadline is set,
    // but it isn't.
    if (req->client_timeout_defined()) {
      MonoTime deadline = context->GetClientDeadline();
      if (deadline == MonoTime::Max()) {
        CalculatorError my_error;
        my_error.set_extra_error_data("Timeout not set");
        context->RespondApplicationError(CalculatorError::app_error_ext.number(),
                                        "Missing required timeout", my_error);
        return;
      }
    }

    if (req->deferred()) {
      std::thread t([this, req, context]() { this->DoSleep(req, context); });
      t.detach();
      return;
    }
    DoSleep(req, context);
  }

  void Echo(const EchoRequestPB *req, EchoResponsePB *resp, RpcContext *context) override {
    resp->set_data(req->data());
    context->RespondSuccess();
  }

  void WhoAmI(const WhoAmIRequestPB* /*req*/,
              WhoAmIResponsePB* resp,
              RpcContext* context) override {
    const RemoteUser& user = context->remote_user();
    resp->mutable_credentials()->set_real_user(user.username());
    resp->set_address(context->remote_address().ToString());
    context->RespondSuccess();
  }

  void TestArgumentsInDiffPackage(const ReqDiffPackagePB *req,
                                  RespDiffPackagePB *resp,
                                  ::kudu::rpc::RpcContext *context) override {
    context->RespondSuccess();
  }

  void Panic(const PanicRequestPB* req, PanicResponsePB* resp, RpcContext* context) override {
    TRACE("Got panic request");
    PANIC_RPC(context, "Test method panicking!");
  }

  void TestInvalidResponse(const TestInvalidResponseRequestPB* req,
                           TestInvalidResponseResponsePB* resp,
                           RpcContext* context) override {
    switch (req->error_type()) {
      case rpc_test::TestInvalidResponseRequestPB_ErrorType_MISSING_REQUIRED_FIELD:
        // Respond without setting the 'resp->response' protobuf field, which is
        // marked as required. This exercises the error path of invalid responses.
        context->RespondSuccess();
        break;
      case rpc_test::TestInvalidResponseRequestPB_ErrorType_RESPONSE_TOO_LARGE:
        resp->mutable_response()->resize(FLAGS_rpc_max_message_size + 1000);
        context->RespondSuccess();
        break;
      default:
        LOG(FATAL);
    }
  }

  bool SupportsFeature(uint32_t feature) const override {
    return feature == FeatureFlags::FOO;
  }

  void AddExactlyOnce(const ExactlyOnceRequestPB* req, ExactlyOnceResponsePB* resp,
                      ::kudu::rpc::RpcContext* context) override {
    if (req->sleep_for_ms() > 0) {
      usleep(req->sleep_for_ms() * 1000);
    }
    // If failures are enabled, cause them some percentage of the time.
    if (req->randomly_fail()) {
      if (rand() % 10 < 3) {
        context->RespondFailure(Status::ServiceUnavailable("Random injected failure."));
        return;
      }
    }
    int result = exactly_once_test_val_ += req->value_to_add();
    resp->set_current_val(result);
    resp->set_current_time_micros(GetCurrentTimeMicros());
    context->RespondSuccess();
  }

  bool AuthorizeDisallowAlice(const google::protobuf::Message* /*req*/,
                              google::protobuf::Message* /*resp*/,
                              RpcContext* context) override {
    if (context->remote_user().username() == "alice") {
      context->RespondFailure(Status::NotAuthorized("alice is not allowed to call this method"));
      return false;
    }
    return true;
  }

  bool AuthorizeDisallowBob(const google::protobuf::Message* /*req*/,
                            google::protobuf::Message* /*resp*/,
                            RpcContext* context) override {
    if (context->remote_user().username() == "bob") {
      context->RespondFailure(Status::NotAuthorized("bob is not allowed to call this method"));
      return false;
    }
    return true;
  }

 private:
  void DoSleep(const SleepRequestPB *req,
               RpcContext *context) {
    TRACE_COUNTER_INCREMENT("test_sleep_us", req->sleep_micros());
    if (Trace::CurrentTrace()) {
      scoped_refptr<Trace> child_trace(new Trace());
      Trace::CurrentTrace()->AddChildTrace("test_child", child_trace.get());
      ADOPT_TRACE(child_trace.get());
      TRACE_COUNTER_INCREMENT("related_trace_metric", 1);
    }

    SleepFor(MonoDelta::FromMicroseconds(req->sleep_micros()));
    context->RespondSuccess();
  }

  std::atomic_int exactly_once_test_val_;

};

const std::string GenericCalculatorService::kFullServiceName =
    "kudu.rpc.GenericCalculatorService";
const std::string GenericCalculatorService::kAddMethodName = "Add";
const std::string GenericCalculatorService::kSleepMethodName = "Sleep";
const std::string GenericCalculatorService::kSleepWithSidecarMethodName = "SleepWithSidecar";
const std::string GenericCalculatorService::kPushStringsMethodName = "PushStrings";
const std::string GenericCalculatorService::kSendTwoStringsMethodName = "SendTwoStrings";
const std::string GenericCalculatorService::kAddExactlyOnce = "AddExactlyOnce";

const char *GenericCalculatorService::kFirstString =
    "1111111111111111111111111111111111111111111111111111111111";
const char *GenericCalculatorService::kSecondString =
    "2222222222222222222222222222222222222222222222222222222222222222222222";

class RpcTestBase : public KuduTest {
 public:
  RpcTestBase()
      : n_acceptor_pool_threads_(2),
        n_server_reactor_threads_(3),
        n_worker_threads_(3),
        keepalive_time_ms_(1000),
        service_queue_length_(200),
        metric_entity_(METRIC_ENTITY_server.Instantiate(&metric_registry_, "test.rpc_test")) {
  }

  void TearDown() override {
    if (service_pool_) {
      server_messenger_->UnregisterService(service_name_);
      service_pool_->Shutdown();
    }
    if (server_messenger_) {
      server_messenger_->Shutdown();
    }
    KuduTest::TearDown();
  }

 protected:
  Status CreateMessenger(const std::string& name,
                         std::shared_ptr<Messenger>* messenger,
                         int n_reactors = 1,
                         bool enable_ssl = false,
                         const std::string& rpc_certificate_file = "",
                         const std::string& rpc_private_key_file = "",
                         const std::string& rpc_ca_certificate_file = "",
                         const std::string& rpc_private_key_password_cmd = "") {
    MessengerBuilder bld(name);

    if (enable_ssl) {
      FLAGS_rpc_encrypt_loopback_connections = true;
      bld.set_epki_cert_key_files(rpc_certificate_file, rpc_private_key_file);
      bld.set_epki_certificate_authority_file(rpc_ca_certificate_file);
      bld.set_epki_private_password_key_cmd(rpc_private_key_password_cmd);
      bld.set_rpc_encryption("required");
      bld.enable_inbound_tls();
    }

    bld.set_num_reactors(n_reactors);
    bld.set_connection_keepalive_time(MonoDelta::FromMilliseconds(keepalive_time_ms_));
    if (keepalive_time_ms_ >= 0) {
      // In order for the keepalive timing to be accurate, we need to scan connections
      // significantly more frequently than the keepalive time. This "coarse timer"
      // granularity determines this.
      bld.set_coarse_timer_granularity(
          MonoDelta::FromMilliseconds(std::min(keepalive_time_ms_ / 5, 100)));
    }
    bld.set_metric_entity(metric_entity_);
    return bld.Build(messenger);
  }

  static Status DoTestSyncCall(Proxy* p, const std::string& method,
                               CredentialsPolicy policy = CredentialsPolicy::ANY_CREDENTIALS) {
    AddRequestPB req;
    req.set_x(rand());
    req.set_y(rand());
    AddResponsePB resp;
    RpcController controller;
    controller.set_timeout(MonoDelta::FromMilliseconds(10000));
    controller.set_credentials_policy(policy);
    RETURN_NOT_OK(p->SyncRequest(method, req, &resp, &controller));

    CHECK_EQ(req.x() + req.y(), resp.result());
    return Status::OK();
  }

static void DoTestSidecar(Proxy* p, int size1, int size2) {
    const uint32_t kSeed = 12345;

    SendTwoStringsRequestPB req;
    req.set_size1(size1);
    req.set_size2(size2);
    req.set_random_seed(kSeed);

    SendTwoStringsResponsePB resp;
    RpcController controller;
    controller.set_timeout(MonoDelta::FromMilliseconds(10000));
    CHECK_OK(p->SyncRequest(GenericCalculatorService::kSendTwoStringsMethodName,
                            req, &resp, &controller));

    Slice first = GetSidecarPointer(controller, resp.sidecar1(), size1);
    Slice second = GetSidecarPointer(controller, resp.sidecar2(), size2);
    Random rng(kSeed);
    faststring expected;

    expected.resize(size1);
    RandomString(expected.data(), size1, &rng);
    CHECK_EQ(Slice(expected), first);

    expected.resize(size2);
    RandomString(expected.data(), size2, &rng);
    CHECK_EQ(Slice(expected), second);
  }

  static Status DoTestOutgoingSidecar(Proxy* p, int size1, int size2) {
    return DoTestOutgoingSidecar(p, {std::string(size1, 'a'), std::string(size2, 'b')});
  }

  static Status DoTestOutgoingSidecar(Proxy* p, const std::vector<std::string>& strings) {
    PushStringsRequestPB request;
    RpcController controller;

    for (const auto& s : strings) {
      int idx;
      CHECK_OK(controller.AddOutboundSidecar(RpcSidecar::FromSlice(Slice(s)), &idx));
      request.add_sidecar_indexes(idx);
    }

    PushStringsResponsePB resp;
    KUDU_RETURN_NOT_OK(p->SyncRequest(GenericCalculatorService::kPushStringsMethodName,
                                      request, &resp, &controller));
    for (int i = 0; i < strings.size(); i++) {
      CHECK_EQ(strings[i].size(), resp.sizes(i));
      CHECK_EQ(crc::Crc32c(strings[i].data(), strings[i].size()),
               resp.crcs(i));
    }

    return Status::OK();
  }

  static void DoTestOutgoingSidecarExpectOK(Proxy* p, int size1, int size2) {
    CHECK_OK(DoTestOutgoingSidecar(p, size1, size2));
  }

  static void DoTestExpectTimeout(Proxy* p,
                                  const MonoDelta& timeout,
                                  bool will_be_cancelled = false,
                                  bool* is_negotiaton_error = nullptr) {
    SleepRequestPB req;
    SleepResponsePB resp;
    // Sleep for 500ms longer than the call timeout.
    int sleep_micros = timeout.ToMicroseconds() + 500 * 1000;
    req.set_sleep_micros(sleep_micros);

    RpcController c;
    c.set_timeout(timeout);
    Stopwatch sw;
    sw.start();
    Status s = p->SyncRequest(GenericCalculatorService::kSleepMethodName, req, &resp, &c);
    sw.stop();
    ASSERT_FALSE(s.ok());
    if (is_negotiaton_error != nullptr) {
      *is_negotiaton_error = c.negotiation_failed();
    }

    int expected_millis = timeout.ToMilliseconds();
    int elapsed_millis = static_cast<int>(sw.elapsed().wall_millis());

    // We shouldn't timeout significantly faster than our configured timeout, unless the
    // rpc is cancelled.
    if (!will_be_cancelled) {
      EXPECT_GE(elapsed_millis, expected_millis - 10);
    }
    // And we also shouldn't take the full time that we asked for
    EXPECT_LT(elapsed_millis * 1000, sleep_micros);
    if (will_be_cancelled) {
      // Cancellation is best effort, so even if we cancel the rpc it may still end up
      // with a timed out status.
      EXPECT_TRUE(s.IsAborted() || s.IsTimedOut()) << s.ToString();
    } else {
      EXPECT_TRUE(s.IsTimedOut()) << s.ToString();
    }
    LOG(INFO) << "status: " << s.ToString() << ", seconds elapsed: " << sw.elapsed().wall_seconds();
  }

  Status StartTestServer(Sockaddr *server_addr,
                         bool enable_ssl = false,
                         const std::string& rpc_certificate_file = "",
                         const std::string& rpc_private_key_file = "",
                         const std::string& rpc_ca_certificate_file = "",
                         const std::string& rpc_private_key_password_cmd = "") {
    return DoStartTestServer<GenericCalculatorService>(
        server_addr, enable_ssl, rpc_certificate_file, rpc_private_key_file,
        rpc_ca_certificate_file, rpc_private_key_password_cmd);
  }

  Status StartTestServerWithGeneratedCode(Sockaddr *server_addr, bool enable_ssl = false) {
    return DoStartTestServer<CalculatorService>(server_addr, enable_ssl);
  }

  Status StartTestServerWithCustomMessenger(Sockaddr *server_addr,
      const std::shared_ptr<Messenger>& messenger, bool enable_ssl = false) {
    return DoStartTestServer<GenericCalculatorService>(
        server_addr, enable_ssl, "", "", "", "", messenger);
  }

  // Start a simple socket listening on a local port, returning the address.
  // This isn't an RPC server -- just a plain socket which can be helpful for testing.
  static Status StartFakeServer(Socket *listen_sock, Sockaddr *listen_addr) {
    Sockaddr bind_addr = Sockaddr::Wildcard();
    bind_addr.set_port(0);
    RETURN_NOT_OK(listen_sock->Init(bind_addr.family(), 0));
    RETURN_NOT_OK(listen_sock->BindAndListen(bind_addr, 1));
    RETURN_NOT_OK(listen_sock->GetSocketAddress(listen_addr));
    LOG(INFO) << "Bound to: " << listen_addr->ToString();
    return Status::OK();
  }

 private:

  static Slice GetSidecarPointer(const RpcController& controller, int idx,
                                 int expected_size) {
    Slice sidecar;
    CHECK_OK(controller.GetInboundSidecar(idx, &sidecar));
    CHECK_EQ(expected_size, sidecar.size());
    return Slice(sidecar.data(), expected_size);
  }

  template<class ServiceClass>
  Status DoStartTestServer(Sockaddr* server_addr,
                           bool enable_ssl = false,
                           const std::string& rpc_certificate_file = "",
                           const std::string& rpc_private_key_file = "",
                           const std::string& rpc_ca_certificate_file = "",
                           const std::string& rpc_private_key_password_cmd = "",
                           const std::shared_ptr<Messenger>& messenger = nullptr) {
    // Default to binding on wildcard unless specified as an in-out parameter.
    if (!server_addr->is_initialized()) {
      *server_addr = Sockaddr::Wildcard();
    }
    if (!messenger) {
      RETURN_NOT_OK(CreateMessenger(
          "TestServer", &server_messenger_, n_server_reactor_threads_, enable_ssl,
          rpc_certificate_file, rpc_private_key_file, rpc_ca_certificate_file,
          rpc_private_key_password_cmd));
    } else {
      server_messenger_ = messenger;
    }
    std::shared_ptr<AcceptorPool> pool;
    RETURN_NOT_OK(server_messenger_->AddAcceptorPool(*server_addr, &pool));
    RETURN_NOT_OK(pool->Start(n_acceptor_pool_threads_));
    *server_addr = pool->bind_address();
    mem_tracker_ = MemTracker::CreateTracker(-1, "result_tracker");
    result_tracker_.reset(new ResultTracker(mem_tracker_));

    std::unique_ptr<ServiceIf> service(new ServiceClass(metric_entity_, result_tracker_));
    service_name_ = service->service_name();
    scoped_refptr<MetricEntity> metric_entity = server_messenger_->metric_entity();
    service_pool_ = new ServicePool(std::move(service), metric_entity, service_queue_length_);
    RETURN_NOT_OK(server_messenger_->RegisterService(service_name_, service_pool_));
    return service_pool_->Init(n_worker_threads_);
  }

 protected:
  int n_acceptor_pool_threads_;
  int n_server_reactor_threads_;
  int n_worker_threads_;
  int keepalive_time_ms_;
  int service_queue_length_;

  std::string service_name_;
  std::shared_ptr<Messenger> server_messenger_;
  scoped_refptr<ServicePool> service_pool_;
  std::shared_ptr<kudu::MemTracker> mem_tracker_;
  scoped_refptr<ResultTracker> result_tracker_;

  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
};

} // namespace rpc
} // namespace kudu
