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

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <boost/container_hash/hash_fwd.hpp>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/macros.h"
#include "kudu/rpc/connection_id.h"
#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/util/locks.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"

namespace google {
namespace protobuf {
class Message;
} // namespace protobuf
} // namespace google

namespace kudu {

class DnsResolver;
class Sockaddr;

namespace rpc {

struct OutboundMethodMetrics {
  scoped_refptr<Counter> request_bytes;
  scoped_refptr<Counter> response_bytes;
};

struct ProxyMetrics {};
using ProxyMetricsPtr = std::shared_ptr<ProxyMetrics>;

using ProxyMetricsFactory = ProxyMetricsPtr(*)(const scoped_refptr<MetricEntity>& entity);

template <size_t size>
struct ProxyMetricsImpl : public ProxyMetrics {
  std::array<OutboundMethodMetrics, size> value;
};

class Messenger;
class RpcController;
class UserCredentials;
class ProxyContext;
class Protocol;

// Interface to send calls to a remote service.
//
// Proxy objects do not map one-to-one with TCP connections.  The underlying TCP
// connection is not established until the first call, and may be torn down and
// re-established as necessary by the messenger. Additionally, the messenger is
// likely to multiplex many Proxy objects on the same connection.
//
// A proxy object can optionally specify the "network plane" it uses. This allows
// proxies of N services to be multiplexed on M TCP connections so that a higher priority
// service (e.g. a control channel) may use a different connection than other services,
// avoiding the chance of being blocked by traffic of other services.
//
// Proxy objects are thread-safe after initialization only.
// Setters on the Proxy are not thread-safe, and calling a setter after any RPC
// request has started will cause a fatal error.
//
// After initialization, multiple threads may make calls using the same proxy object.
class Proxy {
 public:
  Proxy(std::shared_ptr<Messenger> messenger,
        const Sockaddr& remote,
        std::string hostname,
        std::string service_name);

  // TODO(awong): consider a separate auto-resolving proxy class?
  Proxy(std::shared_ptr<Messenger> messenger,
        HostPort hp,
        DnsResolver* dns_resolver,
        std::string service_name);

  ~Proxy();

  // If the proxy is configured for address re-resolution (by supplying a
  // DnsResolver and HostPort in the constructor), performs an initial
  // resolution of the address using the HostPort. If 'addr' is supplied, it is
  // used instead of performing resolution (this is useful to initialize
  // several proxies with a single external DNS resolution).
  //
  // Otherwise, this is a no-op.
  //
  // NOTE: it is always OK to skip calling this method -- if this proxy is
  // configured for address re-resolution and this is skipped, the resolution
  // will happen upon sending the first request.
  void Init(Sockaddr addr = {});

  // Call a remote method asynchronously.
  //
  // Typically, users will not call this directly, but rather through
  // a generated Proxy subclass.
  //
  // method: the method name to invoke on the remote server.
  //
  // req:  the request protobuf. This will be serialized immediately,
  //       so the caller may free or otherwise mutate 'req' safely.
  //
  // resp: the response protobuf. This protobuf will be mutated upon
  //       completion of the call. The RPC system does not take ownership
  //       of this storage.
  //
  // NOTE: 'req' and 'resp' should be the appropriate protocol buffer implementation
  // class corresponding to the parameter and result types of the service method
  // defined in the service's '.proto' file.
  //
  // controller: the RpcController to associate with this call. Each call
  //             must use a unique controller object. Does not take ownership.
  //
  // callback: the callback to invoke upon call completion. This callback may
  //           be invoked before AsyncRequest() itself returns, or any time
  //           thereafter. It may be invoked either on the caller's thread
  //           or by an RPC IO thread, and thus should take care to not
  //           block or perform any heavy CPU work.
  void AsyncRequest(const std::string& method,
                    const google::protobuf::Message& req,
                    google::protobuf::Message* resp,
                    RpcController* controller,
                    const ResponseCallback& callback);

  // The same as AsyncRequest(), except that the call blocks until the call
  // finishes. If the call fails, returns a non-OK result.
  Status SyncRequest(const std::string& method,
                     const google::protobuf::Message& req,
                     google::protobuf::Message* resp,
                     RpcController* controller);

  // Set the user credentials which should be used to log in.
  void set_user_credentials(UserCredentials user_credentials);

  // Get the user credentials which should be used to log in.
  const UserCredentials& user_credentials() const { return conn_id_.user_credentials(); }

  // Set the network plane which this proxy uses.
  void set_network_plane(std::string network_plane);

  // Get the network plane which this proxy uses.
  const std::string& network_plane() const { return conn_id_.network_plane(); }

  std::string ToString() const;

 private:
  // Asynchronously refreshes the DNS, enqueueing the given request upon
  // success, or failing the call and calling the callback upon failure.
  void RefreshDnsAndEnqueueRequest(const std::string& method,
                                   std::unique_ptr<RequestPayload> req_payload,
                                   google::protobuf::Message* response,
                                   RpcController* controller,
                                   const ResponseCallback& callback);

  // Queues the given request as an outbound call using the given messenger,
  // controller, and response.
  void EnqueueRequest(const std::string& method,
                      std::unique_ptr<RequestPayload> req_payload,
                      google::protobuf::Message* response,
                      RpcController* controller,
                      const ResponseCallback& callback,
                      OutboundCall::CallbackBehavior cb_behavior) const;

  // Returns a single Sockaddr from the 'addrs', logging a warning if there is
  // more than one to choose from.
  Sockaddr* GetSingleSockaddr(std::vector<Sockaddr>* addrs) const;

  ConnectionId conn_id() const {
    std::lock_guard<simple_spinlock> l(lock_);
    return conn_id_;
  }

  const std::string service_name_;
  HostPort hp_;
  DnsResolver* dns_resolver_;
  std::shared_ptr<Messenger> messenger_;

  // TODO(awong): consider implementing some lock-free list of ConnectionIds
  // instead of taking a lock every time we want to get the "current"
  // ConnectionId.
  //
  // Connection ID used by this proxy. Once the proxy has started sending
  // requests, the connection ID may be updated in response to calls (e.g. if
  // we re-resolved the physical address in response to an invalid DNS entry).
  // As such, 'conn_id_' is protected by 'lock_', and should be copied and
  // passed around, rather than used directly
  mutable simple_spinlock lock_;
  ConnectionId conn_id_;

  mutable Atomic32 is_started_;

  DISALLOW_COPY_AND_ASSIGN(Proxy);
};

using ProxyPtr = std::shared_ptr<Proxy>;

class ProxyCache {
 public:
  explicit ProxyCache(ProxyContext* context)
      : context_(context) {}

  ProxyPtr GetProxy(
      const HostPort& remote, const Protocol* protocol, const MonoDelta& resolve_cache_timeout);

  ProxyMetricsPtr GetMetrics(const std::string& service_name, ProxyMetricsFactory factory);

 private:
  typedef std::pair<HostPort, const Protocol*> ProxyKey;

  struct ProxyKeyHash {
    size_t operator()(const ProxyKey& key) const {
      size_t result = 0;
      boost::hash_combine(result, key.first);
      boost::hash_combine(result, key.second);
      return result;
    }
  };

  ProxyContext* context_;

  std::mutex proxy_mutex_;
  std::unordered_map<ProxyKey, ProxyPtr, ProxyKeyHash> proxies_ GUARDED_BY(proxy_mutex_);

  std::mutex metrics_mutex_;
  std::unordered_map<std::string , ProxyMetricsPtr> metrics_ GUARDED_BY(metrics_mutex_);
};

} // namespace rpc
} // namespace kudu
