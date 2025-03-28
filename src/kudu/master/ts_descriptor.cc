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

#include "kudu/master/ts_descriptor.h"

#include <cmath>
#include <optional>
#include <ostream>
#include <shared_mutex>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"

DEFINE_int32(tserver_unresponsive_timeout_ms, 60 * 1000,
             "The period of time that a Master can go without receiving a heartbeat from a "
             "tablet server before considering it unresponsive. Unresponsive servers are not "
             "selected when assigning replicas during table creation or re-replication.");
TAG_FLAG(tserver_unresponsive_timeout_ms, advanced);

DEFINE_double(tserver_last_replica_creations_halflife_ms, 60 * 1000,
              "The half-life of last replica creations time. Only for testing!");
TAG_FLAG(tserver_last_replica_creations_halflife_ms, hidden);

using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using std::optional;
using std::shared_lock;
using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

Status TSDescriptor::RegisterNew(const NodeInstancePB& instance,
                                 const ServerRegistrationPB& registration,
                                 const optional<std::string>& location,
                                 DnsResolver* dns_resolver,
                                 shared_ptr<TSDescriptor>* desc) {
  shared_ptr<TSDescriptor> ret(TSDescriptor::make_shared(instance.permanent_uuid()));
  RETURN_NOT_OK(ret->Register(
      instance, registration, location, dns_resolver));
  *desc = std::move(ret);
  return Status::OK();
}

TSDescriptor::TSDescriptor(std::string perm_id)
    : permanent_uuid_(std::move(perm_id)),
      latest_seqno_(-1),
      last_heartbeat_(MonoTime::Now()),
      needs_full_report_(false),
      recent_replica_creations_(0),
      last_replica_creations_decay_(MonoTime::Now()),
      init_time_(MonoTime::Now()),
      num_live_replicas_(0) {
}

// Compares two repeated HostPortPB fields. Returns true if equal, false otherwise.
static bool HostPortPBsEqual(const google::protobuf::RepeatedPtrField<HostPortPB>& pb1,
                             const google::protobuf::RepeatedPtrField<HostPortPB>& pb2) {
  if (pb1.size() != pb2.size()) {
    return false;
  }

  // Do a set-based equality search.
  std::unordered_set<HostPort, HostPortHasher, HostPortEqualityPredicate> hostports1;
  std::unordered_set<HostPort, HostPortHasher, HostPortEqualityPredicate> hostports2;
  for (int i = 0; i < pb1.size(); i++) {
    hostports1.emplace(HostPortFromPB(pb1.Get(i)));
    hostports2.emplace(HostPortFromPB(pb2.Get(i)));
  }
  return hostports1 == hostports2;
}

Status TSDescriptor::Register(const NodeInstancePB& instance,
                              const ServerRegistrationPB& registration,
                              const optional<std::string>& location,
                              DnsResolver* dns_resolver) {
  std::lock_guard l(lock_);
  CHECK_EQ(instance.permanent_uuid(), permanent_uuid_);

  // TODO(KUDU-418): we don't currently support changing RPC addresses since the
  // host/port is stored persistently in each tablet's metadata.
  if (registration_ &&
      !HostPortPBsEqual(registration_->rpc_addresses(), registration.rpc_addresses())) {
    string msg = Substitute(
        "Tablet server $0 is attempting to re-register with a different host/port. "
        "This is not currently supported. Old: {$1} New: {$2}",
        instance.permanent_uuid(),
        SecureShortDebugString(*registration_),
        SecureShortDebugString(registration));
    LOG(ERROR) << msg;
    return Status::InvalidArgument(msg);
  }

  if (registration.rpc_addresses().empty()) {
    return Status::InvalidArgument(
        "invalid registration: must have at least one RPC address",
        SecureShortDebugString(registration));
  }

  if (instance.instance_seqno() < latest_seqno_) {
    return Status::AlreadyPresent(Substitute(
        "Cannot register with sequence number $0:"
        " Already have a registration from sequence number $1",
        instance.instance_seqno(), latest_seqno_));
  } else if (instance.instance_seqno() == latest_seqno_) {
    // It's possible that the TS registered, but our response back to it
    // got lost, so it's trying to register again with the same sequence
    // number. That's fine.
    LOG(INFO) << "Processing retry of TS registration from "
              << SecureShortDebugString(instance);
  }

  latest_seqno_ = instance.instance_seqno();
  registration_.reset(new ServerRegistrationPB(registration));
  ts_admin_proxy_.reset();
  consensus_proxy_.reset();
  dns_resolver_ = dns_resolver;
  location_ = location;
  return Status::OK();
}

void TSDescriptor::UpdateHeartbeatTime() {
  std::lock_guard l(lock_);
  last_heartbeat_ = MonoTime::Now();
}

MonoDelta TSDescriptor::TimeSinceHeartbeat() const {
  MonoTime now(MonoTime::Now());
  shared_lock l(lock_);
  return now - last_heartbeat_;
}

void TSDescriptor::UpdateNeedsFullTabletReport(bool needs_report) {
  std::lock_guard l(lock_);
  needs_full_report_ = needs_report;
}

bool TSDescriptor::needs_full_report() const  {
  shared_lock l(lock_);
  return needs_full_report_;
}

bool TSDescriptor::PresumedDead() const {
  return TimeSinceHeartbeat().ToMilliseconds() >= FLAGS_tserver_unresponsive_timeout_ms;
}

int64_t TSDescriptor::latest_seqno() const {
  shared_lock l(lock_);
  return latest_seqno_;
}

void TSDescriptor::DecayRecentReplicaCreationsUnlocked() {
  // In most cases, we won't have any recent replica creations, so
  // we don't need to bother calling the clock, etc.
  if (recent_replica_creations_ == 0) return;

  const double kHalflifeSecs = FLAGS_tserver_last_replica_creations_halflife_ms / 1000;
  const MonoTime now = MonoTime::Now();
  double secs_since_last_decay = (now - last_replica_creations_decay_).ToSeconds();
  recent_replica_creations_ *= pow(0.5, secs_since_last_decay / kHalflifeSecs);

  // If sufficiently small, reset down to 0 to take advantage of the fast path above.
  if (recent_replica_creations_ < 1e-12) {
    recent_replica_creations_ = 0;
  }
  last_replica_creations_decay_ = now;
}

// TODO(mreddy) Avoid hacky decay code by potentially getting information about recently
// placed replicas from the system catalog, tablet server takes too long to report during
// table creation process as requests aren't sent to servers until after selection process.
void TSDescriptor::DecayRecentReplicaCreationsByRangeUnlocked(const string& range_start_key,
                                                              const string& table_id) {
  // In most cases, we won't have any recent replica creations, so
  // we don't need to bother calling the clock, etc. Such cases include when
  // the map for the table or the range hasn't been initialized yet, or if the value is 0.
  if (recent_replicas_by_range_.find(table_id) == recent_replicas_by_range_.end() ||
      recent_replicas_by_range_[table_id].first.find(range_start_key) ==
      recent_replicas_by_range_[table_id].first.end() ||
      recent_replicas_by_range_[table_id].first[range_start_key] == 0) {
    return;
  }

  const double kHalflifeSecs = FLAGS_tserver_last_replica_creations_halflife_ms / 1000;
  const MonoTime now = MonoTime::Now();
  // If map for the table or range hasn't been initialized yet, use init_time_ as last decay.
  MonoTime last_decay =
      last_replica_decay_by_range_.find(table_id) == last_replica_decay_by_range_.end() ||
      last_replica_decay_by_range_[table_id].first.find(range_start_key) ==
      last_replica_decay_by_range_[table_id].first.end() ?
      init_time_ : last_replica_decay_by_range_[table_id].first[range_start_key];
  double secs_since_last_decay = (now - last_decay).ToSeconds();
  recent_replicas_by_range_[table_id].first[range_start_key] *=
      pow(0.5, secs_since_last_decay / kHalflifeSecs);

  // If sufficiently small, reset down to 0 to take advantage of the fast path above.
  if (recent_replicas_by_range_[table_id].first[range_start_key] < 1e-12) {
    recent_replicas_by_range_[table_id].first[range_start_key] = 0;
  }
  // First time this is set, it silently initializes last_replica_decay_by_range_[table_id].second
  // to 0. This fails monotime init check in DecayTableUnlocked() so it's set here.
  last_replica_decay_by_range_[table_id].first[range_start_key] = now;
  if (!last_replica_decay_by_range_[table_id].second.Initialized()) {
    last_replica_decay_by_range_[table_id].second = now;
  }
}

void TSDescriptor::DecayRecentReplicaCreationsByTableUnlocked(const string& table_id) {
  // In most cases, we won't have any recent replica creations, so
  // we don't need to bother calling the clock, etc.
  if (recent_replicas_by_range_.find(table_id) == recent_replicas_by_range_.end() ||
      recent_replicas_by_range_[table_id].second == 0) {
    return;
  }

  const double kHalflifeSecs = FLAGS_tserver_last_replica_creations_halflife_ms / 1000;
  const MonoTime now = MonoTime::Now();
  // If map for the table hasn't been initialized yet, use init_time_ as last decay.
  MonoTime last_decay =
      last_replica_decay_by_range_.find(table_id) == last_replica_decay_by_range_.end() ?
      init_time_ : last_replica_decay_by_range_[table_id].second;
  double secs_since_last_decay = (now - last_decay).ToSeconds();
  recent_replicas_by_range_[table_id].second *= pow(0.5, secs_since_last_decay / kHalflifeSecs);

  // If sufficiently small, reset down to 0 to take advantage of the fast path above.
  if (recent_replicas_by_range_[table_id].second < 1e-12) {
    recent_replicas_by_range_[table_id].second = 0;
  }
  last_replica_decay_by_range_[table_id].second = now;
}

void TSDescriptor::IncrementRecentReplicaCreations() {
  std::lock_guard l(lock_);
  DecayRecentReplicaCreationsUnlocked();
  recent_replica_creations_ += 1;
}

void TSDescriptor::IncrementRecentReplicaCreationsByRangeAndTable(const string& range_key_start,
                                                                  const string& table_id) {
  std::lock_guard l(lock_);
  DecayRecentReplicaCreationsByRangeUnlocked(range_key_start, table_id);
  DecayRecentReplicaCreationsByTableUnlocked(table_id);
  recent_replicas_by_range_[table_id].first[range_key_start]++;
  recent_replicas_by_range_[table_id].second++;
}

double TSDescriptor::RecentReplicaCreations() {
  // NOTE: not a shared lock because of the "Decay" side effect.
  std::lock_guard l(lock_);
  DecayRecentReplicaCreationsUnlocked();
  return recent_replica_creations_;
}

double TSDescriptor::RecentReplicaCreationsByRange(const string& range_key_start,
                                                   const string& table_id) {
  // NOTE: not a shared lock because of the "Decay" side effect.
  std::lock_guard l(lock_);
  DecayRecentReplicaCreationsByRangeUnlocked(range_key_start, table_id);
  DecayRecentReplicaCreationsByTableUnlocked(table_id);
  return recent_replicas_by_range_[table_id].first[range_key_start];
}

double TSDescriptor::RecentReplicaCreationsByTable(const string& table_id) {
  // NOTE: not a shared lock because of the "Decay" side effect.
  std::lock_guard l(lock_);
  DecayRecentReplicaCreationsByTableUnlocked(table_id);
  return recent_replicas_by_range_[table_id].second;
}

Status TSDescriptor::GetRegistration(ServerRegistrationPB* reg,
                                     bool use_external_addr) const {
  shared_lock l(lock_);
  CHECK(registration_) << "No registration";
  CHECK_NOTNULL(reg)->CopyFrom(*registration_);

  if (use_external_addr) {
    // For a tablet server to be reachable from the outside, its registration
    // should have at least one address in rpc_proxy_addresses and one element
    // in proxy_http_addresses, otherwise it might be a misconfiguration.
    //
    // The internal addresses are not exposed to the outside, and addresses
    // advertised by a proxy to outside clients are reported as the only
    // available ones for a tablet server.
    // TODO(aserbin): add condition on reg->http_proxy_addresses_size()
    //                once http_proxy_addresses flag is introduced
    if (PREDICT_FALSE(reg->rpc_proxy_addresses_size() <= 0)) {
      const auto msg = Substitute(
          "server $0 lacks proxy advertised addresses", ToString());
      DCHECK(false) << msg;
      return Status::IllegalState(msg);
    }
    reg->mutable_rpc_addresses()->Swap(reg->mutable_rpc_proxy_addresses());
    reg->clear_rpc_proxy_addresses();

    // TODO(aserbin): uncomment once http_proxy_addresses flag is introduced
    //reg->mutable_http_addresses()->Swap(reg->mutable_http_proxy_addresses());
    //reg->clear_http_proxy_addresses();
  }
  return Status::OK();
}

void TSDescriptor::GetTSInfoPB(TSInfoPB* tsinfo_pb, bool use_external_addr) const {
  shared_lock l(lock_);
  CHECK(registration_);
  const auto& reg = *registration_;
  tsinfo_pb->mutable_rpc_addresses()->CopyFrom(
      use_external_addr ? reg.rpc_proxy_addresses() : reg.rpc_addresses());
  if (reg.has_unix_domain_socket_path()) {
    tsinfo_pb->set_unix_domain_socket_path(reg.unix_domain_socket_path());
  }
  if (location_) {
    tsinfo_pb->set_location(*location_);
  }
}

void TSDescriptor::GetNodeInstancePB(NodeInstancePB* instance_pb) const {
  shared_lock l(lock_);
  instance_pb->set_permanent_uuid(permanent_uuid_);
  instance_pb->set_instance_seqno(latest_seqno_);
}

Status TSDescriptor::ResolveSockaddr(Sockaddr* addr, string* host) const {
  vector<HostPort> hostports;
  {
    shared_lock l(lock_);
    for (const HostPortPB& addr : registration_->rpc_addresses()) {
      hostports.emplace_back(addr.host(), addr.port());
    }
  }

  // Resolve DNS outside the lock.
  HostPort last_hostport;
  vector<Sockaddr> addrs;
  for (const HostPort& hostport : hostports) {
    RETURN_NOT_OK(dns_resolver_->ResolveAddresses(hostport, &addrs));
    if (!addrs.empty()) {
      last_hostport = hostport;
      break;
    }
  }

  if (addrs.empty()) {
    return Status::NetworkError("Unable to find the TS address: ",
                                SecureDebugString(*registration_));
  }

  if (addrs.size() > 1) {
    LOG(WARNING) << "TS address " << last_hostport.ToString()
                  << " resolves to " << addrs.size() << " different addresses. Using "
                  << addrs[0].ToString();
  }
  *addr = addrs[0];
  *host = last_hostport.host();
  return Status::OK();
}

Status TSDescriptor::GetTSAdminProxy(const shared_ptr<rpc::Messenger>& messenger,
                                     shared_ptr<tserver::TabletServerAdminServiceProxy>* proxy) {
  {
    shared_lock l(lock_);
    if (ts_admin_proxy_) {
      *proxy = ts_admin_proxy_;
      return Status::OK();
    }
  }
  Sockaddr addr;
  string host;
  RETURN_NOT_OK(ResolveSockaddr(&addr, &host));

  std::lock_guard l(lock_);
  if (!ts_admin_proxy_) {
    HostPort hp;
    RETURN_NOT_OK(hp.ParseString(host, addr.port()));
    ts_admin_proxy_.reset(new tserver::TabletServerAdminServiceProxy(
        messenger, hp, dns_resolver_));
    ts_admin_proxy_->Init(addr);
  }
  *proxy = ts_admin_proxy_;
  return Status::OK();
}

Status TSDescriptor::GetConsensusProxy(const shared_ptr<rpc::Messenger>& messenger,
                                       shared_ptr<consensus::ConsensusServiceProxy>* proxy) {
  {
    shared_lock l(lock_);
    if (consensus_proxy_) {
      *proxy = consensus_proxy_;
      return Status::OK();
    }
  }
  Sockaddr addr;
  string host;
  RETURN_NOT_OK(ResolveSockaddr(&addr, &host));

  std::lock_guard l(lock_);
  if (!consensus_proxy_) {
    HostPort hp;
    RETURN_NOT_OK(hp.ParseString(host, addr.port()));
    consensus_proxy_.reset(new consensus::ConsensusServiceProxy(
        messenger, hp, dns_resolver_));
    consensus_proxy_->Init(addr);
  }
  *proxy = consensus_proxy_;
  return Status::OK();
}

string TSDescriptor::ToString() const {
  shared_lock l(lock_);
  CHECK(!registration_->rpc_addresses().empty());
  const auto& addr = registration_->rpc_addresses(0);
  return Substitute("$0 ($1:$2)", permanent_uuid_, addr.host(), addr.port());
}
} // namespace master
} // namespace kudu
