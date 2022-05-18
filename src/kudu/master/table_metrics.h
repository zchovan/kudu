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
#ifndef KUDU_MASTER_TABLE_METRICS_H
#define KUDU_MASTER_TABLE_METRICS_H

#include <cstddef>

#include <cstdint>
#include <string>
#include <unordered_set>

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"

namespace kudu {
namespace master {

// The table metrics consist of the LEADER tablet metrics.
//
// The tservers periodically update tablet metrics based on the gflag
// FLAGS_update_tablet_stats_interval_ms. Then each tserver sends its
// own LEADER tablets' metrics to all of the masters through heartbeat
// messages. But only the LEADER master aggregates and exposes these
// metrics. These metrics are pre-replication.
//
// Note: the process is asynchronous, so the data are lagging.
//
// At the same time, there will be fluctuation of metrics possibly if
// the tablet's leadership changes. And if the new LEADER master is
// elected, the metrics may not be accurate until all tservers report,
// and the time window should be FLAGS_heartbeat_interval_ms.
struct TableMetrics {
  explicit TableMetrics(const scoped_refptr<MetricEntity>& entity);

  scoped_refptr<AtomicGauge<uint64_t>> on_disk_size;
  scoped_refptr<AtomicGauge<uint64_t>> live_row_count;
  scoped_refptr<AtomicGauge<uint32_t>> column_count;
  scoped_refptr<AtomicGauge<uint32_t>> schema_version;
  scoped_refptr<AtomicGauge<size_t>> merged_entities_count_of_table;

  void AddTabletNoOnDiskSize(const std::string& tablet_id);
  void DeleteTabletNoOnDiskSize(const std::string& tablet_id);
  bool ContainsTabletNoOnDiskSize(const std::string& tablet_id) const;
  bool TableSupportsOnDiskSize() const;

  void AddTabletNoLiveRowCount(const std::string& tablet_id);
  void DeleteTabletNoLiveRowCount(const std::string& tablet_id);
  bool ContainsTabletNoLiveRowCount(const std::string& tablet_id) const;
  bool TableSupportsLiveRowCount() const;

 private:
  mutable simple_spinlock lock_;
  // Identifiers of tablets that do not support reporting on disk size.
  std::unordered_set<std::string> tablet_ids_no_on_disk_size_;
  // Identifiers of tablets that do not support reporting live row count.
  std::unordered_set<std::string> tablet_ids_no_live_row_count_;
};

} // namespace master
} // namespace kudu

#endif
