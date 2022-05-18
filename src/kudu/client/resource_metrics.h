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
#ifndef KUDU_CLIENT_RESOURCE_METRICS_H
#define KUDU_CLIENT_RESOURCE_METRICS_H

// NOTE: using stdint.h instead of cstdint because this file might be
//       processed by a compiler lacking C++11 support (Kudu C++ client API
//       still keeps C++98 compatibility)
#include <stdint.h>

#include <map>
#include <string>

#include "kudu/util/kudu_export.h"

#ifdef KUDU_HEADERS_NO_STUBS
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#else
#include "kudu/client/stubs.h"
#endif

namespace kudu {
namespace client {

/// @brief A generic catalog of simple metrics.
class KUDU_EXPORT ResourceMetrics {
 public:
  ResourceMetrics();

  ~ResourceMetrics();

  /// @return A map that contains all metrics, its key is the metric name
  ///   and its value is corresponding metric count.
  std::map<std::string, int64_t> Get() const;

  /// Increment/decrement the given metric.
  ///
  /// @param [in] name
  ///   The name of the metric.
  /// @param [in] amount
  ///   The amount to increment the metric
  ///   (negative @c amount corresponds to decrementing the metric).
  ATTRIBUTE_DEPRECATED("This function will become private in a future release.")
  void Increment(const std::string& name, int64_t amount);

  /// Get current count for the specified metric.
  ///
  /// @param [in] name
  ///   Name of the metric in question.
  /// @return The metric's current count.
  int64_t GetMetric(const std::string& name) const;

 private:
  friend class KuduScanner;
  friend class KuduSession;
  class KUDU_NO_EXPORT Data;
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(ResourceMetrics);
};

} // namespace client
} // namespace kudu

#endif
