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

#include <cstdint>
#include <cstdlib>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/util/int128.h"
#include "kudu/util/random.h"

namespace kudu {

// Writes exactly n random bytes to dest using the parameter Random generator.
// Note RandomString() does not null-terminate its strings, though '\0' could
// be written to dest with the same probability as any other byte.
void RandomString(void* dest, size_t n, Random* rng);

// Same as the above, but returns the string.
std::string RandomString(size_t n, Random* rng);

// Generate a 32-bit random seed from several sources, including timestamp,
// pid & tid.
uint32_t GetRandomSeed32();

// Returns a randomly-selected element from the container.
template <typename Container, typename T, typename Rand>
T SelectRandomElement(const Container& c, Rand* r) {
  CHECK(!c.empty());
  std::vector<T> rand_list;
  ReservoirSample(c, 1, std::set<T>{}, r, &rand_list);
  return rand_list[0];
}

// Returns a randomly-selected subset from the container. The subset will
// include at least 'min_to_return' results, but may contain more.
//
// The results are not stored in a randomized order: the order of results will
// match their order in the input collection.
template <typename Container, typename T, typename Rand>
std::vector<T> SelectRandomSubset(const Container& c, int min_to_return, Rand* r) {
  CHECK_GE(c.size(), min_to_return);
  int num_to_return = min_to_return + r->Uniform(1 + c.size() - min_to_return);
  std::vector<T> rand_list;
  ReservoirSample(c, num_to_return, std::set<T>{}, r, &rand_list);
  return rand_list;
}

// Sample 'k' random elements from the collection 'c' into 'result', taking
// care not to sample any elements that are already present in 'avoid'.
//
// In the case that 'c' has fewer than 'k' elements then all elements in 'c'
// will be selected.
//
// 'c' should be an iterable STL collection such as a vector, set, or list.
// 'avoid' should be an STL-compatible set.
//
// The results are not stored in a randomized order: the order of results will
// match their order in the input collection.
template<class Collection, class Set, class T, typename Rand>
void ReservoirSample(const Collection& c, int k, const Set& avoid,
                     Rand* r, std::vector<T>* result) {
  result->clear();
  result->reserve(k);
  int i = 0;
  for (const T& elem : c) {
    if (ContainsKey(avoid, elem)) {
      continue;
    }
    i++;
    // Fill the reservoir if there is available space.
    if (result->size() < k) {
      result->push_back(elem);
      continue;
    }
    // Otherwise replace existing elements with decreasing probability.
    int j = r->Uniform(i);
    if (j < k) {
      (*result)[j] = elem;
    }
  }
}

// Generates random and unique 32-bit or 64-bit integers that are not present in
// the "avoid" set.
// TODO(bankim): Add support for 128-bit integers by providing hash value for 128-bit data types.
template<typename IntType, class Set, class RNG>
std::unordered_set<IntType> CreateRandomUniqueIntegers(const int num_values,
                                                       const Set& avoid,
                                                       RNG* rand) {
  static_assert(std::is_integral<IntType>::value && (sizeof(IntType) == 4 || sizeof(IntType) == 8),
                "Only 32-bit or 64-bit integers generated");

  std::unordered_set<IntType> result;
  while (result.size() < num_values) {
    const IntType val = GetNextRandom<IntType>(rand);
    if (ContainsKey(avoid, val)) {
      continue;
    }
    if (!ContainsKey(result, val)) {
      result.insert(val);
    }
  }
  return result;
}

// Generates random 32-bit, 64-bit, or 128-bit integers in the [min_val, max_val) range.
template<typename IntType, class RNG>
// When entire range is specified for signed IntType, result of max_val - min_val is -1
// which when converted to unsigned type correctly represents the max value for the
// same unsigned IntType. Variants of Uniform() in RNG work with unsigned types.
// Hence the ASAN suppression.
ATTRIBUTE_NO_SANITIZE_INTEGER
std::vector<IntType> CreateRandomIntegersInRange(const int num_values,
                                                 IntType min_val,
                                                 IntType max_val,
                                                 RNG* rand) {
  // type_traits not defined for 128-bit int data types, hence not checking for integral value.
  static_assert(sizeof(IntType) == 4 || sizeof(IntType) == 8 || sizeof(IntType) == 16,
                "Only 32-bit, 64-bit, or 128-bit integers generated");
  CHECK_LT(min_val, max_val);

  std::vector<IntType> result(num_values);
  for (auto& v : result) {
    v = min_val + GetNextUniformRandom(max_val - min_val, rand);
  }
  return result;
}


} // namespace kudu

