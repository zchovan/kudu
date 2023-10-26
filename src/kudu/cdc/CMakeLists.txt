# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#########################################
# cdc_service_proto
#########################################

KRPC_GENERATE(
  CDC_KRPC_SRCS CDC_KRPC_HDRS CDC_KRPC_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES cdc_service.proto)
set(CDC_KRPC_LIBS
  consensus_metadata_proto
  krpc
  protobuf
  master_proto
  wire_protocol_proto)
ADD_EXPORTABLE_LIBRARY(cdc_service_proto
  SRCS ${CDC_KRPC_SRCS}
  DEPS ${CDC_KRPC_LIBS}
  NONLINK_DEPS ${CDC_KRPC_TGTS})

#########################################
# cdc_service
#########################################

set(CDC_SRCS
  cdc_service.cc
  cdc_producer.cc)

add_library(cdc ${CDC_SRCS})
target_link_libraries(cdc
  protobuf
  cdc_service_proto
  master_proto
  consensus_proto
  log_proto
  log
  consensus
  krpc
  server_process
  tablet
  kudu_util
  gutil)

#######################################
# Test utility library
#######################################
# if(NOT NO_TESTS)
#   # This code is useful for other tests which use the client, but isn't
#   # part of the client itself (ie we don't want to ship it to customers,
#   # and therefore don't need to worry about export strictness)
#   add_library(kudu_cdc_test_util
#     cdc_test_util.cc)
#   target_link_libraries(kudu_cdc_test_util
#     cdc_service_proto
#     gmock
#     gtest)
# endif()

#########################################
# cdc tests
#########################################

SET_KUDU_TEST_LINK_LIBS(
  cdc
  tserver
  tserver_test_util
  kudu_client # yb::client::YBTableName
  tablet_test_util
  # kudu_cdc_test_util
  ${KUDU_MIN_TEST_LIBS}
)

ADD_KUDU_TEST(cdc_service-test)