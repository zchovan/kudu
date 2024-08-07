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
# server_base_proto
#########################################

KRPC_GENERATE(
  SERVER_BASE_PROTO_SRCS SERVER_BASE_PROTO_HDRS SERVER_BASE_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES server_base.proto)

add_library(server_base_proto ${SERVER_BASE_PROTO_SRCS} ${SERVER_BASE_PROTO_HDRS})
target_link_libraries(server_base_proto
  krpc
  kudu_common_proto
  protobuf
  rpc_header_proto
  version_info_proto
  wire_protocol_proto)

#########################################
# server_process
#########################################

set(SERVER_PROCESS_SRCS
  default_path_handlers.cc
  diagnostics_log.cc
  generic_service.cc
  glog_metrics.cc
  pprof_path_handlers.cc
  rpcz-path-handler.cc
  rpc_server.cc
  server_base.cc
  server_base_options.cc
  startup_path_handler.cc
  tcmalloc_metrics.cc
  tracing_path_handlers.cc
  webserver.cc
  webserver_options.cc
  webui_util.cc)

add_library(server_process ${SERVER_PROCESS_SRCS})
target_link_libraries(server_process
  clock
  codegen
  gutil
  krpc
  kudu_common
  kudu_fs
  kudu_jwt_util
  kudu_util
  mustache
  server_base_proto
  squeasel)

# This module depends on tcmalloc and profiler directly, so need to make
# sure that they get linked in the right order.
if(${KUDU_TCMALLOC_AVAILABLE})
  target_link_libraries(server_process tcmalloc profiler)
endif()

#######################################
# Unit tests
#######################################

SET_KUDU_TEST_LINK_LIBS(
  kudu_curl_util
  mini_kdc
  server_process
  security_test_util)
if(NOT NO_ROCKSDB)
  ADD_KUDU_TEST_LINK_LIBS(
    rocksdb
    snappy)
endif()

ADD_KUDU_TEST(rpc_server-test)
ADD_KUDU_TEST(webserver-test)
