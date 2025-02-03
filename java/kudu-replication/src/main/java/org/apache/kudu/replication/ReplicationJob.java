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

package org.apache.kudu.replication;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kudu.sink.KuduSink;

import java.util.ArrayList;
import java.util.Collections;

/**
 * This class is used to submit Kudu Replication Jobs into a Flink Cluster. This is a wrapper for convenience.
 */
public class ReplicationJob {
    static ReplicationJobConfig config;

    /**
     * Generic entry point of the replication job.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // Read parameters from standard args, then convert them into a ReplicationJobConfig object.
        ParameterTool parameters = ParameterTool.fromArgs(args);

        config.setSourceMasterAddresses(new ArrayList<>(
                Collections.singletonList(parameters.get("sourceMasterAddresses", "127.0.0.1:8764"))));
        config.setSinkMasterAddresses(new ArrayList<>(
                Collections.singletonList(parameters.get("sinkMasterAddresses", "127.0.0.1:8764"))));
        config.setTableName(parameters.get("tableName", "test_table"));

        ReplicationJobExecutor replicationJobExecutor = new ReplicationJobExecutor(config);
        replicationJobExecutor.runJob();
    }
}

