// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.kudu.replication;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.connector.writer.KuduWriterConfig;
import org.apache.kudu.client.ReplicaSelection;
import org.apache.kudu.client.SessionConfiguration;

public class ReplicationConfigParser {

  public static ReplicationJobConfig parseJobConfig(ParameterTool params) {
    ReplicationJobConfig.Builder builder = ReplicationJobConfig.Builder.newBuilder()
            .setSourceMasterAddresses(params.getRequired("job.sourceMasterAddresses"))
            .setSinkMasterAddresses(params.getRequired("job.sinkMasterAddresses"))
            .setTableName(params.getRequired("job.tableName"));

    if (params.has("job.restoreOwner")) {
      builder.setRestoreOwner(params.getBoolean("job.restoreOwner"));
    }

    if (params.has("job.tableSuffix")) {
      builder.setTableSuffix(params.get("job.tableSuffix"));
    }

    if (params.has("job.disoveryIntervalSeconds")) {
      builder.setDisoveryIntervalSeconds(params.getInt("job.disoveryIntervalSeconds"));
    }

    return builder.build();
  }

  public static KuduReaderConfig parseReaderConfig(ParameterTool params, String masters) {
    KuduReaderConfig.Builder builder = KuduReaderConfig.Builder.setMasters(masters);

    if (params.has("reader.batchSizeBytes")) {
      builder.setBatchSizeBytes(params.getInt("reader.batchSizeBytes"));
    }
    if (params.has("reader.splitSizeBytes")) {
      builder.setSplitSizeBytes(params.getLong("reader.splitSizeBytes"));
    }
    if (params.has("reader.scanRequestTimeout")) {
      builder.setScanRequestTimeout(params.getLong("reader.scanRequestTimeout"));
    }
    if (params.has("reader.prefetching")) {
      builder.setPrefetching(params.getBoolean("reader.prefetching"));
    }
    if (params.has("reader.keepAlivePeriodMs")) {
      builder.setKeepAlivePeriodMs(params.getLong("reader.keepAlivePeriodMs"));
    }
    if (params.has("reader.replicaSelection")) {
      builder.setReplicaSelection(ReplicaSelection.valueOf(params.get("reader.replicaSelection")));
    }

    return builder.build();
  }

  public static KuduWriterConfig parseWriterConfig(ParameterTool params, String masters) {
    KuduWriterConfig.Builder builder = KuduWriterConfig.Builder.setMasters(masters);

    if (params.has("writer.flushMode")) {
      builder.setConsistency(SessionConfiguration.FlushMode.valueOf(params.get("writer.flushMode")));
    }

    if (params.has("writer.operationTimeout")) {
      builder.setOperationTimeout(params.getLong("writer.operationTimeout"));
    }

    if (params.has("writer.maxBufferSize")) {
      builder.setMaxBufferSize(params.getInt("writer.maxBufferSize"));
    }

    if (params.has("writer.flushInterval")) {
      builder.setFlushInterval(params.getInt("writer.flushInterval"));
    }

    if (params.has("writer.ignoreNotFound")) {
      builder.setIgnoreNotFound(params.getBoolean("writer.ignoreNotFound"));
    }

    if (params.has("writer.ignoreDuplicate")) {
      builder.setIgnoreDuplicate(params.getBoolean("writer.ignoreDuplicate"));
    }

    return builder.build();
  }
}
