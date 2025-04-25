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
import org.junit.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestReplicationConfigParser {

  @Test
  public void testJobConfigMissingSourceMasterAddressesThrows() {
    String[] args = {
            "--job.sinkMasterAddresses", "sink1:7051",
            "--job.tableName", "my_table"
    };
    ParameterTool params = ParameterTool.fromArgs(args);

    assertThatThrownBy(() -> ReplicationConfigParser.parseJobConfig(params))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("No data for required key 'job.sourceMasterAddresses'");
  }

  @Test
  public void testJobConfigMissingSinkMasterAddressesThrows() {
    String[] args = {
            "--job.sourceMasterAddresses", "source1:7051",
            "--job.tableName", "my_table"
    };
    ParameterTool params = ParameterTool.fromArgs(args);

    assertThatThrownBy(() -> ReplicationConfigParser.parseJobConfig(params))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("No data for required key 'job.sinkMasterAddresses'");
  }

  @Test
  public void testJobConfigMissingTableNameThrows() {
    String[] args = {
            "--job.sourceMasterAddresses", "source1:7051",
            "--job.sinkMasterAddresses", "sink1:7051"
    };
    ParameterTool params = ParameterTool.fromArgs(args);

    assertThatThrownBy(() -> ReplicationConfigParser.parseJobConfig(params))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("No data for required key 'job.tableName'");
  }

  @Test
  public void testJobConfigAllRequiredParamsPresent() {
    String[] args = {
            "--job.sourceMasterAddresses", "source1:7051",
            "--job.sinkMasterAddresses", "sink1:7051",
            "--job.tableName", "my_table"
    };

    ParameterTool params = ParameterTool.fromArgs(args);
    ReplicationJobConfig config = ReplicationConfigParser.parseJobConfig(params);

    assertEquals("source1:7051", config.getSourceMasterAddresses());
    assertEquals("sink1:7051", config.getSinkMasterAddresses());
    assertEquals("my_table", config.getTableName());
    assertFalse(config.getRestoreOwner());
    assertEquals("", config.getTableSuffix());
  }

  @Test
  public void testReaderConfigWithAllOptionalParams() {
    String[] args = {
            "--reader.batchSizeBytes", "123456",
            "--reader.splitSizeBytes", "987654321",
            "--reader.scanRequestTimeout", "5000",
            "--reader.prefetching", "true",
            "--reader.keepAlivePeriodMs", "60000",
            "--reader.replicaSelection", "CLOSEST_REPLICA"
    };

    ParameterTool params = ParameterTool.fromArgs(args);
    String masters = "source1:7051";

    KuduReaderConfig config = ReplicationConfigParser.parseReaderConfig(params, masters);

    assertEquals(masters, config.getMasters());
    assertEquals(123456, config.getBatchSizeBytes());
    assertEquals(987654321L, config.getSplitSizeBytes());
    assertEquals(5000L, config.getScanRequestTimeout());
    assertTrue(config.isPrefetching());
    assertEquals(60000L, config.getKeepAlivePeriodMs());
    assertEquals(ReplicaSelection.CLOSEST_REPLICA, config.getReplicaSelection());
  }

  @Test
  public void testWriterConfigWithAllOptionalParams() {
    String[] args = {
            "--writer.flushMode", "AUTO_FLUSH_BACKGROUND",
            "--writer.operationTimeout", "10000",
            "--writer.maxBufferSize", "2000",
            "--writer.flushInterval", "1500",
            "--writer.ignoreNotFound", "true",
            "--writer.ignoreDuplicate", "true"
    };

    ParameterTool params = ParameterTool.fromArgs(args);
    String masters = "sink1:7051";

    KuduWriterConfig config = ReplicationConfigParser.parseWriterConfig(params, masters);

    assertEquals(masters, config.getMasters());
    assertEquals(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND, config.getFlushMode());
    assertEquals(10000L, config.getOperationTimeout());
    assertEquals(2000, config.getMaxBufferSize());
    assertEquals(1500, config.getFlushInterval());
    assertTrue(config.isIgnoreNotFound());
    assertTrue(config.isIgnoreDuplicate());
  }
}
