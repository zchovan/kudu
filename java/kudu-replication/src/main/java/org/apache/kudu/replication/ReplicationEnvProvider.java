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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connector.kudu.sink.KuduSink;
import org.apache.flink.connector.kudu.sink.KuduSinkBuilder;
import org.apache.flink.connector.kudu.source.KuduSource;
import org.apache.flink.connector.kudu.source.enumerator.KuduSourceEnumeratorState;
import org.apache.flink.connector.kudu.source.split.KuduSourceSplit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.kudu.replication.wrappedsource.MetricWrappedKuduSource;

import java.time.Duration;

/**
 * Class for executing the replication actions.
 */
class ReplicationEnvProvider {
  private ReplicationJobConfig jobConfig;
  private KuduReaderConfig readerConfig;
  private KuduWriterConfig writerConfig;

  ReplicationEnvProvider(ReplicationJobConfig jobConfig, KuduReaderConfig readerConfig, KuduWriterConfig writerConfig) {
    this.jobConfig = jobConfig;
    this.readerConfig = readerConfig;
    this.writerConfig = writerConfig;
  }

  /**
   * Executes the actual business logic of the replication.
   */
  public StreamExecutionEnvironment getEnv() throws Exception {

    try (ReplicationTableInitializer tableInitializer = new ReplicationTableInitializer(jobConfig)) {
      tableInitializer.createTableIfNotExists();
    }

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    KuduSink<Row> kuduSink = new KuduSinkBuilder<Row>()
        .setWriterConfig(writerConfig)
        .setTableInfo(KuduTableInfo.forTable(jobConfig.getSinkTableName()))
        .setOperationMapper(new CustomReplicationOperationMapper())
        .build();

    KuduSource<Row> kuduSource = KuduSource.<Row>builder()
        .setReaderConfig(readerConfig)
        .setTableInfo(KuduTableInfo.forTable(jobConfig.getTableName()))
        .setRowResultConverter(new CustomReplicationRowRestultConverter())
        .setBoundedness(Boundedness.CONTINUOUS_UNBOUNDED)
        .setDiscoveryPeriod(Duration.ofSeconds(jobConfig.getDisoveryIntervalSeconds()))
        .build();

    Source<Row, KuduSourceSplit, KuduSourceEnumeratorState> wrappedSource =
            new MetricWrappedKuduSource<>(kuduSource);

    env.fromSource(wrappedSource, WatermarkStrategy.noWatermarks(), "KuduSource")
        .returns(TypeInformation.of(Row.class))
        .sinkTo(kuduSink);

    return env;
  }

}


