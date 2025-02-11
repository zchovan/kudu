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
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.connector.kudu.connector.ColumnSchemasFactory;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.converter.RowResultRowConverter;
import org.apache.flink.connector.kudu.connector.reader.KuduInputSplit;
import org.apache.flink.connector.kudu.connector.reader.KuduReader;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderIterator;
import org.apache.flink.connector.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.connector.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connector.kudu.connector.writer.RowDataUpsertOperationMapper;
import org.apache.flink.connector.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.connector.kudu.sink.KuduSink;
import org.apache.flink.connector.kudu.sink.KuduSinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.shaded.com.google.common.collect.Lists;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Class for executing the replication actions.
 */
class ReplicationJobExecutor {
    private ReplicationJobConfig config;

    ReplicationJobExecutor(ReplicationJobConfig config) {
        this.config = config;
    }

    /**
     * Executes the actual business logic of the replication.
     */
    public void runJob() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KuduTableInfo tableInfo = KuduTableInfo.forTable(config.getTableName());




        KuduReaderConfig readerConfig =
                KuduReaderConfig.Builder
                        .setMasters(String.join(",", config.getSinkMasterAddresses()))
                        .build();

        // TODO(zchovan): replace this with new implementation, when available
        KuduSource source = new KuduSource(config.getSourceMasterAddresses(), config.getTableName());

        KuduWriterConfig writerConfig =
                KuduWriterConfig.Builder
                        .setMasters(String.join(",", config.getSinkMasterAddresses()))
                        .setStrongConsistency()
                        .build();


        RowOperationMapper operationMapper = new RowOperationMapper(
                tableInfo.getSchema()
                        .getColumns()
                        .stream()
                        .map(c -> c.getName())
                        .toArray(size -> new String[size]), AbstractSingleOperationMapper.KuduOperation.UPSERT);


        KuduSink<Row> sink =
                KuduSink.<Row>builder()
                        .setTableInfo(tableInfo)
                        .setOperationMapper(operationMapper)
                        .setWriterConfig(writerConfig)
                        .build();

        env.fromSource(source)
                .sinkTo(sink)
                .execute();
        // 1. Establish connection to both source and sink clusters. Fail the replication
        // job early if there are any problems (e.g. connectivity, auth).
        // Check that the table(s) to be replicated do exist on the source and sink clusters.
        // If either is missing the job fails.

        // 3. Check if the table(s) have been bootstrapped (the target table is not empty).

        // 4a. If bootstrapping is needed, start a full replication.

        // 4b. If bootstrapping is not needed, start from the latest replicated timestamp.

        // 5. If the replication was successful, update and persist the latest replicated timestamp.





    }

}
