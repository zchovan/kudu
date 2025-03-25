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
import org.apache.flink.connector.kudu.source.KuduSource;
import org.apache.flink.connector.kudu.sink.KuduSinkBuilder;
import org.apache.flink.connector.kudu.source.KuduSourceBuilder;
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
    public void runJob() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KuduSink<Row> kuduSink = new KuduSinkBuilder<Row>()
                .setWriterConfig(getWriterConfig())
                .setTableInfo(getTableInfo())
                .setOperationMapper(getRowOperationMapper())
                .build();

        KuduSource<Row> kuduSource = new KuduSourceBuilder<Row>()
            .setReaderConfig(getReaderConfig())
            .setTableInfo(getTableInfo())
            .setRowResultConverter(new RowResultRowConverter())
            .build();

        env.fromSource(kuduSource, WatermarkStrategy.noWatermarks(), "KuduSource")
                .returns(TypeInformation.of(Row.class))
                .sinkTo(kuduSink);


        env.execute("Kudu Flink job");

    }

    private KuduWriterConfig getWriterConfig() {
        return KuduWriterConfig.Builder
            .setMasters(String.join(",", config.getSinkMasterAddresses()))
            .build();
    }

    // TODO: figure out the mechanism to get this auto populated
    // for now just got the column names hardcoed ~.~
    private RowOperationMapper getRowOperationMapper() {
        return new RowOperationMapper(new String[]{"key", "column1_i", "column2_i", "column3_s", "column4_b"}, AbstractSingleOperationMapper.KuduOperation.UPSERT);

    }

    private KuduReaderConfig getReaderConfig() {
        return KuduReaderConfig.Builder
                .setMasters(String.join(",", config.getSourceMasterAddresses()))
                .build();
    }

    private KuduTableInfo getTableInfo() {
        return KuduTableInfo.forTable(config.getTableName());
    }

}
