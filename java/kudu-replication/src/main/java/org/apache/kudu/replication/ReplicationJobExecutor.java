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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.kudu.connector.ColumnSchemasFactory;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.converter.RowResultRowConverter;
import org.apache.flink.connector.kudu.connector.reader.KuduInputSplit;
import org.apache.flink.connector.kudu.connector.reader.KuduReader;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderIterator;
import org.apache.flink.connector.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.connector.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connector.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.connector.kudu.sink.KuduSink;
import org.apache.flink.connector.kudu.sink.KuduSinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.shaded.com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Class for executing the replication actions.
 */
class ReplicationJobExecutor {
    private ReplicationJobConfig config;
    private final String[] columns = new String[] {"id", "title", "author", "price", "quantity"};
    private final Object[][] booksTableData = {
            {1001, "Java for dummies", "Tan Ah Teck", 11.11, 11},
            {1002, "More Java for dummies", "Tan Ah Teck", 22.22, 22},
            {1003, "More Java for more dummies", "Mohammad Ali", 33.33, 33},
            {1004, "A Cup of Java", "Kumar", 44.44, 44},
            {1005, "A Teaspoon of Java", "Kevin Jones", 55.55, 55}
    };

    private List<Row> booksDataRow;

    ReplicationJobExecutor(ReplicationJobConfig config) {
        this.config = config;
        this.booksDataRow =  Arrays.stream(booksTableData)
            .map(
                row -> {
                    Integer rowId = (Integer) row[0];
                    if (rowId % 2 == 1) {
                        Row values = new Row(5);
                        values.setField(0, row[0]);
                        values.setField(1, row[1]);
                        values.setField(2, row[2]);
                        values.setField(3, row[3]);
                        values.setField(4, row[4]);
                        return values;
                    } else {
                        Row values = new Row(5);
                        values.setField(0, row[0]);
                        values.setField(1, row[1]);
                        values.setField(2, row[2]);
                        return values;
                    }
                })
            .collect(Collectors.toList());
    }

    /**
     * Executes the actual business logic of the replication.
     */
    public void runJob() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<String> kuduSourceStream = env.addSource(
//                new KuduSource(config.getSourceMasterAddresses(), config.getTableName()));

        KuduWriterConfig writerConfig =
                KuduWriterConfig.Builder
                        .setMasters(String.join(",", config.getSinkMasterAddresses()))
                        .setStrongConsistency()
                        .build();

        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), true);

        KuduSink<Row> sink =
                KuduSink.<Row>builder()
                        .setWriterConfig(writerConfig)
                        .setTableInfo(tableInfo)
                        .setOperationMapper(initOperationMapper(columns))
                        .build();


        try {
            SinkWriter<Row> writer = sink.createWriter((Sink.InitContext) null);

            for (Row kuduRow : booksDataRow) {
                writer.write(kuduRow, null);
            }
            writer.close();

            List<Row> rows = readRows(tableInfo, String.join(",", config.getSinkMasterAddresses()));

            env.execute("Kudu Test Job");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }


    // everything below this is just temp helper stuff
    private RowOperationMapper initOperationMapper(String[] cols) {
        return new RowOperationMapper(cols, AbstractSingleOperationMapper.KuduOperation.INSERT);
    }

    public static KuduTableInfo booksTableInfo(String tableName, boolean createIfNotExist) {

        KuduTableInfo tableInfo = KuduTableInfo.forTable(tableName);

        if (createIfNotExist) {
            ColumnSchemasFactory schemasFactory =
                    () -> {
                        List<ColumnSchema> schemas = new ArrayList<>();
                        schemas.add(
                                new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32)
                                        .key(true)
                                        .build());
                        schemas.add(
                                new ColumnSchema.ColumnSchemaBuilder("title", Type.STRING).build());
                        schemas.add(
                                new ColumnSchema.ColumnSchemaBuilder("author", Type.STRING)
                                        .build());
                        schemas.add(
                                new ColumnSchema.ColumnSchemaBuilder("price", Type.DOUBLE)
                                        .nullable(true)
                                        .build());
                        schemas.add(
                                new ColumnSchema.ColumnSchemaBuilder("quantity", Type.INT32)
                                        .nullable(true)
                                        .build());
                        return schemas;
                    };

            tableInfo.createTableIfNotExists(
                    schemasFactory,
                    () ->
                            new CreateTableOptions()
                                    .setNumReplicas(1)
                                    .addHashPartitions(Lists.newArrayList("id"), 2));
        }

        return tableInfo;
    }

    protected List<Row> readRows(KuduTableInfo tableInfo, String masterAddresses) throws Exception {
        KuduReaderConfig readerConfig =
                KuduReaderConfig.Builder.setMasters(masterAddresses).build();
        KuduReader<Row> reader =
                new KuduReader<>(tableInfo, readerConfig, new RowResultRowConverter());

        KuduInputSplit[] splits = reader.createInputSplits(1);
        List<Row> rows = new ArrayList<>();
        for (KuduInputSplit split : splits) {
            KuduReaderIterator<Row> resultIterator = reader.scanner(split.getScanToken());
            while (resultIterator.hasNext()) {
                Row row = resultIterator.next();
                if (row != null) {
                    rows.add(row);
                }
            }
        }
        reader.close();

        return rows;
    }



}
