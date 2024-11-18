/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kudu.replication;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kudu.client.*;
import org.apache.kudu.util.HybridTimeUtil;

public class KuduSource extends RichSourceFunction<String> {
    private final List<String> kuduMasters;
    private final String tableName;
    private transient KuduClient client;
    private volatile boolean isRunning = true;
    private final long scanIntervalMillis = 5000; // Set your desired scan interval
    private long lastTimestamp = 0; // Track the last timestamp
    private boolean isFirstScan = true; // Track if it's the first scan

    public KuduSource(List<String> masterAddresses, String tableName) {
        this.kuduMasters = masterAddresses;
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            client = new KuduClient.KuduClientBuilder(kuduMasters).build();
            System.out.println("Kudu client initialized.");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to initialize Kudu client.", e);
        }
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            KuduTable table = client.openTable(tableName);
            long currentMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
            long currentTimestamp = HybridTimeUtil.physicalAndLogicalToHTTimestamp(currentMicros, 0) + 1;
            List<KuduScanToken> tokens;
            if (isFirstScan) {
                // Perform the initial snapshot scan
                tokens = client.newScanTokenBuilder(table)
                        .snapshotTimestampRaw(currentTimestamp)
                        .readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT)
                        .build();
                for (KuduScanToken token : tokens) {
                    KuduScanner scanner = KuduScanToken.deserializeIntoScanner(token.serialize(), client);
                    while (scanner.hasMoreRows()) {
                        RowResultIterator results = scanner.nextRows();
                        while (results.hasNext()) {
                            RowResult result = results.next();
                            String rowData = result.rowToString();
                            System.out.println("Fetched row: " + rowData);
                            synchronized (ctx.getCheckpointLock()) {
                                ctx.collect(rowData);
                            }
                        }
                    }
                }
                isFirstScan = false; // Mark the first scan as complete
            } else {
                // Perform the timestamp-based diff scan
                tokens = client.newScanTokenBuilder(table)
                        .readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT)
                        .diffScan(lastTimestamp, currentTimestamp)
                        .build();
                for (KuduScanToken token : tokens) {
                    KuduScanner scanner = KuduScanToken.deserializeIntoScanner(token.serialize(), client);
                    while (scanner.hasMoreRows()) {
                        RowResultIterator results = scanner.nextRows();
                        while (results.hasNext()) {
                            RowResult result = results.next();
                            String rowData = result.rowToString();
                            System.out.println("Fetched row: " + rowData);
                            synchronized (ctx.getCheckpointLock()) {
                                ctx.collect(rowData);
                            }
                        }
                    }
                }
            }
            // Update the last timestamp for the next diff scan
            lastTimestamp = currentTimestamp;
            // Sleep for the defined interval before the next scan
            Thread.sleep(scanIntervalMillis);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        if (client != null) {
            try {
                client.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
        }
    }
}
