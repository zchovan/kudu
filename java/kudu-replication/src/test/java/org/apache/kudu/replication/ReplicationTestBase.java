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

import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.connector.writer.KuduWriterConfig;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.*;

import static org.apache.kudu.test.ClientTestUtil.createTableWithOneThousandRows;
import static org.apache.kudu.test.KuduTestHarness.DEFAULT_SLEEP;

public abstract class ReplicationTestBase {

  protected static final String TABLE_NAME = "replication_test_table";

  @Rule
  public final KuduTestHarness sourceHarness = new KuduTestHarness();
  @Rule
  public final KuduTestHarness sinkHarness = new KuduTestHarness();

  protected KuduClient sourceClient;
  protected KuduClient sinkClient;

  @Before
  public void setupClients() {
    this.sourceClient = sourceHarness.getClient();
    this.sinkClient = sinkHarness.getClient();
  }

  protected ReplicationJobConfig createDefaultJobConfig() {
    ReplicationJobConfig jobConfig = ReplicationJobConfig.Builder.newBuilder()
      .setSourceMasterAddresses(sourceHarness.getMasterAddressesAsString())
      .setSinkMasterAddresses(sinkHarness.getMasterAddressesAsString())
      .setTableName(TABLE_NAME)
      .setDisoveryIntervalSeconds(2)
      .build();
    return jobConfig;
  }

  protected KuduReaderConfig createDefaultReaderConfig() {
    KuduReaderConfig readerConfig = KuduReaderConfig.Builder
      .setMasters(sourceHarness.getMasterAddressesAsString())
      .build();
    return readerConfig;
  }

  protected KuduWriterConfig createDefaultWriterConfig() {
    KuduWriterConfig writerConfig = KuduWriterConfig.Builder
      .setMasters(sinkHarness.getMasterAddressesAsString())
      .build();
    return writerConfig;
  }


  // create datagen methods:
  // - insert/update/delete all types, auto increment

  protected void createAndFillSourceTable() throws Exception {
    createTableWithOneThousandRows(
            sourceHarness.getAsyncClient(), TABLE_NAME, 10, DEFAULT_SLEEP);
  }
}