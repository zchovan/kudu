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

import static org.apache.kudu.test.ClientTestUtil.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.kudu.client.*;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestReplication extends ReplicationTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestReplication.class);


  @Test
  public void testBasicReplication() throws Exception {
    //Setup source table
    createAndFillSourceTable();

    ReplicationEnvProvider executor = new ReplicationEnvProvider(
            createDefaultJobConfig(),
            createDefaultReaderConfig(),
            createDefaultWriterConfig());

    executor.getEnv().executeAsync();
    Thread.sleep(3000);

    KuduTable sinkTable = sinkClient.openTable(TABLE_NAME);
    while (1000 != countRowsInTable(sinkTable)) {
      Thread.sleep(1000);
    }


//    insertAutoIncrementingTableWithOneThousandRows(this.sourceHarness.getAsyncClient(),
//        TABLE_NAME, 1000, 10, DEFAULT_SLEEP);

    // TEST DELETE
//    KuduTable sourceTable = sourceClient.openTable(config.getTableName());
//    KuduScanner scanner = sourceClient.newScannerBuilder(sourceTable).build();
//    KuduSession session = sourceClient.newSession();
//    ArrayList<RowResult> resultList = new ArrayList<>();
//    while (scanner.hasMoreRows()) {
//      RowResultIterator results = scanner.nextRows();
//      while (results.hasNext()) {
//        RowResult result = results.next();
//        resultList.add(result);
//      }
//    }
//    for (RowResult result : resultList) {
//      Delete delete = sourceTable.newDelete();
//      PartialRow row = delete.getRow();
//      row.addInt("key", result.getInt("key"));
//      row.addLong("auto_incrementing_id", result.getLong("auto_incrementing_id"));
//      row.addInt("column1_i", result.getInt("column1_i"));
//      row.addInt("column2_i", result.getInt("column2_i"));
//      row.addString("column3_s", result.getString("column3_s"));
//      row.addBoolean("column4_b", result.getBoolean("column4_b"));
//      session.apply(delete);
//      session.flush();
//    }
//    sinkTable = sinkClient.openTable(TABLE_NAME);
//    while (0 != countRowsInTable(sinkTable)) {
//      Thread.sleep(1000);
//      System.out.println("Row count: " + countRowsInTable(sinkTable));
//    }

  }

  @Test
  public void testSourceTableMissing() {
    assertTrue(true);
  }

  @Test
  public void testSinkTableMissing() {
    assertTrue(true);
  }

  @Test
  public void testTableSchemaDoNotMatch() {
    assertTrue(true);
  }

  @Test
  public void testSourceClusterUnavailable() {
    assertTrue(true);
  }

  @Test
  public void testSinkClusterUnavailable() {
    assertTrue(true);
  }

  @Test
  public void testRestartFailedJob() {
    assertTrue(true);
  }

  @Test
  public void testReplicatingAlreadyBootstrappedTable() {
    assertTrue(true);
  }

  @Test
  public void testReplicatingMultipleTables() {
    assertTrue(true);
  }


}
