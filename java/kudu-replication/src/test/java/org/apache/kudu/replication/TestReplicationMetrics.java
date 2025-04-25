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
import static org.apache.kudu.test.KuduTestHarness.DEFAULT_SLEEP;
import static org.junit.Assert.fail;

import java.util.*;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.kudu.client.*;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.kudu.test.KuduTestHarness;

public class TestReplicationMetrics extends ReplicationTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestReplication.class);
  private static final String TABLE_NAME = "replication_test_table";
  private static final InMemoryReporter reporter = InMemoryReporter.createWithRetainedMetrics();

  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
          new MiniClusterWithClientResource(
                  new MiniClusterResourceConfiguration.Builder()
                          .setNumberSlotsPerTaskManager(1)
                          .setNumberTaskManagers(1)
                          .setConfiguration(reporter.addToConfiguration(new Configuration()))
                          .build());

  @Rule
  public final KuduTestHarness sourceHarness = new KuduTestHarness();
  @Rule
  public final KuduTestHarness sinkHarness = new KuduTestHarness();

  private KuduClient sourceClient;
  private KuduClient sinkClient;

  @Before
  public void setup()  {
    this.sourceClient = sourceHarness.getClient();
    this.sinkClient = sinkHarness.getClient();
  }


  @Test
  public void testMetrics() throws Exception {
    //Setup source table
    try {
      createAutoIncrementingTableWithOneThousandRows(
              this.sourceHarness.getAsyncClient(), TABLE_NAME, 10, DEFAULT_SLEEP);
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error(e.getMessage());
      fail(e.getMessage());
    }

    ReplicationEnvProvider executor = new ReplicationEnvProvider(
            createDefaultJobConfig(),
            createDefaultReaderConfig(),
            createDefaultWriterConfig());

    JobID jobId = executor.getEnv().executeAsync().getJobID();
    Thread.sleep(1000);

    Map<String, Metric> metrics = reporter.getMetricsByIdentifiers(jobId);
    for (String key : metrics.keySet()) {
      if (key.contains("lastEndTimestamp")) {
        System.out.println("FOUND METRIC: " + key);
        System.out.println(( (Gauge<String>) metrics.get(key)).getValue());
      }
      else if (key.contains("pendingSplits")) {
        System.out.println("FOUND METRIC: " + key);
        System.out.println(( (Gauge<Integer>) metrics.get(key)).getValue());
      }
      else if (key.contains("unassignedSplits")) {
        System.out.println("FOUND METRIC: " + key);
        System.out.println(( (Gauge<Integer>) metrics.get(key)).getValue());
      }

    }
  }
}

