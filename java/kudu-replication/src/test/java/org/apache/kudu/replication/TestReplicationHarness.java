package org.apache.kudu.replication;

import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.junit.Assert.assertTrue;

public class TestReplicationHarness {
    private static final Logger LOG = LoggerFactory.getLogger(TestReplicationHarness.class);
    private static final String TABLE_NAME = "replication_test_table";

    @Rule
    public final KuduTestHarness sourceHarness = new KuduTestHarness();
    @Rule
    public final KuduTestHarness sinkHarness = new KuduTestHarness();

    private KuduClient sourceClient;
    private KuduClient sinkClient;

    @Before
    public void setup()  {
        CreateTableOptions builder = new CreateTableOptions().setNumReplicas(3);
        Schema basicSchema = getBasicSchema();

        this.sourceClient = sourceHarness.getClient();
        this.sinkClient = sinkHarness.getClient();

    }

    @Test
    public void BenchmarkReplicationScenario() {
        ReplicationJobConfig config = new ReplicationJobConfig();

        config.setSourceMasterAddresses(Arrays.asList(sourceHarness.getMasterAddressesAsString().split(",")));
        config.setSinkMasterAddresses(Arrays.asList(sinkHarness.getMasterAddressesAsString().split(",")));
        config.setTableName(TABLE_NAME);

        ReplicationJobExecutor executor = new ReplicationJobExecutor(config);
        executor.runJob();

        assertTrue(true);
    }

}
