package org.apache.kudu.replication;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.kudu.test.ClientTestUtil.createTableWithOneThousandRows;
import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.apache.kudu.test.KuduTestHarness.DEFAULT_SLEEP;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestReplication {
    private static final Logger LOG = LoggerFactory.getLogger(TestReplication.class);
    private static final String TABLE_NAME = "replication_test_table";

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
    public void TestBasicReplication() {
        //Setup source table
        try {
            createTableWithOneThousandRows(
                    this.sourceHarness.getAsyncClient(), TABLE_NAME, 32 * 1024, DEFAULT_SLEEP);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            fail(e.getMessage());
        }

        ReplicationJobConfig config = new ReplicationJobConfig();

        config.setSourceMasterAddresses(Arrays.asList(sourceHarness.getMasterAddressesAsString().split(",")));
        config.setSinkMasterAddresses(Arrays.asList(sinkHarness.getMasterAddressesAsString().split(",")));
        config.setTableName(TABLE_NAME);

        ReplicationJobExecutor executor = new ReplicationJobExecutor(config);
        executor.runJob();

        assertTrue(true);
    }

    @Test
    @KuduTestHarness.EnableKerberos
    public void TestReplicationWithKerberos() {
        try {
            createTableWithOneThousandRows(
                    this.sourceHarness.getAsyncClient(), TABLE_NAME, 32 * 1024, DEFAULT_SLEEP);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            fail(e.getMessage());
        }

        ReplicationJobConfig config = new ReplicationJobConfig();

        config.setSourceMasterAddresses(Arrays.asList(sourceHarness.getMasterAddressesAsString().split(",")));
        config.setSinkMasterAddresses(Arrays.asList(sinkHarness.getMasterAddressesAsString().split(",")));
        config.setTableName(TABLE_NAME);

        ReplicationJobExecutor executor = new ReplicationJobExecutor(config);
        executor.runJob();

        assertTrue(true);
    }
}
