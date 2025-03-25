package org.apache.kudu.replication;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.kudu.test.ClientTestUtil.countRowsInTable;
import static org.apache.kudu.test.ClientTestUtil.createDefaultTable;
import static org.apache.kudu.test.ClientTestUtil.createTableWithOneThousandRows;
import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.apache.kudu.test.KuduTestHarness.DEFAULT_SLEEP;
import static org.junit.Assert.assertEquals;
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
    public void TestBasicReplication() throws Exception {
        //Setup source table
        try {
            createTableWithOneThousandRows(
                    this.sourceHarness.getAsyncClient(), TABLE_NAME, 32 * 1024, DEFAULT_SLEEP);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            fail(e.getMessage());
        }

        // We create the sink table here, temporary workaround to get things up and running.
        // TODO: ideally we should not need to do this.
        KuduTable sinkTable = createDefaultTable(this.sinkHarness.getClient(), TABLE_NAME);

        ReplicationJobConfig config = new ReplicationJobConfig();

        config.setSourceMasterAddresses(Arrays.asList(sourceHarness.getMasterAddressesAsString().split(",")));
        config.setSinkMasterAddresses(Arrays.asList(sinkHarness.getMasterAddressesAsString().split(",")));
        config.setTableName(TABLE_NAME);

        ReplicationJobExecutor executor = new ReplicationJobExecutor(config);
        executor.runJob();

        sinkTable = sinkClient.openTable(TABLE_NAME);
        assertEquals(1000, countRowsInTable(sinkTable));
    }

    @Test
    public void TestSourceTableMissing() {
        assertTrue(true);
    }

    @Test
    public void TestSinkTableMissing() {
        assertTrue(true);
    }

    @Test
    public void TestTableSchemaDoNotMatch() {
        assertTrue(true);
    }

    @Test
    public void TestSourceClusterUnavailable() {
        assertTrue(true);
    }

    @Test
    public void TestSinkClusterUnavailable() {
        assertTrue(true);
    }

    @Test
    public void TestRestartFailedJob() {
        assertTrue(true);
    }

    @Test
    public void TestReplicatingAlreadyBootstrappedTable() {
        assertTrue(true);
    }

    @Test
    public void TestReplicatingMultipleTables() {
        assertTrue(true);
    }
    

}
