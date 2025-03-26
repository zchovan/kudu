package org.apache.kudu.replication;

import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Partition;
import org.apache.kudu.client.PartitionSchema;

import java.util.ArrayList;
import java.util.List;

public class ReplicationTableInitializer {

    static public void createTableIfNotExists(ReplicationJobConfig config) throws KuduException {
        try (KuduClient sourceClient = buildClient(config.getSourceMasterAddresses());
             KuduClient sinkClient = buildClient(config.getSinkMasterAddresses())) {

            String tableName = config.getTableName();

            if (!sinkClient.tableExists(tableName)) {
                KuduTable sourceTable = sourceClient.openTable(tableName);
                Schema schema = sourceTable.getSchema();
                CreateTableOptions options = extractCreateOptions(sourceTable);

                sinkClient.createTable(tableName, schema, options);
            }
        }
    }

    static private KuduClient buildClient(List<String> masterAddresses) {
        return new KuduClient.KuduClientBuilder(String.join(",", masterAddresses)).build();
    }

    // TODO write this function!!
    static private CreateTableOptions extractCreateOptions(KuduTable table) {
        try {
            CreateTableOptions createTableOptions = new CreateTableOptions();

            createTableOptions.setComment(table.getComment());
            createTableOptions.setNumReplicas(table.getNumReplicas());
            createTableOptions.setExtraConfigs(table.getExtraConfig());
            PartitionSchema ps = table.getPartitionSchema();

            // Add hash partitions into the mix
            List<PartitionSchema.HashBucketSchema> hb = ps.getHashBucketSchemas();
            for (PartitionSchema.HashBucketSchema hbSchema : hb) {
                List<String> colNames = new ArrayList<>();
                for (int id : hbSchema.getColumnIds()) {
                    int idx = table.getSchema().getColumnIndex(id);
                    colNames.add(table.getSchema().getColumnByIndex(idx).getName());
                }
                createTableOptions.addHashPartitions(colNames, hbSchema.getNumBuckets());
            }

            PartitionSchema.RangeSchema rangeSchema = ps.getRangeSchema();
            List<String> colNames = new ArrayList<>();
            for (int id : rangeSchema.getColumnIds()) {
                int idx = table.getSchema().getColumnIndex(id);
                colNames.add(table.getSchema().getColumnByIndex(idx).getName());
            }
            createTableOptions.setRangePartitionColumns(colNames);
            List<Partition> ranges = table.getRangePartitions(9999);
            for (Partition partition : ranges) {
                createTableOptions.addRangePartition(partition.getDecodedRangeKeyStart(table), partition.getDecodedRangeKeyEnd(table));
            }

            return createTableOptions;
        } catch (RuntimeException | KuduException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
