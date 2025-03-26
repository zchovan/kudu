package org.apache.kudu.replication;

import org.apache.flink.connector.kudu.connector.ColumnSchemasFactory;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;

import java.util.List;

public class CustomColumnSchemas implements ColumnSchemasFactory {
    private final ReplicationJobConfig config;

    public CustomColumnSchemas(ReplicationJobConfig config) {
        this.config = config;
    }

    @Override
    public List<ColumnSchema> getColumnSchemas() {
        try {
            KuduClient sourceClient = new KuduClient.KuduClientBuilder(String.join(",", config.getSourceMasterAddresses())).build();
            KuduTable table = sourceClient.openTable(config.getTableName());
            sourceClient.close();
            return table.getSchema().getColumns();
        } catch (KuduException e) {
            throw new RuntimeException(e);
        }
    }
}