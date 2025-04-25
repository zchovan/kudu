package org.apache.kudu.replication;

import java.util.ArrayList;
import java.util.List;

import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Partition;
import org.apache.kudu.client.PartitionSchema;
import org.apache.kudu.client.RangePartitionBound;
import org.apache.kudu.client.RangePartitionWithCustomHashSchema;

/**
 * Helper class that initializes the sink-side table for replication.
 * If the table does not exist on the sink, it creates an identical table
 * with the same schema, table parameters, and partitioning scheme as the source.
 */
public class ReplicationTableInitializer implements AutoCloseable {
  private final KuduClient sourceClient;
  private final KuduClient sinkClient;
  private final String sourceTableName;
  private final String sinkTableName;
  private final Boolean restoreOwner;
  private KuduTable sourceTable;

  public ReplicationTableInitializer(ReplicationJobConfig config) {
    sourceClient = new KuduClient.KuduClientBuilder(
            String. join(",", config.getSourceMasterAddresses())).build();
    sinkClient = new KuduClient.KuduClientBuilder(
            String.join(",", config.getSinkMasterAddresses())).build();
    sourceTableName = config.getTableName();
    sinkTableName = config.getSinkTableName();
    restoreOwner = config.getRestoreOwner();
  }

  public void createTableIfNotExists() throws Exception {
    if (!sinkClient.tableExists(sinkTableName)) {
      try {
        sourceTable = sourceClient.openTable(sourceTableName);
        createTableRangePartitionByRangePartition();
      } catch (Exception e) {
        throw new RuntimeException("Failed to create table " + sinkTableName, e);
      }
    }
  }

  private void createTableRangePartitionByRangePartition() throws Exception {
    CreateTableOptions options = getCreateTableOptionsWithoutRangePartitions();
    List<Partition> boundsWithoutHashSchema = sourceTable.getRangePartitions(
        sourceClient.getDefaultAdminOperationTimeoutMs());
    List<RangePartitionWithCustomHashSchema> boundsWithCustomHashSchema =
        getRangeBoundsPartialRowsWithHashSchemas();

    if (!boundsWithoutHashSchema.isEmpty()) {
      options.addRangePartition(boundsWithoutHashSchema.get(0).getDecodedRangeKeyStart(sourceTable),
          boundsWithoutHashSchema.get(0).getDecodedRangeKeyStart(sourceTable));
      sinkClient.createTable(sinkTableName, sourceTable.getSchema(), options);

      boundsWithoutHashSchema.stream().skip(1).forEach(partition -> {
        AlterTableOptions alterOptions = new AlterTableOptions();
        options.addRangePartition(partition.getDecodedRangeKeyStart(sourceTable),
            partition.getDecodedRangeKeyStart(sourceTable));
        try {
          sinkClient.alterTable(sinkTableName, alterOptions);
        } catch (KuduException e) {
          throw new RuntimeException("Failed to alter table: " + sinkTableName, e);
        }
      });

      boundsWithCustomHashSchema.stream().forEach(partition -> {
        AlterTableOptions alterOptions = new AlterTableOptions();
        alterOptions.addRangePartition(partition);
        try {
          sinkClient.alterTable(sinkTableName, alterOptions);
        } catch (KuduException e) {
          throw new RuntimeException("Failed to alter table: " + sinkTableName, e);
        }
      });


    } else if (!boundsWithCustomHashSchema.isEmpty()) {
      options.addRangePartition(boundsWithCustomHashSchema.get(0));
      sinkClient.createTable(sinkTableName, sourceTable.getSchema(), options);

      boundsWithCustomHashSchema.stream().skip(1).forEach(partition -> {
        AlterTableOptions alterOptions = new AlterTableOptions();
        alterOptions.addRangePartition(partition);
        try {
          sinkClient.alterTable(sinkTableName, alterOptions);
        } catch (KuduException e) {
          throw new RuntimeException("Failed to alter table: " + sinkTableName, e);
        }
      });
    }

  }

  private CreateTableOptions getCreateTableOptionsWithoutRangePartitions() {
    CreateTableOptions options = new CreateTableOptions();
    if (restoreOwner) {
      options.setOwner(sourceTable.getOwner());
    }
    options.setComment(sourceTable.getComment());
    options.setNumReplicas(sourceTable.getNumReplicas());
    options.setExtraConfigs(sourceTable.getExtraConfig());
    PartitionSchema partitionSchema = sourceTable.getPartitionSchema();

    List<PartitionSchema.HashBucketSchema> hashBucketSchemas =
        partitionSchema.getHashBucketSchemas();
    for (PartitionSchema.HashBucketSchema hashBucketSchema : hashBucketSchemas) {
      List<String> colNames = getColumnNamesFromColumnIds(hashBucketSchema.getColumnIds());
      options.addHashPartitions(colNames, hashBucketSchema.getNumBuckets());
    }

    PartitionSchema.RangeSchema rangeSchema = partitionSchema.getRangeSchema();
    List<String> colNames = getColumnNamesFromColumnIds(rangeSchema.getColumnIds());
    options.setRangePartitionColumns(colNames);

    return options;
  }

  private List<RangePartitionWithCustomHashSchema> getRangeBoundsPartialRowsWithHashSchemas() {
    List<RangePartitionWithCustomHashSchema> rangeBoundsPartialRowsWithHashSchemas =
        new ArrayList<>();
    PartitionSchema partitionSchema = sourceTable.getPartitionSchema();
    List<PartitionSchema.RangeWithHashSchema> rangesWithHashSchemas =
        partitionSchema.getRangesWithHashSchemas();
    for (PartitionSchema.RangeWithHashSchema rangeWithHashSchema : rangesWithHashSchemas) {
      RangePartitionWithCustomHashSchema partition = new RangePartitionWithCustomHashSchema(
          rangeWithHashSchema.lowerBound, rangeWithHashSchema.upperBound,
          RangePartitionBound.INCLUSIVE_BOUND, RangePartitionBound.EXCLUSIVE_BOUND);

      List<PartitionSchema.HashBucketSchema> hashBucketSchemas = rangeWithHashSchema.hashSchemas;
      for (PartitionSchema.HashBucketSchema hashBucketSchema : hashBucketSchemas) {
        List<String> colNames = getColumnNamesFromColumnIds(hashBucketSchema.getColumnIds());
        partition.addHashPartitions(colNames, hashBucketSchema.getNumBuckets(),
            hashBucketSchema.getSeed());
      }
      rangeBoundsPartialRowsWithHashSchemas.add(partition);
    }
    return rangeBoundsPartialRowsWithHashSchemas;
  }

  private List<String> getColumnNamesFromColumnIds(List<Integer> columnIds) {
    List<String> columnNames = new ArrayList<>();
    for (int id : columnIds) {
      int idx = sourceTable.getSchema().getColumnIndex(id);
      columnNames.add(sourceTable.getSchema().getColumnByIndex(idx).getName());
    }
    return columnNames;
  }

  @Override
  public void close() throws Exception {
    sourceClient.close();
    sinkClient.close();
  }
}
