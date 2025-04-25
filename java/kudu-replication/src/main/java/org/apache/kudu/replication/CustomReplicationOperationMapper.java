package org.apache.kudu.replication;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.flink.connector.kudu.connector.writer.KuduOperationMapper;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;

/**
 * Simple operation mapper.
 */
public class CustomReplicationOperationMapper implements KuduOperationMapper<Row> {

  public Optional<Operation> createBaseOperation(Row input, KuduTable table) {
    RowKind kind = input.getKind();
    if (kind == RowKind.DELETE) {
      return Optional.of(table.newDeleteIgnore());
    } else {
      return Optional.of(table.newUpsertIgnore());
    }
  }

  @Override
  public List<Operation> createOperations(Row input, KuduTable table) {
    Optional<Operation> operationOpt = createBaseOperation(input, table);
    if (!operationOpt.isPresent()) {
      return Collections.emptyList();
    }

    Operation operation = operationOpt.get();
    PartialRow partialRow = operation.getRow();

    for (int i = 0; i < input.getArity(); i++) {
      partialRow.addObject(i, input.getField(i));
    }

    return Collections.singletonList(operation);
  }

}