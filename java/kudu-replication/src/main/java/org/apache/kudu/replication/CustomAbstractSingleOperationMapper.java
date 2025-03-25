package org.apache.kudu.replication;

import org.apache.flink.connector.kudu.connector.writer.KuduOperationMapper;
import org.apache.flink.types.Row;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class CustomAbstractSingleOperationMapper implements KuduOperationMapper<Row> {

    private final KuduOperation operation;


    public CustomAbstractSingleOperationMapper(KuduOperation operation) {
        this.operation = operation;
    }

    public Optional<Operation> createBaseOperation(Row input, KuduTable table) {
        if (operation == null) {
            throw new UnsupportedOperationException(
                    "createBaseOperation must be overridden if no operation specified in constructor");
        }
        switch (operation) {
            case INSERT:
                return Optional.of(table.newInsert());
            case UPDATE:
                return Optional.of(table.newUpdate());
            case UPSERT:
                return Optional.of(table.newUpsert());
            case DELETE:
                return Optional.of(table.newDelete());
            default:
                throw new RuntimeException("Unknown operation " + operation);
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

    /** Kudu operation types. */
    public enum KuduOperation {
        INSERT,
        UPDATE,
        UPSERT,
        DELETE
    }
}