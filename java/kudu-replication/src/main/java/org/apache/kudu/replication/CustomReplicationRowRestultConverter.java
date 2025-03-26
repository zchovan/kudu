package org.apache.kudu.replication;

import org.apache.flink.connector.kudu.connector.converter.RowResultConverter;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.kudu.Schema;
import org.apache.kudu.client.RowResult;

public class CustomReplicationRowRestultConverter implements RowResultConverter<Row> {
    public CustomReplicationRowRestultConverter() {
    }

    public Row convert(RowResult row) {
        Schema schema = row.getColumnProjection();
        Row values = new Row(schema.getColumnCount());
        if (schema.hasIsDeleted()) {
            values.setKind((RowKind.DELETE));
        }
        schema.getColumns().forEach((column) -> {
            String name = column.getName();
            int pos = schema.getColumnIndex(name);
            if (!row.isNull(name)) {
                values.setField(pos, row.getObject(name));
            }
        });
        return values;
    }
}
