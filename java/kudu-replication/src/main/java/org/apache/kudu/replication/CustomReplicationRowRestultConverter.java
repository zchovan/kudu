package org.apache.kudu.replication;

import org.apache.flink.connector.kudu.connector.converter.RowResultConverter;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.apache.kudu.Schema;
import org.apache.kudu.client.RowResult;

/**
 * Custom RowResult converter that sets DELETE if necessary.
 */
public class CustomReplicationRowRestultConverter implements RowResultConverter<Row> {
  public CustomReplicationRowRestultConverter() {
  }

  @Override
  public Row convert(RowResult row) {
    Schema schema = row.getColumnProjection();
    int columnCount = schema.hasIsDeleted() ? schema.getColumnCount() - 1 : schema.getColumnCount();
    Row values = new Row(columnCount);

    int isDeletedIndex = schema.hasIsDeleted() ? schema.getIsDeletedIndex() : -1;

    if (row.hasIsDeleted() && row.isDeleted()) {
      values.setKind(RowKind.DELETE);
    }
    schema.getColumns().forEach(column -> {
      int pos = schema.getColumnIndex(column.getName());
      // Skip the isDeleted column if it exists
      if (pos == isDeletedIndex && row.hasIsDeleted()) {
        return;
      }
      if (!row.isNull(pos)) {
        values.setField(pos, row.getObject(pos));
      }
    });

    return values;
  }
}
