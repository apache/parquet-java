package redelm.io;


import java.util.Arrays;
import java.util.List;

import redelm.column.ColumnDescriptor;
import redelm.column.ColumnReader;
import redelm.column.ColumnWriter;
import redelm.column.ColumnsStore;
import redelm.data.GroupValueSource;
import redelm.schema.Type;


public class PrimitiveColumnIO extends ColumnIO {
//  private static final Logger logger = Logger.getLogger(PrimitiveColumnIO.class.getName());

  private ColumnWriter columnWriter;
  private ColumnIO[] path;
  private ColumnsStore columns;
  private ColumnDescriptor columnDescriptor;

  PrimitiveColumnIO(Type type, GroupColumnIO parent) {
    super(type, parent);
  }

  @Override
  void writeValue(GroupValueSource parent, String field, int index, int r, int d) {
    columnDescriptor.getType().writeValueToColumn(parent, field, index, r, d, columnWriter);
  }

  @Override
  void setLevels(int r, int d, String[] fieldPath, List<ColumnIO> repetition, List<ColumnIO> path, ColumnsStore columns) {
    this.columns = columns;
    super.setLevels(r, d, fieldPath, repetition, path, columns);
    this.columnDescriptor = new ColumnDescriptor(fieldPath, getType().asPrimitiveType().getPrimitive());
    this.columnWriter = columns.getColumnWriter(columnDescriptor);
    this.path = path.toArray(new ColumnIO[path.size()]);
  }

  @Override
  List<String[]> getColumnNames() {
    return Arrays.asList(new String[][] { getFieldPath() });
  }

  @Override
  void writeNull(int r, int d) {
    columnWriter.writeNull(r, d);
  }

  ColumnReader getColumnReader() {
    return columns.getColumnReader(columnDescriptor);
  }

  public ColumnIO[] getPath() {
    return path;
  }

  public boolean isLast(int r) {
    return getLast(r) == this;
  }

  private PrimitiveColumnIO getLast(int r) {
    ColumnIO parent = getParent(r);

    PrimitiveColumnIO last = parent.getLast();
    return last;
  }

  @Override
  PrimitiveColumnIO getLast() {
    return this;
  }

  @Override
  PrimitiveColumnIO getFirst() {
    return this;
  }
  public boolean isFirst(int r) {
    return getFirst(r) == this;
  }

  private PrimitiveColumnIO getFirst(int r) {
    ColumnIO parent = getParent(r);
    return parent.getFirst();
  }

}
