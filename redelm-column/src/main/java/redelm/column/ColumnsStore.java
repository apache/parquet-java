package redelm.column;

import java.util.Collection;

abstract public class ColumnsStore {

  abstract public ColumnReader getColumnReader(ColumnDescriptor path);

  abstract public ColumnWriter getColumnWriter(ColumnDescriptor path);

  abstract public Collection<ColumnReader> getColumnReaders();

}
