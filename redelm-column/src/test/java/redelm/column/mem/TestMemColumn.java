package redelm.column.mem;

import redelm.column.ColumnDescriptor;
import redelm.column.ColumnReader;
import redelm.column.ColumnWriter;
import redelm.schema.PrimitiveType.Primitive;

import org.junit.Assert;
import org.junit.Test;

public class TestMemColumn {
  @Test
  public void testMemColumn() throws Exception {
    System.out.println("<<<");
    MemColumnsStore memColumnsStore = new MemColumnsStore(1024);
    ColumnDescriptor path = new ColumnDescriptor(new String[]{"foo", "bar"}, Primitive.INT64);
    ColumnWriter columnWriter = memColumnsStore.getColumnWriter(path);
    columnWriter.write(42, 0, 0);
    ColumnReader columnReader = memColumnsStore.getColumnReader(path);
    System.out.println(memColumnsStore.toString());
    System.out.println("value, r, d");
    while (!columnReader.isFullyConsumed()) {
      Assert.assertEquals(columnReader.getCurrentRepetitionLevel(), 0);
      Assert.assertEquals(columnReader.getCurrentDefinitionLevel(), 0);
      Assert.assertEquals(columnReader.getInt(), 42);
      System.out.println(columnReader.getInt()
          +", "+columnReader.getCurrentRepetitionLevel()
          +", "+columnReader.getCurrentDefinitionLevel());
      columnReader.consume();
    }
    System.out.println(">>>");
  }

  @Test
  public void testMemColumnString() throws Exception {
    System.out.println("<<<");
    MemColumnsStore memColumnsStore = new MemColumnsStore(1024);
    ColumnDescriptor path = new ColumnDescriptor(new String[]{"foo", "bar"}, Primitive.STRING);
    ColumnWriter columnWriter = memColumnsStore.getColumnWriter(path);
    columnWriter.write("42", 0, 0);
    ColumnReader columnReader = memColumnsStore.getColumnReader(path);
    System.out.println(memColumnsStore.toString());
    System.out.println("value, r, d");
    while (!columnReader.isFullyConsumed()) {
      Assert.assertEquals(columnReader.getCurrentRepetitionLevel(), 0);
      Assert.assertEquals(columnReader.getCurrentDefinitionLevel(), 0);
      Assert.assertEquals(columnReader.getString(), "42");
      System.out.println(columnReader.getString()
          +", "+columnReader.getCurrentRepetitionLevel()
          +", "+columnReader.getCurrentDefinitionLevel());
      columnReader.consume();
    }
    System.out.println(">>>");
  }
}
