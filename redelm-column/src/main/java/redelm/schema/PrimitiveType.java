package redelm.schema;

import redelm.column.ColumnReader;
import redelm.column.ColumnWriter;
import redelm.data.Group;
import redelm.data.GroupValueSource;
import redelm.io.RecordConsumer;

public class PrimitiveType extends Type {
  public static enum Primitive {
    STRING {
      @Override
      public void writeValueToColumn(GroupValueSource parent, String field,
          int index, int r, int d, ColumnWriter columnWriter) {
        columnWriter.write(parent.getString(field, index), r, d);
      }

      @Override
      public void addValueToGroup(Group group, String field,
          ColumnReader value) {
        group.add(field, value.getString());
      }

      @Override
      public String toString(ColumnReader columnReader) {
        return columnReader.getString();
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addString(columnReader.getString());
      }
    },
    INT64 {
      @Override
      public void writeValueToColumn(GroupValueSource parent, String field,
          int index, int r, int d, ColumnWriter columnWriter) {
        columnWriter.write(parent.getInt(field, index), r, d);
      }

      @Override
      public void addValueToGroup(Group group, String field,
          ColumnReader value) {
        group.add(field, value.getInt());
      }

      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getInt());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addInt(columnReader.getInt());
      }
    },
    BOOL {
      @Override
      public void writeValueToColumn(GroupValueSource parent, String field,
          int index, int r, int d, ColumnWriter columnWriter) {
        columnWriter.write(parent.getBool(field, index), r, d);
      }

      @Override
      public void addValueToGroup(Group group, String field,
          ColumnReader value) {
        group.add(field, value.getBool());
      }

      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getBool());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addBoolean(columnReader.getBool());
      }
    },
    BINARY {
      @Override
      public void writeValueToColumn(GroupValueSource parent, String field,
          int index, int r, int d, ColumnWriter columnWriter) {
        columnWriter.write(parent.getBinary(field, index), r, d);
      }

      @Override
      public void addValueToGroup(Group group, String field,
          ColumnReader value) {
        group.add(field, value.getBinary());
      }

      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getBinary());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addBinary(columnReader.getBinary());
      }
    };


    abstract public void writeValueToColumn(GroupValueSource parent, String field, int index, int r, int d, ColumnWriter columnWriter);

    abstract public void addValueToGroup(Group group, String field, ColumnReader value);

    abstract public String toString(ColumnReader columnReader);

    abstract public void addValueToRecordConsumer(RecordConsumer recordConsumer, ColumnReader columnReader);

  }

  private final Primitive primitive;

  public PrimitiveType(Repetition repeatition, Primitive primitive, String name) {
    super(name, repeatition);
    this.primitive = primitive;
  }

  public Primitive getPrimitive() {
    return primitive;
  }

  @Override
  public boolean isPrimitive() {
    return true;
  }

  @Override
  public String toString() {
    return getName() + ": " + primitive;
  }

  @Override
  public String toString(String indent) {
    return indent+getRepetition().name().toLowerCase()+" "+primitive.name().toLowerCase()+" "+getName();
  }

  @Override
  public void accept(TypeVisitor visitor) {
    visitor.visit(this);
  }

}
