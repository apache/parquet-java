package parquet.thrift.projection.amend;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;
import parquet.thrift.ParquetProtocol;
import parquet.thrift.struct.ThriftField;
import parquet.thrift.struct.ThriftType;
import parquet.thrift.struct.ThriftTypeID;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Generate events for Parquet Protocol for a Thrift Field. The generated events are used for default values
 * when required fields are missing.
 */
class ProtocolEventsGenerator {
  public static ParquetProtocol READ_FIELD_STOP = new ParquetProtocol("readFieldBegin() => STOP") {
    final TField stop = new TField("", TType.STOP, (short) 0);

    @Override
    public TField readFieldBegin() throws TException {
      return stop;
    }
  };
  public static ParquetProtocol READ_STRUCT_END = new ParquetProtocol("readStructEnd()") {
    @Override
    public void readStructEnd() throws TException {
    }
  };
  public static ParquetProtocol READ_FIELD_END = new ParquetProtocol("readFieldEnd()") {
    @Override
    public void readFieldEnd() throws TException {
    }
  };
  List createdEvents = new ArrayList<TProtocol>();

  public List<TProtocol> createProtocolEventsForField(ThriftField missingField) {
    TProtocol fieldBegin = new ProtocolEventsGenerator.ReadFieldBeginProtocol(missingField);
    createdEvents.add(fieldBegin);

    ProtocolEventsGenerator.DummyCreatorVisitor dummyCreatorvisitor = new ProtocolEventsGenerator.DummyCreatorVisitor();
    missingField.getType().accept(dummyCreatorvisitor);
    //....struct should handle the field begin and field end stuff
    createdEvents.addAll(dummyCreatorvisitor.getEvents());
    createdEvents.add(READ_FIELD_END);
    return createdEvents;
  }

  private static class StringProtocol extends ParquetProtocol {

    private final String str;

    public StringProtocol(String str) {
      super("readString() binary");
      this.str = str;
    }

    @Override
    public String readString() throws TException {
      return "str";
    }

    @Override
    public ByteBuffer readBinary() throws TException {
      return ByteBuffer.wrap("str".getBytes());
    }
  }

  private static class ReadFieldBeginProtocol extends ParquetProtocol {
    private final ThriftField field;
    private final byte thriftType;

    public ReadFieldBeginProtocol(ThriftField missingField) {
      super("readFieldBegin()");
      this.field = missingField;
      this.thriftType =
              missingField.getType().getType() == ThriftTypeID.ENUM ?
                      ThriftTypeID.I32.getThriftType() : // enums are serialized as I32
                      missingField.getType().getType().getThriftType();
    }

    @Override
    public TField readFieldBegin() throws TException {
      return new TField(field.getName(), thriftType, field.getFieldId());
    }
  }

  private class DummyCreatorVisitor implements ThriftType.TypeVisitor {
    List<ParquetProtocol> dummyEvents= new ArrayList<ParquetProtocol>();
    @Override
    public void visit(ThriftType.MapType mapType) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(ThriftType.SetType setType) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(ThriftType.ListType listType) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(ThriftType.StructType structType) {
      dummyEvents.add(new StructBeginProtocol("struct"));//TODO: name of a field doesn't matter??
      List<ThriftField> children = structType.getChildren();
      for (ThriftField child : children) {
        dummyEvents.add(new ProtocolEventsGenerator.ReadFieldBeginProtocol(child));
        child.getType().accept(this);
        dummyEvents.add(READ_FIELD_END);
      }
      dummyEvents.add(READ_FIELD_STOP);
      dummyEvents.add(READ_STRUCT_END);

    }

    @Override
    public void visit(ThriftType.EnumType enumType) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(ThriftType.BoolType boolType) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(ThriftType.ByteType byteType) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(ThriftType.DoubleType doubleType) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(ThriftType.I16Type i16Type) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(ThriftType.I32Type i32Type) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(ThriftType.I64Type i64Type) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(ThriftType.StringType stringType) {
      dummyEvents.add(new ProtocolEventsGenerator.StringProtocol("dummy"));
    }

    public List<ParquetProtocol> getEvents() {
      return dummyEvents;
    }
  }
}
