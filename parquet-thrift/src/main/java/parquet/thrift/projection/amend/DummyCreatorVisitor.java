package parquet.thrift.projection.amend;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TSet;
import parquet.thrift.ParquetProtocol;
import parquet.thrift.struct.ThriftField;
import parquet.thrift.struct.ThriftType;

import java.util.ArrayList;
import java.util.List;

/**
* Created with IntelliJ IDEA.
* User: tdeng
* Date: 9/10/13
* Time: 10:32 AM
* To change this template use File | Settings | File Templates.
*/
class DummyCreatorVisitor implements ThriftType.TypeVisitor {
  List<ParquetProtocol> dummyEvents= new ArrayList<ParquetProtocol>();
  @Override
  public void visit(ThriftType.MapType mapType) {
     dummyEvents.add(new ParquetProtocol("readMapBegin()") {
       @Override
       public TMap readMapBegin() throws TException {
         return new TMap();
       }
     });
  }

  @Override
  public void visit(final ThriftType.SetType setType) {
    dummyEvents.add(new ParquetProtocol("readSetBegin()") {
      @Override
      public TSet readSetBegin() throws TException {
        return new TSet();
      }
    });
  }


  //TODO, unit tests for thie
  @Override
  public void visit(final ThriftType.ListType listType) {
    dummyEvents.add(new ParquetProtocol("readListBegin()") {
      @Override
      public TList readListBegin() throws TException {
        return new TList();
      }
    });
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void visit(ThriftType.StructType structType) {
    dummyEvents.add(new StructBeginProtocol("struct"));//TODO: name of a field doesn't matter??
    List<ThriftField> children = structType.getChildren();
    for (ThriftField child : children) {
      dummyEvents.add(new ReadFieldBeginProtocol(child));
      child.getType().accept(this);
      dummyEvents.add(ProtocolEventsGenerator.READ_FIELD_END);
    }
    dummyEvents.add(ProtocolEventsGenerator.READ_FIELD_STOP);
    dummyEvents.add(ProtocolEventsGenerator.READ_STRUCT_END);

  }

  //TODO: confirm with julien
  @Override
  public void visit(ThriftType.EnumType enumType) {
    dummyEvents.add(new ParquetProtocol("readI32() enum") {
      @Override
      public int readI32() throws TException {
        return 0;
      }
    });
  }

  @Override
  public void visit(ThriftType.BoolType boolType) {
    dummyEvents.add(new ParquetProtocol("readBool()") {
      @Override
      public boolean readBool() throws TException {
        return false;
      }
    });
  }


  @Override
  public void visit(ThriftType.ByteType byteType) {
    dummyEvents.add(new ParquetProtocol("readByte() int") {
      @Override
      public byte readByte() throws TException {
        return (byte) 0;
      }
    });
  }

  @Override
  public void visit(ThriftType.DoubleType doubleType) {
    dummyEvents.add(new ParquetProtocol("readDouble()") {
      @Override
      public double readDouble() throws TException {
        return 1.0;
      }
    });
  }

  @Override
  public void visit(ThriftType.I16Type i16Type) {
    dummyEvents.add(new ParquetProtocol("readI16()") {
      @Override
      public short readI16() throws TException {
        return (short) 0;
      }
    });
  }

  @Override
  public void visit(ThriftType.I32Type i32Type) {
    dummyEvents.add(new ParquetProtocol("readI32()") {
      @Override
      public int readI32() throws TException {
        return 0;
      }
    });
  }

  @Override
  public void visit(ThriftType.I64Type i64Type) {
    dummyEvents.add(new ParquetProtocol("readI64()") {
      @Override
      public long readI64() throws TException {
        return 1;
      }
    });
  }

  @Override
  public void visit(ThriftType.StringType stringType) {
    dummyEvents.add(new StringProtocol("dummy"));
  }

  public List<ParquetProtocol> getEvents() {
    return dummyEvents;
  }
}
