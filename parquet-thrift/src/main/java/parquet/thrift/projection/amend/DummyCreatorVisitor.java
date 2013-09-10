package parquet.thrift.projection.amend;

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
      dummyEvents.add(new ReadFieldBeginProtocol(child));
      child.getType().accept(this);
      dummyEvents.add(ProtocolEventsGenerator.READ_FIELD_END);
    }
    dummyEvents.add(ProtocolEventsGenerator.READ_FIELD_STOP);
    dummyEvents.add(ProtocolEventsGenerator.READ_STRUCT_END);

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
    dummyEvents.add(new StringProtocol("dummy"));
  }

  public List<ParquetProtocol> getEvents() {
    return dummyEvents;
  }
}
