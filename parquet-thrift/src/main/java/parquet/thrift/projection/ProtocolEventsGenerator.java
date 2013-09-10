package parquet.thrift.projection;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;
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
  List<TProtocol> createdEvents = new ArrayList<TProtocol>();

  public List<TProtocol> createProtocolEventsForField(ThriftField missingField) {
    TProtocol fieldBegin = new ReadFieldBeginProtocol(missingField);
    createdEvents.add(fieldBegin);

    createMissingStuff(missingField.getType());
    //....struct should handle the field begin and field end stuff
    createdEvents.add(READ_FIELD_END);
    return createdEvents;
  }

  private void createMissingStuff(ThriftType fieldType) {
    if (fieldType instanceof ThriftType.StructType) {
      createStruct((ThriftType.StructType) fieldType);
    } else if (fieldType instanceof ThriftType.ListType) {
      System.out.println("no!!!!!!");
      //createList, empty!
    } else if (fieldType instanceof ThriftType.SetType) {
      //createSet, empty!
    } else if (fieldType instanceof ThriftType.MapType) {
      //empty Map
    } else if (fieldType instanceof ThriftType.StringType) {
      createdEvents.add(new StringProtocol("dummy"));
      //primitive
    }
  }

  private void createStruct(ThriftType.StructType fieldType) {
    createdEvents.add(new StructBeginProtocol("struct"));//TODO: name of a field doesn't matter??
    List<ThriftField> children = fieldType.getChildren();
    for (ThriftField child : children) {
      createdEvents.add(new ReadFieldBeginProtocol(child));
      createMissingStuff(child.getType());
      createdEvents.add(READ_FIELD_END);
    }
    createdEvents.add(READ_FIELD_STOP);
    createdEvents.add(READ_STRUCT_END);
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
}

class StructBeginProtocol extends ParquetProtocol {
  String structName;

  StructBeginProtocol(String structName) {
    super("readStructBegin()");
    this.structName = structName;
  }

  @Override
  public TStruct readStructBegin() throws TException {
    return new TStruct(structName);//TODO: pass in field, and use real name
  }
}
