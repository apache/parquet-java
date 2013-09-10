package parquet.thrift.projection.amend;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;
import parquet.thrift.ParquetProtocol;
import parquet.thrift.struct.ThriftField;

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
    TProtocol fieldBegin = new ReadFieldBeginProtocol(missingField);
    createdEvents.add(fieldBegin);

    DummyCreatorVisitor dummyCreatorvisitor = new DummyCreatorVisitor();
    missingField.getType().accept(dummyCreatorvisitor);
    //....struct should handle the field begin and field end stuff
    createdEvents.addAll(dummyCreatorvisitor.getEvents());
    createdEvents.add(READ_FIELD_END);
    return createdEvents;
  }

}
