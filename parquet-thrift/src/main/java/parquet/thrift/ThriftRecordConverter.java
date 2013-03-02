package parquet.thrift;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;

import parquet.bytes.BytesUtils;
import parquet.io.convert.Converter;
import parquet.io.convert.GroupConverter;
import parquet.io.convert.PrimitiveConverter;
import parquet.io.convert.RecordConverter;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;
import parquet.thrift.ThriftRecordConverter.Handler;
import parquet.thrift.struct.ThriftField;
import parquet.thrift.struct.ThriftField.Requirement;
import parquet.thrift.struct.ThriftType;
import parquet.thrift.struct.ThriftType.ListType;
import parquet.thrift.struct.ThriftType.StructType;
import parquet.thrift.struct.ThriftTypeID;

public class ThriftRecordConverter<T> extends RecordConverter<T> {

  public static class Handler {

    private int count;
    private final ThriftField field;
    private final List<TProtocol> parentEvents;

    private Handler(List<TProtocol> parentEvents, ThriftField field) {
      super();
      this.parentEvents = parentEvents;
      this.field = field;
    }

    public void startCounting() {
      count = 0;
    }

    public int getCount() {
      return count;
    }

    public void countOne() {
      ++ count;
    }

    public void endField() {
      parentEvents.add(new ParquetProtocol("readFieldEnd()") {
        @Override
        public void readFieldEnd() throws TException {
        }
      });
    }

    public void startField() {
      parentEvents.add(new ParquetProtocol("readFieldBegin()") {
        @Override
        public TField readFieldBegin() throws TException {
          return new TField(field.getName(), TType.LIST, field.getFieldId());
        }
      });
    }
  }

  private interface ThriftConverter {

    public Handler getHandler();

  }

  private abstract static class ThriftPrimitiveConverter extends PrimitiveConverter implements ThriftConverter {

    private Handler handler;

    private ThriftPrimitiveConverter(List<TProtocol> parentEvents, ThriftField field) {
      super();
      this.handler = new Handler(parentEvents, field);
    }

    @Override
    public Handler getHandler() {
      return handler;
    }
  }

  private abstract static class ThriftGroupConverter extends GroupConverter implements ThriftConverter {

    private Handler handler;

    private ThriftGroupConverter(List<TProtocol> parentEvents, ThriftField field) {
      this(new Handler(parentEvents, field));
    }

    private ThriftGroupConverter(Handler handler) {
      this.handler = handler;
    }

    @Override
    public Handler getHandler() {
      return handler;
    }
  }

  public class FieldPrimitiveConverter extends ThriftPrimitiveConverter {

    private final List<TProtocol> events;

    public FieldPrimitiveConverter(List<TProtocol> events, ThriftField field) {
      super(events, field);
      this.events = events;
    }

    @Override
    public void addBoolean(final boolean value) {
      events.add(new ParquetProtocol() {
        @Override
        public boolean readBool() throws TException {
          return value;
        }
      });
      getHandler().countOne();
    }

    @Override
    public void addDouble(final double value) {
      events.add(new ParquetProtocol() {
        @Override
        public double readDouble() throws TException {
          return value;
        }
      });
      getHandler().countOne();
    }

    @Override
    public void addFloat(final float value) {
      // TODO: check thrift has no float
      events.add(new ParquetProtocol() {
        @Override
        public double readDouble() throws TException {
          return value;
        }
      });
      getHandler().countOne();
    }

    @Override
    public void addInt(final int value) {
      // TODO: check smaller types
      events.add(new ParquetProtocol() {
        @Override
        public int readI32() throws TException {
          return value;
        }
      });
      getHandler().countOne();
    }

    @Override
    public void addLong(final long value) {
      events.add(new ParquetProtocol() {
        @Override
        public long readI64() throws TException {
          return value;
        }
      });
      getHandler().countOne();
    }

  }

  public class FieldStringConverter extends ThriftPrimitiveConverter {

    private final List<TProtocol> events;

    public FieldStringConverter(List<TProtocol> events, ThriftField field) {
      super(events, field);
      this.events = events;
    }

    @Override
    public void addBinary(final byte[] value) {
      events.add(new ParquetProtocol() {
        @Override
        public String readString() throws TException {
          return new String(value, BytesUtils.UTF8);
        }
      });
      getHandler().countOne();
    }

  }

  public class MapConverter extends ThriftGroupConverter {

    public MapConverter(List<TProtocol> events, GroupType asGroupType, ThriftField field) {
      super(events, field);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      throw new UnsupportedOperationException("NYI");
    }

    @Override
    public void start() {
      throw new UnsupportedOperationException("NYI");
    }

    @Override
    public void end() {
      throw new UnsupportedOperationException("NYI");
    }

  }

  public class SetConverter extends ThriftGroupConverter {

    public SetConverter(List<TProtocol> events, GroupType asGroupType, ThriftField field) {
      super(events, field);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      throw new UnsupportedOperationException("NYI");
    }

    @Override
    public void start() {
      throw new UnsupportedOperationException("NYI");
    }

    @Override
    public void end() {
      throw new UnsupportedOperationException("NYI");
    }

  }

  public class ListConverter extends ThriftGroupConverter {

    private final Converter child;
    private final Handler childHandler;
    private final ThriftField field;
    private List<TProtocol> listEvents = new ArrayList<TProtocol>();
    private final List<TProtocol> parentEvents;
    private ThriftTypeID valuesType;

    ListConverter(List<TProtocol> parentEvents, GroupType parquetSchema, ThriftField field) {
      super(parentEvents, field);
      this.parentEvents = parentEvents;
      this.field = field;
      if (parquetSchema.getFieldCount() != 1) {
        throw new IllegalArgumentException("lists have only one field. " + parquetSchema + " size = " + parquetSchema.getFieldCount());
      }
      nestedType = parquetSchema.getType(0);
      final ThriftField values = ((ListType)field.getType()).getValues();
      valuesType = values.getType().getType();
      child = newConverter(listEvents, nestedType, values);
      childHandler = ((ThriftConverter)child).getHandler();
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      if (fieldIndex != 0) {
        throw new IllegalArgumentException("lists have only one field. can't reach " + fieldIndex);
      }
      return child;
    }

    @Override
    public void start() {
      childHandler.startCounting();
      // we'll add the events in the end as we need to count elements
    }

    final ParquetProtocol readListEnd = new ParquetProtocol("readListEnd()") {
      @Override
      public void readListEnd() throws TException {
      }
    };
    private Type nestedType;

    private int count() {
      return childHandler.getCount();
    }

    @Override
    public void end() {
      final int count = count();
      getHandler().startField();
      parentEvents.add(new ParquetProtocol("readListBegin()") {
        @Override
        public TList readListBegin() throws TException {
          return new TList(valuesType.getThriftType(), count);
        }
      });
      parentEvents.addAll(listEvents);
      parentEvents.add(readListEnd);
      getHandler().endField();
      getHandler().countOne();
    }

  }

  public class StructConverter extends ThriftGroupConverter {

    private final GroupType parquetSchema;
    private final int schemaSize;

    private final Converter[] converters;
    private final StructType thriftType;
    private final String name;
    private final TStruct tStruct;
    private final List<TProtocol> events;

    private StructConverter(List<TProtocol> events, GroupType parquetSchema, ThriftField field) {
      this(events, parquetSchema, field, false);
    }

    private StructConverter(List<TProtocol> events, GroupType parquetSchema, ThriftField field, boolean isRoot) {
      super(isRoot ? new Handler(events, field) : new Handler(events, field) {
        @Override
        public void startField() {
        }
        @Override
        public void endField() {
        }
      });
      this.events = events;
      this.name = field.getName();
      this.tStruct = new TStruct(name);
      this.parquetSchema = parquetSchema;
      this.thriftType = (StructType)field.getType();
      this.schemaSize = parquetSchema.getFieldCount();
      if (schemaSize != thriftType.getChildren().size()) {
        throw new IllegalArgumentException("schema sizes don't match:\n" + parquetSchema + "\n" + thriftType);
      }
      this.converters = new Converter[this.schemaSize];
      for (int i = 0; i < schemaSize; i++) {
        ThriftField childField = thriftType.getChildren().get(i);
        Type type = parquetSchema.getType(i);
        converters[i] = newConverter(events, type, childField);
      }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converters[fieldIndex];
    }

    final ParquetProtocol readStructBegin = new ParquetProtocol("readStructBegin()") {
      @Override
      public TStruct readStructBegin() throws TException {
        return tStruct;
      }
    };

    @Override
    public void start() {
      events.add(readStructBegin);
    }

    private final ParquetProtocol readFieldStop = new ParquetProtocol("readFieldBegin() => STOP") {
      final TField stop = new TField("", TType.STOP, (short)0);
      @Override
      public TField readFieldBegin() throws TException {
        return stop;
      }
    };
    private final ParquetProtocol readStructEnd = new ParquetProtocol("readStructEnd()") {
      @Override
      public void readStructEnd() throws TException {
      }
    };

    @Override
    public void end() {
      events.add(readFieldStop);
      events.add(readStructEnd);
      getHandler().countOne();
    }

  }
  private final ThriftReader<T> thriftReader;
  private final ParquetReadProtocol protocol;
  private final GroupConverter structConverter;
  private List<TProtocol> rootEvents = new ArrayList<TProtocol>();

  public ThriftRecordConverter(ThriftReader<T> thriftReader, String name, MessageType parquetSchema, ThriftType.StructType thriftType) {
    super();
    this.thriftReader = thriftReader;
    this.protocol = new ParquetReadProtocol();
    this.structConverter = new StructConverter(rootEvents, parquetSchema, new ThriftField(name, (short)0, Requirement.REQUIRED, thriftType), true);
  }

  @Override
  public T getCurrentRecord() {
    try {
      protocol.addAll(rootEvents);
      return thriftReader.readOneRecord(protocol);
    } catch (TException e) {
      //TODO: cleanup
      throw new RuntimeException(e);
    }
  }

  @Override
  public GroupConverter getRootConverter() {
    return structConverter;
  }

  private <C extends Converter & ThriftConverter> C newConverter(List<TProtocol> events, Type type, ThriftField field) {
    switch (field.getType().getType()) {
    case LIST:
      return (C)new ListConverter(events, type.asGroupType(), field);
    case SET:
      return (C)new SetConverter(events, type.asGroupType(), field);
    case MAP:
      return (C)new MapConverter(events, type.asGroupType(), field);
    case STRUCT:
      return (C)new StructConverter(events, type.asGroupType(), field);
    case STRING:
      return (C)new FieldStringConverter(events, field);
//      case BINARY:
//        primitiveConverters[i] = new FieldByteArrayConverter(i);
//        break;
    default:
      return (C)new FieldPrimitiveConverter(events, field);
    }
  }
}
