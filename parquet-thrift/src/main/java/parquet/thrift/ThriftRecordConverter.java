/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.thrift;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;

import parquet.io.Binary;
import parquet.io.ParquetDecodingException;
import parquet.io.convert.Converter;
import parquet.io.convert.GroupConverter;
import parquet.io.convert.PrimitiveConverter;
import parquet.io.convert.RecordConverter;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.Type;
import parquet.thrift.struct.ThriftField;
import parquet.thrift.struct.ThriftField.Requirement;
import parquet.thrift.struct.ThriftType;
import parquet.thrift.struct.ThriftType.ListType;
import parquet.thrift.struct.ThriftType.MapType;
import parquet.thrift.struct.ThriftType.StructType;
import parquet.thrift.struct.ThriftTypeID;

/**
 * converts the columnar events into a Thrift protocol.
 *
 * @author Julien Le Dem
 *
 * @param <T>
 */
public class ThriftRecordConverter<T> extends RecordConverter<T> {

  final ParquetProtocol readFieldEnd = new ParquetProtocol("readFieldEnd()") {
    @Override
    public void readFieldEnd() throws TException {
    }
  };

  /**
   * Handles field events creation by wrapping the converter for the actual type
   *
   * @author Julien Le Dem
   *
   */
  class PrimitiveFieldHandler extends PrimitiveConverter {

    private final PrimitiveConverter delegate;
    private final List<TProtocol> events;
    private final ParquetProtocol readFieldBegin;

    private void startField() {
      events.add(readFieldBegin);
    }

    private void endField() {
      events.add(readFieldEnd);
    }

    public PrimitiveFieldHandler(PrimitiveConverter delegate, final ThriftField field, List<TProtocol> events) {
      this.delegate = delegate;
      this.events = events;
      this.readFieldBegin = new ParquetProtocol("readFieldBegin()") {
        @Override
        public TField readFieldBegin() throws TException {
          return new TField(field.getName(), field.getType().getType().getThriftType(), field.getFieldId());
        }
      };
    }

    @Override
    public void addBinary(Binary value) {
      startField();
      delegate.addBinary(value);
      endField();
    }

    @Override
    public void addBoolean(boolean value) {
      startField();
      delegate.addBoolean(value);
      endField();
    }

    @Override
    public void addDouble(double value) {
      startField();
      delegate.addDouble(value);
      endField();
    }

    @Override
    public void addFloat(float value) {
      startField();
      delegate.addFloat(value);
      endField();
    }

    @Override
    public void addInt(int value) {
      startField();
      delegate.addInt(value);
      endField();
    }

    @Override
    public void addLong(long value) {
      startField();
      delegate.addLong(value);
      endField();
    }

  }

  /**
   * Handles field events creation by wrapping the converter for the actual type
   *
   * @author Julien Le Dem
   *
   */
  class GroupFieldhandler extends GroupConverter {

    private final GroupConverter delegate;
    private final List<TProtocol> events;
    private final ParquetProtocol readFieldBegin;

    public GroupFieldhandler(GroupConverter delegate, final ThriftField field, List<TProtocol> events) {
      this.delegate = delegate;
      this.events = events;
      this.readFieldBegin = new ParquetProtocol("readFieldBegin()") {
        @Override
        public TField readFieldBegin() throws TException {
          return new TField(field.getName(), field.getType().getType().getThriftType(), field.getFieldId());
        }
      };
    }

    @Override
    public Converter getConverter(int fieldIndex) {
       return delegate.getConverter(fieldIndex);
    }

    @Override
    public void start() {
      events.add(readFieldBegin);
      delegate.start();
    }

    @Override
    public void end() {
      delegate.end();
      events.add(readFieldEnd);
    }

  }

  interface Counter {

    void startCounting();
    int getCount();

  }

  /**
   * counts the instances created to use in List/Set/Map that need to inform of the element count in the protocol
   *
   * @author Julien Le Dem
   *
   */
  class GroupCounter extends GroupConverter implements Counter {

    private final GroupConverter delegate;
    private int count;

    public GroupCounter(GroupConverter delegate) {
      this.delegate = delegate;
    }

    @Override
    public Converter getConverter(int fieldIndex) {
       return delegate.getConverter(fieldIndex);
    }

    @Override
    public void start() {
      delegate.start();
    }

    @Override
    public void end() {
      delegate.end();
      ++ count;
    }

    @Override
    public void startCounting() {
      count = 0;
    }

    @Override
    public int getCount() {
      return count;
    }

  }

  /**
   * counts the instances created to use in List/Set/Map that need to inform of the element count in the protocol
   *
   * @author Julien Le Dem
   *
   */
  class PrimitiveCounter extends PrimitiveConverter implements Counter {

    private final PrimitiveConverter delegate;
    private int count;

    public PrimitiveCounter(PrimitiveConverter delegate) {
      this.delegate = delegate;
    }

    @Override
    public void addBinary(Binary value) {
      delegate.addBinary(value);
      ++ count;
    }

    @Override
    public void addBoolean(boolean value) {
      delegate.addBoolean(value);
      ++ count;
    }

    @Override
    public void addDouble(double value) {
      delegate.addDouble(value);
      ++ count;
    }

    @Override
    public void addFloat(float value) {
      delegate.addFloat(value);
      ++ count;
    }

    @Override
    public void addInt(int value) {
      delegate.addInt(value);
      ++ count;
    }

    @Override
    public void addLong(long value) {
      delegate.addLong(value);
      ++ count;
    }

    @Override
    public void startCounting() {
      count = 0;
    }

    @Override
    public int getCount() {
      return count;
    }

  }

  /**
   * convert primitive values
   *
   * @author Julien Le Dem
   *
   */
  class FieldPrimitiveConverter extends PrimitiveConverter {

    private final List<TProtocol> events;

    public FieldPrimitiveConverter(List<TProtocol> events, ThriftField field) {
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
    }

    @Override
    public void addDouble(final double value) {
      events.add(new ParquetProtocol() {
        @Override
        public double readDouble() throws TException {
          return value;
        }
      });
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
    }

    @Override
    public void addLong(final long value) {
      events.add(new ParquetProtocol() {
        @Override
        public long readI64() throws TException {
          return value;
        }
      });
    }

  }

  /**
   * converts Binary into String
   * @author Julien Le Dem
   *
   */
  class FieldStringConverter extends PrimitiveConverter {

    private final List<TProtocol> events;

    public FieldStringConverter(List<TProtocol> events, ThriftField field) {
      this.events = events;
    }

    @Override
    public void addBinary(final Binary value) {
      events.add(new ParquetProtocol() {
        @Override
        public String readString() throws TException {
          return value.toStringUsingUTF8();
        }
      });
    }

  }

  /**
   * convert to Maps
   * @author Julien Le Dem
   *
   */
  class MapConverter extends GroupConverter {

    private final GroupCounter child;
    private final List<TProtocol> mapEvents = new ArrayList<TProtocol>();
    private final List<TProtocol> parentEvents;
    private final byte keyType;
    private final byte valueType;

    MapConverter(List<TProtocol> parentEvents, GroupType parquetSchema, ThriftField field) {
      this.parentEvents = parentEvents;
      if (parquetSchema.getFieldCount() != 1) {
        throw new IllegalArgumentException("maps have only one field. " + parquetSchema + " size = " + parquetSchema.getFieldCount());
      }
      Type nestedType = parquetSchema.getType(0);
      final ThriftField key = ((MapType)field.getType()).getKey();
      keyType = key.getType().getType().getThriftType();
      final ThriftField value = ((MapType)field.getType()).getValue();
      valueType = value.getType().getType().getThriftType();
      child = new GroupCounter(new MapKeyValueConverter(mapEvents, nestedType, key, value));
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
      child.startCounting();
      // we'll add the events in the end as we need to count elements
    }

    final ParquetProtocol readMapEnd = new ParquetProtocol("readMapEnd()") {
      @Override
      public void readMapEnd() throws TException {
      }
    };

    @Override
    public void end() {
      final int count = child.getCount();
      parentEvents.add(new ParquetProtocol("readMapBegin()") {
        @Override
        public TMap readMapBegin() throws TException {
          return new TMap(keyType, valueType, count);
        }
      });
      parentEvents.addAll(mapEvents);
      mapEvents.clear();
      parentEvents.add(readMapEnd);
    }

  }

  /**
   * converts to a key value pair (in maps)
   * @author Julien Le Dem
   *
   */
  class MapKeyValueConverter extends GroupConverter {

    private Converter keyConverter;
    private Converter valueConverter;

    public MapKeyValueConverter(List<TProtocol> mapEvents, Type nestedType,
        ThriftField key, ThriftField value) {
      keyConverter = newConverter(mapEvents, nestedType.asGroupType().getType(0), key);
      valueConverter = newConverter(mapEvents, nestedType.asGroupType().getType(1), value);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      switch (fieldIndex) {
      case 0:
        return keyConverter;
      case 1:
        return valueConverter;
      default:
        throw new IllegalArgumentException("only key (0) and value (1) are supported. got " + fieldIndex);
      }
    }

    @Override
    public void start() {
    }

    @Override
    public void end() {
    }

  }

  /**
   * converts to a Set
   * @author Julien Le Dem
   *
   */
  class SetConverter extends CollectionConverter {

    final ParquetProtocol readSetEnd = new ParquetProtocol("readSetEnd()") {
      @Override
      public void readSetEnd() throws TException {
      }
    };

    private final List<TProtocol> parentEvents;

    public SetConverter(List<TProtocol> parentEvents, GroupType parquetSchema, ThriftField field) {
      super(parentEvents, parquetSchema, ((ListType)field.getType()).getValues());
      this.parentEvents = parentEvents;
    }

    @Override
    void collectionStart(final int count, final byte type) {
      parentEvents.add(new ParquetProtocol("readSetBegin()") {
        @Override
        public TSet readSetBegin() throws TException {
          return new TSet(type, count);
        }
      });
    }

    @Override
    void collectionEnd() {
      parentEvents.add(readSetEnd);
    }

  }

  /**
   * converts to a List
   * @author Julien Le Dem
   *
   */
  class ListConverter extends CollectionConverter {

    final ParquetProtocol readListEnd = new ParquetProtocol("readListEnd()") {
      @Override
      public void readListEnd() throws TException {
      }
    };

    private final List<TProtocol> parentEvents;

    ListConverter(List<TProtocol> parentEvents, GroupType parquetSchema, ThriftField field) {
      super(parentEvents, parquetSchema, ((ListType)field.getType()).getValues());
      this.parentEvents = parentEvents;
    }

    @Override
    void collectionStart(final int count, final byte type) {
      parentEvents.add(new ParquetProtocol("readListBegin()") {
        @Override
        public TList readListBegin() throws TException {
          return new TList(type, count);
        }
      });
    }

    @Override
    void collectionEnd() {
      parentEvents.add(readListEnd);
    }

  }

  /**
   * Base class to convert List and Set which basically work the same
   * @author Julien Le Dem
   *
   */
  abstract class CollectionConverter extends GroupConverter {

    private final Converter child;
    private final Counter childCounter;
    private List<TProtocol> listEvents = new ArrayList<TProtocol>();
    private final List<TProtocol> parentEvents;
    private ThriftTypeID valuesType;
    private final Type nestedType;

    CollectionConverter(List<TProtocol> parentEvents, GroupType parquetSchema, ThriftField values) {
      this.parentEvents = parentEvents;
      if (parquetSchema.getFieldCount() != 1) {
        throw new IllegalArgumentException("lists have only one field. " + parquetSchema + " size = " + parquetSchema.getFieldCount());
      }
      nestedType = parquetSchema.getType(0);
      valuesType = values.getType().getType();
      if (nestedType.isPrimitive()) {
        PrimitiveCounter counter = new PrimitiveCounter(newConverter(listEvents, nestedType, values).asPrimitiveConverter());
        child = counter;
        childCounter = counter;
      } else {
        GroupCounter counter = new GroupCounter(newConverter(listEvents, nestedType, values).asGroupConverter());
        child = counter;
        childCounter = counter;
      }
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
      childCounter.startCounting();
      // we'll add the events in the end as we need to count elements
    }

    @Override
    public void end() {
      final int count = childCounter.getCount();
      collectionStart(count, valuesType.getThriftType());
      parentEvents.addAll(listEvents);
      listEvents.clear();
      collectionEnd();
    }

    abstract void collectionStart(int count, byte type);

    abstract void collectionEnd();

  }

  /**
   * converts to Struct
   * @author Julien Le Dem
   *
   */
  class StructConverter extends GroupConverter {

    private final int schemaSize;

    private final Converter[] converters;
    private final StructType thriftType;
    private final String name;
    private final TStruct tStruct;
    private final List<TProtocol> events;

    private StructConverter(List<TProtocol> events, GroupType parquetSchema, ThriftField field) {
      this.events = events;
      this.name = field.getName();
      this.tStruct = new TStruct(name);
      this.thriftType = (StructType)field.getType();
      this.schemaSize = parquetSchema.getFieldCount();
      if (schemaSize != thriftType.getChildren().size()) {
        throw new IllegalArgumentException("schema sizes don't match:\n" + parquetSchema + "\n" + thriftType);
      }
      this.converters = new Converter[this.schemaSize];
      for (int i = 0; i < schemaSize; i++) {
        ThriftField childField = thriftType.getChildren().get(i);
        Type type = parquetSchema.getType(i);
        if (type.isPrimitive()) {
          converters[i] = new PrimitiveFieldHandler(newConverter(events, type, childField).asPrimitiveConverter(), childField, events);
        } else {
          converters[i] = new GroupFieldhandler(newConverter(events, type, childField).asGroupConverter(), childField, events);
        }
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
    }

  }
  private final ThriftReader<T> thriftReader;
  private final ParquetReadProtocol protocol;
  private final GroupConverter structConverter;
  private List<TProtocol> rootEvents = new ArrayList<TProtocol>();

  /**
   *
   * @param thriftReader the class responsible for instantiating the final object and read from the protocol
   * @param name the name of that type ( the thrift class simple name)
   * @param parquetSchema the schema for the incoming columnar events
   * @param thriftType the thrift type descriptor
   */
  public ThriftRecordConverter(ThriftReader<T> thriftReader, String name, MessageType parquetSchema, ThriftType.StructType thriftType) {
    super();
    this.thriftReader = thriftReader;
    this.protocol = new ParquetReadProtocol();
    this.structConverter = new StructConverter(rootEvents, parquetSchema, new ThriftField(name, (short)0, Requirement.REQUIRED, thriftType));
  }

  /**
   *
   * {@inheritDoc}
   * @see parquet.io.convert.RecordConverter#getCurrentRecord()
   */
  @Override
  public T getCurrentRecord() {
    try {
      protocol.addAll(rootEvents);
      rootEvents.clear();
      return thriftReader.readOneRecord(protocol);
    } catch (TException e) {
      throw new ParquetDecodingException("Could not read thrift object from protocol", e);
    }
  }

  /**
   *
   * {@inheritDoc}
   * @see parquet.io.convert.RecordConverter#getRootConverter()
   */
  @Override
  public GroupConverter getRootConverter() {
    return structConverter;
  }

  private Converter newConverter(List<TProtocol> events, Type type, ThriftField field) {
    switch (field.getType().getType()) {
    case LIST:
      return new ListConverter(events, type.asGroupType(), field);
    case SET:
      return new SetConverter(events, type.asGroupType(), field);
    case MAP:
      return new MapConverter(events, type.asGroupType(), field);
    case STRUCT:
      return new StructConverter(events, type.asGroupType(), field);
    case STRING:
      return new FieldStringConverter(events, field);
    default:
      return new FieldPrimitiveConverter(events, field);
    }
  }
}
