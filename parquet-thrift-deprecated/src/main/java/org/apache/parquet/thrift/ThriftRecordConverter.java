/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.thrift;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;

import org.apache.parquet.Preconditions;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.thrift.projection.amend.ProtocolEventsAmender;
import org.apache.parquet.thrift.struct.ThriftField;
import org.apache.parquet.thrift.struct.ThriftField.Requirement;
import org.apache.parquet.thrift.struct.ThriftType;
import org.apache.parquet.thrift.struct.ThriftType.EnumType;
import org.apache.parquet.thrift.struct.ThriftType.EnumValue;
import org.apache.parquet.thrift.struct.ThriftType.ListType;
import org.apache.parquet.thrift.struct.ThriftType.MapType;
import org.apache.parquet.thrift.struct.ThriftType.SetType;
import org.apache.parquet.thrift.struct.ThriftType.StructType;
import org.apache.parquet.thrift.struct.ThriftTypeID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * converts the columnar events into a Thrift protocol.
 *
 * @param <T> the Java type of records created by this converter
 */
@Deprecated
public class ThriftRecordConverter<T> extends RecordMaterializer<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ThriftRecordConverter.class);

  public static final String IGNORE_NULL_LIST_ELEMENTS =
      "parquet.thrift.ignore-null-elements";
  private static final boolean IGNORE_NULL_LIST_ELEMENTS_DEFAULT = false;

  final static ParquetProtocol readFieldEnd = new ParquetProtocol("readFieldEnd()") {
    @Override
    public void readFieldEnd() throws TException {
    }
  };
  private final StructType thriftType;

  /**
   * Handles field events creation by wrapping the converter for the actual type
   */
  static class PrimitiveFieldHandler extends PrimitiveConverter {

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
      final byte thriftType =
          field.getType().getType() == ThriftTypeID.ENUM ?
              ThriftTypeID.I32.getThriftType() : // enums are serialized as I32
              field.getType().getType().getThriftType();
      this.readFieldBegin = new ParquetProtocol("readFieldBegin()") {
        @Override
        public TField readFieldBegin() throws TException {
          return new TField(field.getName(), thriftType, field.getFieldId());
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
   */
  static class GroupFieldhandler extends GroupConverter {

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
   */
  static class GroupCounter extends GroupConverter implements Counter {

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
   */
  static class PrimitiveCounter extends PrimitiveConverter implements Counter {

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
   */
  static class FieldPrimitiveConverter extends PrimitiveConverter {

    private final List<TProtocol> events;
    private ThriftTypeID type;

    public FieldPrimitiveConverter(List<TProtocol> events, ThriftField field) {
      this.events = events;
      this.type = field.getType().getType();
    }

    @Override
    public void addBoolean(final boolean value) {
      events.add(new ParquetProtocol("readBool()") {
        @Override
        public boolean readBool() throws TException {
          return value;
        }
      });
    }

    @Override
    public void addDouble(final double value) {
      events.add(new ParquetProtocol("readDouble()") {
        @Override
        public double readDouble() throws TException {
          return value;
        }
      });
    }

    @Override
    public void addFloat(final float value) {
      // TODO: check thrift has no float
      events.add(new ParquetProtocol("readDouble() float") {
        @Override
        public double readDouble() throws TException {
          return value;
        }
      });
    }

    @Override
    public void addInt(final int value) {
      // TODO: make subclass per type
      switch (type) {
      case BYTE:
        events.add(new ParquetProtocol("readByte() int") {
          @Override
          public byte readByte() throws TException {
            return (byte)value;
          }
        });
        break;
      case I16:
        events.add(new ParquetProtocol("readI16()") {
          @Override
          public short readI16() throws TException {
            return (short)value;
          }
        });
        break;
      case I32:
        events.add(new ParquetProtocol("readI32()") {
          @Override
          public int readI32() throws TException {
            return value;
          }
        });
        break;
        default:
          throw new UnsupportedOperationException("not convertible type " + type);
      }
    }

    @Override
    public void addLong(final long value) {
      events.add(new ParquetProtocol("readI64()") {
        @Override
        public long readI64() throws TException {
          return value;
        }
      });
    }

  }

  /**
   * converts Binary into String
   */
  static class FieldStringConverter extends PrimitiveConverter {

    private final List<TProtocol> events;

    public FieldStringConverter(List<TProtocol> events, ThriftField field) {
      this.events = events;
    }

    @Override
    public void addBinary(final Binary value) {
      events.add(new ParquetProtocol("readString() binary") {
        @Override
        public String readString() throws TException {
          return value.toStringUsingUTF8();
        }
        @Override
        public ByteBuffer readBinary() throws TException {
          return value.toByteBuffer();
        }
      });
    }

  }

  /**
   * converts Binary into Enum
   */
   static class FieldEnumConverter extends PrimitiveConverter {

    private final List<TProtocol> events;
    private final Map<Binary, Integer> enumLookup = new HashMap<Binary, Integer>();
    private final ThriftField field;

    public FieldEnumConverter(List<TProtocol> events, ThriftField field) {
      this.events = events;
      this.field = field;
      final Iterable<EnumValue> values = ((EnumType)field.getType()).getValues();
      for (EnumValue enumValue : values) {
        enumLookup.put(Binary.fromString(enumValue.getName()), enumValue.getId());
      }
    }

    @Override
    public void addBinary(final Binary value) {
      final Integer id = enumLookup.get(value);

      if (id == null) {
        throw new ParquetDecodingException("Unrecognized enum value: "
            + value.toStringUsingUTF8()
            + " known values: "
            + enumLookup
            + " in " + this.field);
      }

      events.add(new ParquetProtocol("readI32() enum") {
        @Override
        public int readI32() throws TException {
          return id;
        }
      });
    }

  }

  /**
   * convert to Maps
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
   */
  class SetConverter extends CollectionConverter {

    final ParquetProtocol readSetEnd = new ParquetProtocol("readSetEnd()") {
      @Override
      public void readSetEnd() throws TException {
      }
    };

    private final List<TProtocol> parentEvents;

    public SetConverter(List<TProtocol> parentEvents, GroupType parquetSchema, ThriftField field) {
      super(parentEvents, parquetSchema, ((SetType)field.getType()).getValues());
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
   */
  abstract class CollectionConverter extends GroupConverter {

    private ElementConverter elementConverter = null;
    private final Converter child;
    private final Counter childCounter;
    private List<TProtocol> listEvents = new ArrayList<TProtocol>();
    private final List<TProtocol> parentEvents;
    private ThriftTypeID valuesType;

    CollectionConverter(List<TProtocol> parentEvents, GroupType parquetSchema, ThriftField values) {
      this.parentEvents = parentEvents;
      if (parquetSchema.getFieldCount() != 1) {
        throw new IllegalArgumentException("lists have only one field. " + parquetSchema + " size = " + parquetSchema.getFieldCount());
      }
      Type repeatedType = parquetSchema.getType(0);
      valuesType = values.getType().getType();
      if (ThriftSchemaConverter.isListElementType(repeatedType, values)) {
        if (repeatedType.isPrimitive()) {
          PrimitiveCounter counter = new PrimitiveCounter(newConverter(listEvents, repeatedType, values).asPrimitiveConverter());
          child = counter;
          childCounter = counter;
        } else {
          GroupCounter counter = new GroupCounter(newConverter(listEvents, repeatedType, values).asGroupConverter());
          child = counter;
          childCounter = counter;
        }
      } else {
        this.elementConverter = new ElementConverter(parquetSchema.getName(),
            listEvents, repeatedType.asGroupType(), values);
        GroupCounter counter = new GroupCounter(elementConverter);
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
      int count = childCounter.getCount();
      if (elementConverter != null) {
        count -= elementConverter.getNullElementCount();
      }
      collectionStart(count, valuesType.getThriftType());
      parentEvents.addAll(listEvents);
      listEvents.clear();
      collectionEnd();
    }

    abstract void collectionStart(int count, byte type);

    abstract void collectionEnd();

  }

  class ElementConverter extends GroupConverter {

    private Converter elementConverter;
    private List<TProtocol> listEvents;
    private List<TProtocol> elementEvents;
    private int nullElementCount;

    public ElementConverter(String listName, List<TProtocol> listEvents,
                            GroupType repeatedType, ThriftField thriftElement) {
      this.listEvents = listEvents;
      this.elementEvents = new ArrayList<TProtocol>();
      Type elementType = repeatedType.getType(0);
      if (elementType.isRepetition(Type.Repetition.OPTIONAL)) {
        if (ignoreNullElements) {
          LOG.warn("List " + listName +
              " has optional elements: null elements are ignored.");
        } else {
          throw new ParquetDecodingException("Cannot read list " + listName +
              " with optional elements: set " + IGNORE_NULL_LIST_ELEMENTS +
              " to ignore nulls.");
        }
      }
      elementConverter = newConverter(elementEvents, elementType, thriftElement);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      Preconditions.checkArgument(
          fieldIndex == 0, "Illegal field index: %s", fieldIndex);
      return elementConverter;
    }

    @Override
    public void start() {
      elementEvents.clear();
    }

    @Override
    public void end() {
      if (elementEvents.size() > 0) {
        listEvents.addAll(elementEvents);
      } else {
        nullElementCount += 1;
      }
    }

    public int getNullElementCount() {
      return nullElementCount;
    }
  }

  /**
   * converts to Struct
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
      this.converters = new Converter[this.schemaSize];
      List<ThriftField> thriftChildren = thriftType.getChildren();
      for (int i = 0; i < schemaSize; i++) {
        Type schemaType = parquetSchema.getType(i);
        String fieldName = schemaType.getName();
        ThriftField matchingThrift = null;
        for (ThriftField childField: thriftChildren) {
          String thriftChildName = childField.getName();
          if (thriftChildName != null && thriftChildName.equalsIgnoreCase(fieldName)) {
            matchingThrift = childField;
            break;
          }
        }
        if (matchingThrift == null) {
        	// this means the file did not contain that field
          // it will never be populated in this instance
          // other files might populate it
        	continue;
        }
        if (schemaType.isPrimitive()) {
        	converters[i] = new PrimitiveFieldHandler(newConverter(events, schemaType, matchingThrift).asPrimitiveConverter(), matchingThrift, events);
        } else {
        	converters[i] = new GroupFieldhandler(newConverter(events, schemaType, matchingThrift).asGroupConverter(), matchingThrift, events);
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
  private GroupConverter structConverter;
  private List<TProtocol> rootEvents = new ArrayList<TProtocol>();
  private boolean missingRequiredFieldsInProjection = false;
  private boolean ignoreNullElements = IGNORE_NULL_LIST_ELEMENTS_DEFAULT;

  /**
   * This is for compatibility only.
   * @param thriftReader the class responsible for instantiating the final object and read from the protocol
   * @param name the name of that type ( the thrift class simple name)
   * @param requestedParquetSchema the schema for the incoming columnar events
   * @param thriftType the thrift type descriptor
   * @deprecated will be removed in 2.x
   */
  @Deprecated
  public ThriftRecordConverter(ThriftReader<T> thriftReader, String name, MessageType requestedParquetSchema, ThriftType.StructType thriftType) {
    this(thriftReader, name, requestedParquetSchema, thriftType, null);
  }

  /**
   *
   * @param thriftReader the class responsible for instantiating the final object and read from the protocol
   * @param name the name of that type ( the thrift class simple name)
   * @param requestedParquetSchema the schema for the incoming columnar events
   * @param thriftType the thrift type descriptor
   * @param conf a Configuration
   */
  public ThriftRecordConverter(ThriftReader<T> thriftReader, String name, MessageType requestedParquetSchema, ThriftType.StructType thriftType, Configuration conf) {
    super();
    this.thriftReader = thriftReader;
    this.protocol = new ParquetReadProtocol();
    this.thriftType = thriftType;
    if (conf != null) {
      this.ignoreNullElements = conf.getBoolean(
          IGNORE_NULL_LIST_ELEMENTS,
          IGNORE_NULL_LIST_ELEMENTS_DEFAULT);
    }
    MessageType fullSchema = ThriftSchemaConverter.convertWithoutProjection(thriftType);
    missingRequiredFieldsInProjection = hasMissingRequiredFieldInGroupType(requestedParquetSchema, fullSchema);
    this.structConverter = new StructConverter(rootEvents, requestedParquetSchema, new ThriftField(name, (short)0, Requirement.REQUIRED, thriftType));
  }

  private boolean hasMissingRequiredFieldInGroupType(GroupType requested, GroupType fullSchema) {
    for (Type field : fullSchema.getFields()) {

      if (requested.containsField(field.getName())) {
        Type requestedType = requested.getType(field.getName());
        // if a field is in requested schema and the type of it is a group type, then do recursive check
        if (!field.isPrimitive()) {
          if (hasMissingRequiredFieldInGroupType(requestedType.asGroupType(), field.asGroupType())) {
            return true;
          } else {
            continue;// check next field
          }
        }
      } else {
        if (field.getRepetition() == Type.Repetition.REQUIRED) {
          return true; // if a field is missing in requested schema and it's required
        } else {
          continue; // the missing field is not required, then continue checking next field
        }
      }
    }

    return false;
  }

  /**
   *
   * {@inheritDoc}
   * @see org.apache.parquet.io.api.RecordMaterializer#getCurrentRecord()
   */
  @Override
  public T getCurrentRecord() {
    try {
      if (missingRequiredFieldsInProjection) {
        List<TProtocol> fixedEvents = new ProtocolEventsAmender(rootEvents).amendMissingRequiredFields(thriftType);
        protocol.addAll(fixedEvents);
      } else {
        protocol.addAll(rootEvents);
      }

      rootEvents.clear();
      return thriftReader.readOneRecord(protocol);
    } catch (TException e) {
      protocol.clear();
      rootEvents.clear();
      throw new RecordMaterializationException("Could not read thrift object from protocol", e);
    }
  }

  @Override
  public void skipCurrentRecord() {
    rootEvents.clear();
  }

  /**
   *
   * {@inheritDoc}
   * @see org.apache.parquet.io.api.RecordMaterializer#getRootConverter()
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
    case ENUM:
      return new FieldEnumConverter(events, field);
    default:
      return new FieldPrimitiveConverter(events, field);
    }
  }
}
