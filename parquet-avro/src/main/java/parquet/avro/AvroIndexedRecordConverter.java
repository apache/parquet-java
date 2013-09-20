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
package parquet.avro;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import parquet.Preconditions;
import parquet.io.InvalidRecordException;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.Type;

class AvroIndexedRecordConverter<T extends IndexedRecord> extends GroupConverter {

  private final ParentValueContainer parent;
  protected T currentRecord;
  private final Converter[] converters;

  private final Schema avroSchema;
  private final Class<? extends IndexedRecord> specificClass;

  public AvroIndexedRecordConverter(MessageType parquetSchema, Schema avroSchema) {
    this(null, parquetSchema, avroSchema);
  }

  public AvroIndexedRecordConverter(ParentValueContainer parent, GroupType
      parquetSchema, Schema avroSchema) {
    this.parent = parent;
    this.avroSchema = avroSchema;
    int schemaSize = parquetSchema.getFieldCount();
    this.converters = new Converter[schemaSize];
    this.specificClass = SpecificData.get().getClass(avroSchema);

    Map<String, Integer> avroFieldIndexes = new HashMap<String, Integer>();
    int avroFieldIndex = 0;
    for (Schema.Field field: avroSchema.getFields()) {
        avroFieldIndexes.put(field.name(), avroFieldIndex++);
    }
    int parquetFieldIndex = 0;
    for (Type parquetField: parquetSchema.getFields()) {
      Schema.Field avroField = avroSchema.getField(parquetField.getName());
      if (avroField == null) {
        throw new InvalidRecordException(String.format("Parquet/Avro schema mismatch. Avro field '%s' not found.",
                parquetField.getName()));
      }
      Schema nonNullSchema = AvroSchemaConverter.getNonNull(avroField.schema());
      final int finalAvroIndex = avroFieldIndexes.get(avroField.name());
      converters[parquetFieldIndex++] = newConverter(nonNullSchema, parquetField, new ParentValueContainer() {
        @Override
        void add(Object value) {
          AvroIndexedRecordConverter.this.set(finalAvroIndex, value);
        }
      });
    }
  }

  private static Converter newConverter(Schema schema, Type type,
      ParentValueContainer parent) {
    if (schema.getType().equals(Schema.Type.BOOLEAN)) {
      return new FieldBooleanConverter(parent);
    } else if (schema.getType().equals(Schema.Type.INT)) {
      return new FieldIntegerConverter(parent);
    } else if (schema.getType().equals(Schema.Type.LONG)) {
      return new FieldLongConverter(parent);
    } else if (schema.getType().equals(Schema.Type.FLOAT)) {
      return new FieldFloatConverter(parent);
    } else if (schema.getType().equals(Schema.Type.DOUBLE)) {
      return new FieldDoubleConverter(parent);
    } else if (schema.getType().equals(Schema.Type.BYTES)) {
      return new FieldBytesConverter(parent);
    } else if (schema.getType().equals(Schema.Type.STRING)) {
      return new FieldStringConverter(parent);
    } else if (schema.getType().equals(Schema.Type.RECORD)) {
      return new AvroIndexedRecordConverter(parent, type.asGroupType(), schema);
    } else if (schema.getType().equals(Schema.Type.ENUM)) {
      return new FieldEnumConverter(parent,schema);
    } else if (schema.getType().equals(Schema.Type.ARRAY)) {
      return new AvroArrayConverter(parent, type, schema);
    } else if (schema.getType().equals(Schema.Type.MAP)) {
      return new MapConverter(parent, type, schema);
    } else if (schema.getType().equals(Schema.Type.UNION)) {
      return new AvroUnionConverter(parent, type, schema);
    } else if (schema.getType().equals(Schema.Type.FIXED)) {
      return new FieldFixedConverter(parent, schema);
    }
    throw new UnsupportedOperationException(String.format("Cannot convert Avro type: %s" +
        " (Parquet type: %s) ", schema, type));
  }

  private void set(int index, Object value) {
    this.currentRecord.put(index, value);
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    // Should do the right thing whether it is generic or specific
    this.currentRecord = (T) ((this.specificClass == null) ?
            new GenericData.Record(avroSchema) :
            SpecificData.newInstance(specificClass, avroSchema));
  }

  @Override
  public void end() {
    if (parent != null) {
      parent.add(currentRecord);
    }
  }

  T getCurrentRecord() {
    return currentRecord;
  }

  static abstract class ParentValueContainer {

    /**
     * Adds the value to the parent.
     */
    abstract void add(Object value);

  }

  static final class FieldBooleanConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldBooleanConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBoolean(boolean value) {
      parent.add(value);
    }

  }

  static final class FieldIntegerConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldIntegerConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addInt(int value) {
      parent.add(value);
    }

  }

  static final class FieldLongConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldLongConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addLong(long value) {
      parent.add(value);
    }

  }

  static final class FieldFloatConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldFloatConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addFloat(float value) {
      parent.add(value);
    }

  }

  static final class FieldDoubleConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldDoubleConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addDouble(double value) {
      parent.add(value);
    }

  }

  static final class FieldBytesConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldBytesConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(ByteBuffer.wrap(value.getBytes()));
    }

  }

  static final class FieldStringConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldStringConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(value.toStringUsingUTF8());
    }

  }

  static final class FieldEnumConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;
    private final Class<? extends Enum> enumClass;

    public FieldEnumConverter(ParentValueContainer parent, Schema enumSchema) {
      this.parent = parent;
      this.enumClass = SpecificData.get().getClass(enumSchema);
    }

    @Override
    final public void addBinary(Binary value) {
      Object enumValue = value.toStringUsingUTF8();
      if (enumClass != null) {
        enumValue = (Enum.valueOf(enumClass,(String)enumValue));
      }
      parent.add(enumValue);
    }
  }

  static final class FieldFixedConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;
    private final Schema avroSchema;

    public FieldFixedConverter(ParentValueContainer parent, Schema avroSchema) {
      this.parent = parent;
      this.avroSchema = avroSchema;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(new GenericData.Fixed(avroSchema, value.getBytes()));
    }
  }

  static final class AvroArrayConverter<T> extends GroupConverter {

    private final ParentValueContainer parent;
    private final Schema avroSchema;
    private final Converter converter;
    private GenericArray<T> array;

    public AvroArrayConverter(ParentValueContainer parent, Type parquetSchema,
        Schema avroSchema) {
      this.parent = parent;
      this.avroSchema = avroSchema;
      Type elementType = parquetSchema.asGroupType().getType(0);
      Schema elementSchema = avroSchema.getElementType();
      converter = newConverter(elementSchema, elementType, new ParentValueContainer() {
        @Override
        @SuppressWarnings("unchecked")
        void add(Object value) {
          array.add((T) value);
        }
      });
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converter;
    }

    @Override
    public void start() {
      array = new GenericData.Array<T>(0, avroSchema);
    }

    @Override
    public void end() {
      parent.add(array);
    }
  }

  static final class AvroUnionConverter<T> extends GroupConverter {

    private final ParentValueContainer parent;
    private final Converter[] memberConverters;
    private Object memberValue = null;

    public AvroUnionConverter(ParentValueContainer parent, Type parquetSchema,
                              Schema avroSchema) {
      this.parent = parent;
      GroupType parquetGroup = parquetSchema.asGroupType();
      this.memberConverters = new Converter[ parquetGroup.getFieldCount()];

      int parquetIndex = 0;
      for (int index = 0; index < avroSchema.getTypes().size(); index++) {
        Schema memberSchema = avroSchema.getTypes().get(index);
        if (!memberSchema.getType().equals(Schema.Type.NULL)) {
          Type memberType = parquetGroup.getType(parquetIndex);
          memberConverters[parquetIndex] = newConverter(memberSchema, memberType, new ParentValueContainer() {
            @Override
            void add(Object value) {
              Preconditions.checkArgument(memberValue==null, "Union is resolving to more than one type");
              memberValue = value;
            }
          });
          parquetIndex++; // Note for nulls the parquetIndex id not increased
        }
      }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return memberConverters[fieldIndex];
    }

    @Override
    public void start() {
      memberValue = null;
    }

    @Override
    public void end() {
      parent.add(memberValue);
    }
  }

  static final class MapConverter<V> extends GroupConverter {

    private final ParentValueContainer parent;
    private final Converter keyValueConverter;
    private Map<String, V> map;

    public MapConverter(ParentValueContainer parent, Type parquetSchema,
        Schema avroSchema) {
      this.parent = parent;
      this.keyValueConverter = new MapKeyValueConverter(parquetSchema, avroSchema);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return keyValueConverter;
    }

    @Override
    public void start() {
      this.map = new HashMap<String, V>();
    }

    @Override
    public void end() {
      parent.add(map);
    }

    final class MapKeyValueConverter extends GroupConverter {

      private String key;
      private V value;
      private Converter keyConverter;
      private Converter valueConverter;

      public MapKeyValueConverter(Type parquetSchema, Schema avroSchema) {
        keyConverter = new PrimitiveConverter() {
          @Override
          final public void addBinary(Binary value) {
            key = value.toStringUsingUTF8();
          }
        };

        Type valueType = parquetSchema.asGroupType().getType(0).asGroupType().getType(1);
        Schema valueSchema = avroSchema.getValueType();
        valueConverter = newConverter(valueSchema, valueType, new ParentValueContainer() {
          @Override
          @SuppressWarnings("unchecked")
          void add(Object value) {
            MapKeyValueConverter.this.value = (V) value;
          }
        });
      }

      @Override
      public Converter getConverter(int fieldIndex) {
        if (fieldIndex == 0) {
          return keyConverter;
        } else if (fieldIndex == 1) {
          return valueConverter;
        }
        throw new IllegalArgumentException("only the key (0) and value (1) fields expected: " + fieldIndex);
      }

      @Override
      public void start() {
        key = null;
        value = null;
      }

      @Override
      public void end() {
        map.put(key, value);
      }
    }
  }

}
