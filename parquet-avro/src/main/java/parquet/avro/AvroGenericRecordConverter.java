package parquet.avro;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.Type;

class AvroGenericRecordConverter extends GroupConverter {

  private final ParentValueContainer parent;
  protected GenericData.Record currentRecord;
  private final Converter[] converters;

  private final GroupType parquetSchema;
  private final Schema avroSchema;

  public AvroGenericRecordConverter(MessageType parquetSchema, Schema avroSchema) {
    this(null, parquetSchema, avroSchema);
  }

  public AvroGenericRecordConverter(ParentValueContainer parent, GroupType
      parquetSchema, Schema avroSchema) {
    this.parent = parent;
    this.parquetSchema = parquetSchema;
    this.avroSchema = avroSchema;
    int schemaSize = parquetSchema.getFieldCount();
    this.converters = new Converter[schemaSize];
    int index = 0; // parquet ignores Avro nulls, so index may differ
    for (int avroIndex = 0; avroIndex < avroSchema.getFields().size(); avroIndex++) {
      Schema.Field field = avroSchema.getFields().get(avroIndex);
      if (field.schema().getType().equals(Schema.Type.NULL)) {
        continue;
      }
      Type type = parquetSchema.getType(index);
      final int finalAvroIndex = avroIndex;
      converters[index] = newConverter(field.schema(), type, new ParentValueContainer() {
        @Override
        void add(Object value) {
          AvroGenericRecordConverter.this.set(finalAvroIndex, value);
        }
      });
      index++;
    }
    this.currentRecord = new GenericData.Record(avroSchema);
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
      return new AvroGenericRecordConverter(parent, type.asGroupType(), schema);
    } else if (schema.getType().equals(Schema.Type.ENUM)) {
      return new FieldStringConverter(parent);
    } else if (schema.getType().equals(Schema.Type.ARRAY)) {
      return new GenericArrayConverter(parent, type, schema);
    } else if (schema.getType().equals(Schema.Type.MAP)) {
      return new MapConverter(parent, type, schema);
    } else if (schema.getType().equals(Schema.Type.FIXED)) {
      return new FieldFixedConverter(parent, schema);
    }
    throw new UnsupportedOperationException();
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
    this.currentRecord = new GenericData.Record(avroSchema);
  }

  @Override
  public void end() {
    if (parent != null) {
      parent.add(currentRecord);
    }
  }

  GenericRecord getCurrentRecord() {
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

  static final class GenericArrayConverter<T> extends GroupConverter {

    private final ParentValueContainer parent;
    private GenericArray<T> array;
    private final Converter converter;

    public GenericArrayConverter(ParentValueContainer parent, Type parquetSchema,
        Schema avroSchema) {
      this.parent = parent;
      array = new GenericData.Array<T>(0, avroSchema);
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

    @Override public void start() { }

    @Override
    public void end() {
      parent.add(array);
    }
  }

  static final class MapConverter<V> extends GroupConverter {

    private final ParentValueContainer parent;
    private Map<String, V> map;
    private final Converter keyValueConverter;

    public MapConverter(ParentValueContainer parent, Type parquetSchema,
        Schema avroSchema) {
      this.parent = parent;
      this.map = new HashMap<String, V>();
      this.keyValueConverter = new MapKeyValueConverter(parquetSchema, avroSchema);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return keyValueConverter;
    }

    @Override public void start() { }

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
