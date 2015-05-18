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
package org.apache.parquet.avro;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.Preconditions;

/**
 * Avro implementation of {@link WriteSupport} for generic, specific, and
 * reflect models. Use {@link AvroParquetWriter} or
 * {@link AvroParquetOutputFormat} rather than using this class directly.
 */
public class AvroWriteSupport<T> extends WriteSupport<T> {

  public static final String AVRO_DATA_SUPPLIER = "parquet.avro.write.data.supplier";

  public static void setAvroDataSupplier(
      Configuration configuration, Class<? extends AvroDataSupplier> suppClass) {
    configuration.set(AVRO_DATA_SUPPLIER, suppClass.getName());
  }

  static final String AVRO_SCHEMA = "parquet.avro.schema";
  private static final Schema MAP_KEY_SCHEMA = Schema.create(Schema.Type.STRING);

  public static final String WRITE_OLD_LIST_STRUCTURE =
      "parquet.avro.write-old-list-structure";
  static final boolean WRITE_OLD_LIST_STRUCTURE_DEFAULT = true;

  private static final String MAP_REPEATED_NAME = "key_value";
  private static final String MAP_KEY_NAME = "key";
  private static final String MAP_VALUE_NAME = "value";
  private static final String LIST_REPEATED_NAME = "list";
  static final String LIST_ELEMENT_NAME = "element";

  private RecordConsumer recordConsumer;
  private MessageType rootSchema;
  private Schema rootAvroSchema;
  private GenericData model;
  private ListWriter listWriter;

  public AvroWriteSupport() {
  }

  /**
   * @deprecated use {@link AvroWriteSupport(MessageType, Schema, Configuration)}
   */
  @Deprecated
  public AvroWriteSupport(MessageType schema, Schema avroSchema) {
    this.rootSchema = schema;
    this.rootAvroSchema = avroSchema;
    this.model = null;
  }

  public AvroWriteSupport(MessageType schema, Schema avroSchema,
                          GenericData model) {
    this.rootSchema = schema;
    this.rootAvroSchema = avroSchema;
    this.model = model;
  }

  /**
   * @see org.apache.parquet.avro.AvroParquetOutputFormat#setSchema(org.apache.hadoop.mapreduce.Job, org.apache.avro.Schema)
   */
  public static void setSchema(Configuration configuration, Schema schema) {
    configuration.set(AVRO_SCHEMA, schema.toString());
  }

  @Override
  public WriteContext init(Configuration configuration) {
    if (rootAvroSchema == null) {
      this.rootAvroSchema = new Schema.Parser().parse(configuration.get(AVRO_SCHEMA));
      this.rootSchema = new AvroSchemaConverter().convert(rootAvroSchema);
    }

    if (model == null) {
      this.model = getDataModel(configuration);
    }

    boolean writeOldListStructure = configuration.getBoolean(
        WRITE_OLD_LIST_STRUCTURE, WRITE_OLD_LIST_STRUCTURE_DEFAULT);
    if (writeOldListStructure) {
      this.listWriter = new TwoLevelWriter();
    } else {
      this.listWriter = new ThreeLevelWriter();
    }

    Map<String, String> extraMetaData = new HashMap<String, String>();
    extraMetaData.put(AvroReadSupport.AVRO_SCHEMA_METADATA_KEY, rootAvroSchema.toString());
    return new WriteContext(rootSchema, extraMetaData);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  // overloaded version for backward compatibility
  @SuppressWarnings("unchecked")
  public void write(IndexedRecord record) {
    recordConsumer.startMessage();
    writeRecordFields(rootSchema, rootAvroSchema, record);
    recordConsumer.endMessage();
  }

  @Override
  public void write(T record) {
    recordConsumer.startMessage();
    writeRecordFields(rootSchema, rootAvroSchema, record);
    recordConsumer.endMessage();
  }

  private void writeRecord(GroupType schema, Schema avroSchema,
                           Object record) {
    recordConsumer.startGroup();
    writeRecordFields(schema, avroSchema, record);
    recordConsumer.endGroup();
  }

  private void writeRecordFields(GroupType schema, Schema avroSchema,
                                 Object record) {
    List<Type> fields = schema.getFields();
    List<Schema.Field> avroFields = avroSchema.getFields();
    int index = 0; // parquet ignores Avro nulls, so index may differ
    for (int avroIndex = 0; avroIndex < avroFields.size(); avroIndex++) {
      Schema.Field avroField = avroFields.get(avroIndex);
      if (avroField.schema().getType().equals(Schema.Type.NULL)) {
        continue;
      }
      Type fieldType = fields.get(index);
      Object value = model.getField(record, avroField.name(), avroIndex);
      if (value != null) {
        recordConsumer.startField(fieldType.getName(), index);
        writeValue(fieldType, avroField.schema(), value);
        recordConsumer.endField(fieldType.getName(), index);
      } else if (fieldType.isRepetition(Type.Repetition.REQUIRED)) {
        throw new RuntimeException("Null-value for required field: " + avroField.name());
      }
      index++;
    }
  }

  private <V> void writeMap(GroupType schema, Schema avroSchema,
                            Map<CharSequence, V> map) {
    GroupType innerGroup = schema.getType(0).asGroupType();
    Type keyType = innerGroup.getType(0);
    Type valueType = innerGroup.getType(1);

    recordConsumer.startGroup(); // group wrapper (original type MAP)
    if (map.size() > 0) {
      recordConsumer.startField(MAP_REPEATED_NAME, 0);

      for (Map.Entry<CharSequence, V> entry : map.entrySet()) {
        recordConsumer.startGroup(); // repeated group key_value, middle layer
        recordConsumer.startField(MAP_KEY_NAME, 0);
        writeValue(keyType, MAP_KEY_SCHEMA, entry.getKey());
        recordConsumer.endField(MAP_KEY_NAME, 0);
        V value = entry.getValue();
        if (value != null) {
          recordConsumer.startField(MAP_VALUE_NAME, 1);
          writeValue(valueType, avroSchema.getValueType(), value);
          recordConsumer.endField(MAP_VALUE_NAME, 1);
        } else if (!valueType.isRepetition(Type.Repetition.OPTIONAL)) {
          throw new RuntimeException("Null map value for " + avroSchema.getName());
        }
        recordConsumer.endGroup();
      }

      recordConsumer.endField(MAP_REPEATED_NAME, 0);
    }
    recordConsumer.endGroup();
  }

  private void writeUnion(GroupType parquetSchema, Schema avroSchema, 
                          Object value) {
    recordConsumer.startGroup();

    // ResolveUnion will tell us which of the union member types to
    // deserialise.
    int avroIndex = model.resolveUnion(avroSchema, value);

    // For parquet's schema we skip nulls
    GroupType parquetGroup = parquetSchema.asGroupType();
    int parquetIndex = avroIndex;
    for (int i = 0; i < avroIndex; i++) {
      if (avroSchema.getTypes().get(i).getType().equals(Schema.Type.NULL)) {
        parquetIndex--;
      }
    }

    // Sparsely populated method of encoding unions, each member has its own
    // set of columns.
    String memberName = "member" + parquetIndex;
    recordConsumer.startField(memberName, parquetIndex);
    writeValue(parquetGroup.getType(parquetIndex),
               avroSchema.getTypes().get(avroIndex), value);
    recordConsumer.endField(memberName, parquetIndex);

    recordConsumer.endGroup();
  }

  @SuppressWarnings("unchecked")
  private void writeValue(Type type, Schema avroSchema, Object value) {
    Schema nonNullAvroSchema = AvroSchemaConverter.getNonNull(avroSchema);
    Schema.Type avroType = nonNullAvroSchema.getType();
    if (avroType.equals(Schema.Type.BOOLEAN)) {
      recordConsumer.addBoolean((Boolean) value);
    } else if (avroType.equals(Schema.Type.INT)) {
      if (value instanceof Character) {
        recordConsumer.addInteger((Character) value);
      } else {
        recordConsumer.addInteger(((Number) value).intValue());
      }
    } else if (avroType.equals(Schema.Type.LONG)) {
      recordConsumer.addLong(((Number) value).longValue());
    } else if (avroType.equals(Schema.Type.FLOAT)) {
      recordConsumer.addFloat(((Number) value).floatValue());
    } else if (avroType.equals(Schema.Type.DOUBLE)) {
      recordConsumer.addDouble(((Number) value).doubleValue());
    } else if (avroType.equals(Schema.Type.BYTES)) {
      if (value instanceof byte[]) {
        recordConsumer.addBinary(Binary.fromByteArray((byte[]) value));
      } else {
        recordConsumer.addBinary(Binary.fromByteBuffer((ByteBuffer) value));
      }
    } else if (avroType.equals(Schema.Type.STRING)) {
      recordConsumer.addBinary(fromAvroString(value));
    } else if (avroType.equals(Schema.Type.RECORD)) {
      writeRecord(type.asGroupType(), nonNullAvroSchema, value);
    } else if (avroType.equals(Schema.Type.ENUM)) {
      recordConsumer.addBinary(Binary.fromString(value.toString()));
    } else if (avroType.equals(Schema.Type.ARRAY)) {
      listWriter.writeList(type.asGroupType(), nonNullAvroSchema, value);
    } else if (avroType.equals(Schema.Type.MAP)) {
      writeMap(type.asGroupType(), nonNullAvroSchema, (Map<CharSequence, ?>) value);
    } else if (avroType.equals(Schema.Type.UNION)) {
      writeUnion(type.asGroupType(), nonNullAvroSchema, value);
    } else if (avroType.equals(Schema.Type.FIXED)) {
      recordConsumer.addBinary(Binary.fromByteArray(((GenericFixed) value).bytes()));
    }
  }

  private Binary fromAvroString(Object value) {
    if (value instanceof Utf8) {
      Utf8 utf8 = (Utf8) value;
      return Binary.fromByteArray(utf8.getBytes(), 0, utf8.getByteLength());
    }
    return Binary.fromString(value.toString());
  }

  private static GenericData getDataModel(Configuration conf) {
    Class<? extends AvroDataSupplier> suppClass = conf.getClass(
        AVRO_DATA_SUPPLIER, SpecificDataSupplier.class, AvroDataSupplier.class);
    return ReflectionUtils.newInstance(suppClass, conf).get();
  }

  private abstract class ListWriter {
    public void writeList(GroupType schema, Schema avroSchema, Object value) {
      recordConsumer.startGroup(); // group wrapper (original type LIST)
      if (value instanceof Collection) {
        writeCollection(schema, avroSchema, (Collection) value);
      } else {
        Class<?> arrayClass = value.getClass();
        Preconditions.checkArgument(arrayClass.isArray(),
            "Cannot write unless collection or array: " + arrayClass.getName());
        writeJavaArray(schema, avroSchema, arrayClass, value);
      }
      recordConsumer.endGroup();
    }

    public void writeJavaArray(GroupType schema, Schema avroSchema,
                               Class<?> arrayClass, Object value) {
      Class<?> elementClass = arrayClass.getComponentType();

      if (!elementClass.isPrimitive()) {
        writeObjectArray(schema, avroSchema, (Object[]) value);
        return;
      }

      switch (avroSchema.getElementType().getType()) {
        case BOOLEAN:
          Preconditions.checkArgument(elementClass == boolean.class,
              "Cannot write as boolean array: " + arrayClass.getName());
          writeBooleanArray((boolean[]) value);
          break;
        case INT:
          if (elementClass == byte.class) {
            writeByteArray((byte[]) value);
          } else if (elementClass == char.class) {
            writeCharArray((char[]) value);
          } else if (elementClass == short.class) {
            writeShortArray((short[]) value);
          } else if (elementClass == int.class) {
            writeIntArray((int[]) value);
          } else {
            throw new IllegalArgumentException(
                "Cannot write as an int array: " + arrayClass.getName());
          }
          break;
        case LONG:
          Preconditions.checkArgument(elementClass == long.class,
              "Cannot write as long array: " + arrayClass.getName());
          writeLongArray((long[]) value);
          break;
        case FLOAT:
          Preconditions.checkArgument(elementClass == float.class,
              "Cannot write as float array: " + arrayClass.getName());
          writeFloatArray((float[]) value);
          break;
        case DOUBLE:
          Preconditions.checkArgument(elementClass == double.class,
              "Cannot write as double array: " + arrayClass.getName());
          writeDoubleArray((double[]) value);
          break;
        default:
          throw new IllegalArgumentException("Cannot write " +
              avroSchema.getElementType() + " array: " + arrayClass.getName());
      }
    }

    protected abstract void writeCollection(
        GroupType type, Schema schema, Collection<?> collection);

    protected abstract void writeObjectArray(
        GroupType type, Schema schema, Object[] array);

    protected abstract void writeBooleanArray(boolean[] array);

    protected abstract void writeByteArray(byte[] array);

    protected abstract void writeShortArray(short[] array);

    protected abstract void writeCharArray(char[] array);

    protected abstract void writeIntArray(int[] array);

    protected abstract void writeLongArray(long[] array);

    protected abstract void writeFloatArray(float[] array);

    protected abstract void writeDoubleArray(double[] array);
  }

  /**
   * For backward-compatibility. This preserves how lists were written in 1.x.
   */
  private class TwoLevelWriter extends ListWriter {
    @Override
    public void writeCollection(GroupType schema, Schema avroSchema,
                                Collection<?> array) {
      if (array.size() > 0) {
        recordConsumer.startField("array", 0);
        for (Object elt : array) {
          writeValue(schema.getType(0), avroSchema.getElementType(), elt);
        }
        recordConsumer.endField("array", 0);
      }
    }

    @Override
    protected void writeObjectArray(GroupType type, Schema schema,
                                    Object[] array) {
      if (array.length > 0) {
        recordConsumer.startField("array", 0);
        for (Object element : array) {
          writeValue(type.getType(0), schema.getElementType(), element);
        }
        recordConsumer.endField("array", 0);
      }
    }

    @Override
    protected void writeBooleanArray(boolean[] array) {
      if (array.length > 0) {
        recordConsumer.startField("array", 0);
        for (boolean element : array) {
          recordConsumer.addBoolean(element);
        }
        recordConsumer.endField("array", 0);
      }
    }

    @Override
    protected void writeByteArray(byte[] array) {
      if (array.length > 0) {
        recordConsumer.startField("array", 0);
        for (byte element : array) {
          recordConsumer.addInteger(element);
        }
        recordConsumer.endField("array", 0);
      }
    }

    @Override
    protected void writeShortArray(short[] array) {
      if (array.length > 0) {
        recordConsumer.startField("array", 0);
        for (short element : array) {
          recordConsumer.addInteger(element);
        }
        recordConsumer.endField("array", 0);
      }
    }

    @Override
    protected void writeCharArray(char[] array) {
      if (array.length > 0) {
        recordConsumer.startField("array", 0);
        for (char element : array) {
          recordConsumer.addInteger(element);
        }
        recordConsumer.endField("array", 0);
      }
    }

    @Override
    protected void writeIntArray(int[] array) {
      if (array.length > 0) {
        recordConsumer.startField("array", 0);
        for (int element : array) {
          recordConsumer.addInteger(element);
        }
        recordConsumer.endField("array", 0);
      }
    }

    @Override
    protected void writeLongArray(long[] array) {
      if (array.length > 0) {
        recordConsumer.startField("array", 0);
        for (long element : array) {
          recordConsumer.addLong(element);
        }
        recordConsumer.endField("array", 0);
      }
    }

    @Override
    protected void writeFloatArray(float[] array) {
      if (array.length > 0) {
        recordConsumer.startField("array", 0);
        for (float element : array) {
          recordConsumer.addFloat(element);
        }
        recordConsumer.endField("array", 0);
      }
    }

    @Override
    protected void writeDoubleArray(double[] array) {
      if (array.length > 0) {
        recordConsumer.startField("array", 0);
        for (double element : array) {
          recordConsumer.addDouble(element);
        }
        recordConsumer.endField("array", 0);
      }
    }
  }

  private class ThreeLevelWriter extends ListWriter {
    @Override
    protected void writeCollection(GroupType type, Schema schema, Collection<?> collection) {
      if (collection.size() > 0) {
        recordConsumer.startField(LIST_REPEATED_NAME, 0);
        GroupType repeatedType = type.getType(0).asGroupType();
        Type elementType = repeatedType.getType(0);
        for (Object element : collection) {
          recordConsumer.startGroup(); // repeated group array, middle layer
          if (element != null) {
            recordConsumer.startField(LIST_ELEMENT_NAME, 0);
            writeValue(elementType, schema.getElementType(), element);
            recordConsumer.endField(LIST_ELEMENT_NAME, 0);
          } else if (!elementType.isRepetition(Type.Repetition.OPTIONAL)) {
            throw new RuntimeException(
                "Null list element for " + schema.getName());
          }
          recordConsumer.endGroup();
        }
        recordConsumer.endField(LIST_REPEATED_NAME, 0);
      }
    }

    @Override
    protected void writeObjectArray(GroupType type, Schema schema,
                                    Object[] array) {
      if (array.length > 0) {
        recordConsumer.startField(LIST_REPEATED_NAME, 0);
        GroupType repeatedType = type.getType(0).asGroupType();
        Type elementType = repeatedType.getType(0);
        for (Object element : array) {
          recordConsumer.startGroup(); // repeated group array, middle layer
          if (element != null) {
            recordConsumer.startField(LIST_ELEMENT_NAME, 0);
            writeValue(elementType, schema.getElementType(), element);
            recordConsumer.endField(LIST_ELEMENT_NAME, 0);
          } else if (!elementType.isRepetition(Type.Repetition.OPTIONAL)) {
            throw new RuntimeException(
                "Null list element for " + schema.getName());
          }
          recordConsumer.endGroup();
        }
        recordConsumer.endField(LIST_REPEATED_NAME, 0);
      }
    }

    @Override
    protected void writeBooleanArray(boolean[] array) {
      if (array.length > 0) {
        recordConsumer.startField(LIST_REPEATED_NAME, 0);
        recordConsumer.startGroup(); // repeated group array, middle layer
        recordConsumer.startField(LIST_ELEMENT_NAME, 0);
        for (boolean element : array) {
          recordConsumer.addBoolean(element);
        }
        recordConsumer.endField(LIST_ELEMENT_NAME, 0);
        recordConsumer.endGroup();
        recordConsumer.endField(LIST_REPEATED_NAME, 0);
      }
    }

    @Override
    protected void writeByteArray(byte[] array) {
      if (array.length > 0) {
        recordConsumer.startField(LIST_REPEATED_NAME, 0);
        recordConsumer.startGroup(); // repeated group array, middle layer
        recordConsumer.startField(LIST_ELEMENT_NAME, 0);
        for (byte element : array) {
          recordConsumer.addInteger(element);
        }
        recordConsumer.endField(LIST_ELEMENT_NAME, 0);
        recordConsumer.endGroup();
        recordConsumer.endField(LIST_REPEATED_NAME, 0);
      }
    }

    @Override
    protected void writeShortArray(short[] array) {
      if (array.length > 0) {
        recordConsumer.startField("array", 0);
        recordConsumer.startGroup(); // repeated group array, middle layer
        recordConsumer.startField(LIST_ELEMENT_NAME, 0);
        for (short element : array) {
          recordConsumer.addInteger(element);
        }
        recordConsumer.endField(LIST_ELEMENT_NAME, 0);
        recordConsumer.endGroup();
        recordConsumer.endField("array", 0);
      }
    }

    @Override
    protected void writeCharArray(char[] array) {
      if (array.length > 0) {
        recordConsumer.startField("array", 0);
        recordConsumer.startGroup(); // repeated group array, middle layer
        recordConsumer.startField(LIST_ELEMENT_NAME, 0);
        for (char element : array) {
          recordConsumer.addInteger(element);
        }
        recordConsumer.endField(LIST_ELEMENT_NAME, 0);
        recordConsumer.endGroup();
        recordConsumer.endField("array", 0);
      }
    }

    @Override
    protected void writeIntArray(int[] array) {
      if (array.length > 0) {
        recordConsumer.startField("array", 0);
        recordConsumer.startGroup(); // repeated group array, middle layer
        recordConsumer.startField(LIST_ELEMENT_NAME, 0);
        for (int element : array) {
          recordConsumer.addInteger(element);
        }
        recordConsumer.endField(LIST_ELEMENT_NAME, 0);
        recordConsumer.endGroup();
        recordConsumer.endField("array", 0);
      }
    }

    @Override
    protected void writeLongArray(long[] array) {
      if (array.length > 0) {
        recordConsumer.startField("array", 0);
        recordConsumer.startGroup(); // repeated group array, middle layer
        recordConsumer.startField(LIST_ELEMENT_NAME, 0);
        for (long element : array) {
          recordConsumer.addLong(element);
        }
        recordConsumer.endField(LIST_ELEMENT_NAME, 0);
        recordConsumer.endGroup();
        recordConsumer.endField("array", 0);
      }
    }

    @Override
    protected void writeFloatArray(float[] array) {
      if (array.length > 0) {
        recordConsumer.startField("array", 0);
        recordConsumer.startGroup(); // repeated group array, middle layer
        recordConsumer.startField(LIST_ELEMENT_NAME, 0);
        for (float element : array) {
          recordConsumer.addFloat(element);
        }
        recordConsumer.endField(LIST_ELEMENT_NAME, 0);
        recordConsumer.endGroup();
        recordConsumer.endField("array", 0);
      }
    }

    @Override
    protected void writeDoubleArray(double[] array) {
      if (array.length > 0) {
        recordConsumer.startField("array", 0);
        recordConsumer.startGroup(); // repeated group array, middle layer
        recordConsumer.startField(LIST_ELEMENT_NAME, 0);
        for (double element : array) {
          recordConsumer.addDouble(element);
        }
        recordConsumer.endField(LIST_ELEMENT_NAME, 0);
        recordConsumer.endGroup();
        recordConsumer.endField("array", 0);
      }
    }
  }
}
