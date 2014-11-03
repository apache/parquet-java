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
package parquet.avro;

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
import parquet.hadoop.api.WriteSupport;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.Type;

/**
 * Avro implementation of {@link WriteSupport} for {@link IndexedRecord}s - both Avro Generic and Specific.
 * Users should use {@link AvroParquetWriter} or {@link AvroParquetOutputFormat} rather than using
 * this class directly.
 */
public class AvroWriteSupport extends WriteSupport<IndexedRecord> {

  static final String AVRO_SCHEMA = "parquet.avro.schema";
  private static final Schema MAP_KEY_SCHEMA = Schema.create(Schema.Type.STRING);

  private RecordConsumer recordConsumer;
  private MessageType rootSchema;
  private Schema rootAvroSchema;

  public AvroWriteSupport() {
  }

  public AvroWriteSupport(MessageType schema, Schema avroSchema) {
    this.rootSchema = schema;
    this.rootAvroSchema = avroSchema;
  }

  /**
   * @see parquet.avro.AvroParquetOutputFormat#setSchema(org.apache.hadoop.mapreduce.Job, org.apache.avro.Schema)
   */
  public static void setSchema(Configuration configuration, Schema schema) {
    configuration.set(AVRO_SCHEMA, schema.toString());
  }

  @Override
  public WriteContext init(Configuration configuration) {
    if (rootAvroSchema == null) {
      rootAvroSchema = new Schema.Parser().parse(configuration.get(AVRO_SCHEMA));
      rootSchema = new AvroSchemaConverter().convert(rootAvroSchema);
    }
    Map<String, String> extraMetaData = new HashMap<String, String>();
    extraMetaData.put(AvroReadSupport.AVRO_SCHEMA_METADATA_KEY, rootAvroSchema.toString());
    return new WriteContext(rootSchema, extraMetaData);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(IndexedRecord record) {
    recordConsumer.startMessage();
    writeRecordFields(rootSchema, rootAvroSchema, record);
    recordConsumer.endMessage();
  }

  private void writeRecord(GroupType schema, Schema avroSchema,
                           IndexedRecord record) {
    recordConsumer.startGroup();
    writeRecordFields(schema, avroSchema, record);
    recordConsumer.endGroup();
  }

  private void writeRecordFields(GroupType schema, Schema avroSchema,
                                 IndexedRecord record) {
    List<Type> fields = schema.getFields();
    List<Schema.Field> avroFields = avroSchema.getFields();
    int index = 0; // parquet ignores Avro nulls, so index may differ
    for (int avroIndex = 0; avroIndex < avroFields.size(); avroIndex++) {
      Schema.Field avroField = avroFields.get(avroIndex);
      if (avroField.schema().getType().equals(Schema.Type.NULL)) {
        continue;
      }
      Type fieldType = fields.get(index);
      Object value = record.get(avroIndex);
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

  private <T> void writeArray(GroupType schema, Schema avroSchema,
                              Collection<T> array) {
    recordConsumer.startGroup(); // group wrapper (original type LIST)
    if (array.size() > 0) {
      recordConsumer.startField("array", 0);
      for (T elt : array) {
        writeValue(schema.getType(0), avroSchema.getElementType(), elt);
      }
      recordConsumer.endField("array", 0);
    }
    recordConsumer.endGroup();
  }

  private <V> void writeMap(GroupType schema, Schema avroSchema,
                            Map<CharSequence, V> map) {
    GroupType innerGroup = schema.getType(0).asGroupType();
    Type keyType = innerGroup.getType(0);
    Type valueType = innerGroup.getType(1);

    recordConsumer.startGroup(); // group wrapper (original type MAP)
    if (map.size() > 0) {
      recordConsumer.startField("map", 0);

      for (Map.Entry<CharSequence, V> entry : map.entrySet()) {
        recordConsumer.startGroup(); // repeated group key_value, middle layer
        recordConsumer.startField("key", 0);
        writeValue(keyType, MAP_KEY_SCHEMA, entry.getKey());
        recordConsumer.endField("key", 0);
        V value = entry.getValue();
        if (value != null) {
          recordConsumer.startField("value", 1);
          writeValue(valueType, avroSchema.getValueType(), value);
          recordConsumer.endField("value", 1);
        } else if (!valueType.isRepetition(Type.Repetition.OPTIONAL)) {
          throw new RuntimeException("Null map value for " + avroSchema.getName());
        }
        recordConsumer.endGroup();
      }

      recordConsumer.endField("map", 0);
    }
    recordConsumer.endGroup();
  }

  private void writeUnion(GroupType parquetSchema, Schema avroSchema, 
                          Object value) {
    recordConsumer.startGroup();

    // ResolveUnion will tell us which of the union member types to
    // deserialise.
    int avroIndex = GenericData.get().resolveUnion(avroSchema, value);

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
      recordConsumer.addInteger(((Number) value).intValue());
    } else if (avroType.equals(Schema.Type.LONG)) {
      recordConsumer.addLong(((Number) value).longValue());
    } else if (avroType.equals(Schema.Type.FLOAT)) {
      recordConsumer.addFloat(((Number) value).floatValue());
    } else if (avroType.equals(Schema.Type.DOUBLE)) {
      recordConsumer.addDouble(((Number) value).doubleValue());
    } else if (avroType.equals(Schema.Type.BYTES)) {
      recordConsumer.addBinary(Binary.fromByteBuffer((ByteBuffer) value));
    } else if (avroType.equals(Schema.Type.STRING)) {
      recordConsumer.addBinary(fromAvroString(value));
    } else if (avroType.equals(Schema.Type.RECORD)) {
      writeRecord((GroupType) type, nonNullAvroSchema, (IndexedRecord) value);
    } else if (avroType.equals(Schema.Type.ENUM)) {
      recordConsumer.addBinary(Binary.fromString(value.toString()));
    } else if (avroType.equals(Schema.Type.ARRAY)) {
      writeArray((GroupType) type, nonNullAvroSchema, (Collection<?>) value);
    } else if (avroType.equals(Schema.Type.MAP)) {
      writeMap((GroupType) type, nonNullAvroSchema, (Map<CharSequence, ?>) value);
    } else if (avroType.equals(Schema.Type.UNION)) {
      writeUnion((GroupType) type, nonNullAvroSchema, value);
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

}
