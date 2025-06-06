/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.avro;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.DirectWriterTest;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.variant.ImmutableMetadata;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantArrayBuilder;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantObjectBuilder;
import org.junit.Test;

public class TestWriteVariant extends DirectWriterTest {

  private static Variant fullVariant(Consumer<VariantBuilder> appendValue) {
    VariantBuilder builder = new VariantBuilder();
    appendValue.accept(builder);
    return builder.build();
  }

  // Return only the byte[], which is usually all we want.
  private static ByteBuffer variant(Consumer<VariantBuilder> appendValue) {
    return fullVariant(appendValue).getValueBuffer();
  }

  // Returns a value based on building with fixed metadata.
  private static ByteBuffer variant(ByteBuffer metadata, Consumer<VariantBuilder> appendValue) {
    VariantBuilder builder = new VariantBuilder(new ImmutableMetadata(metadata));
    appendValue.accept(builder);
    return builder.build().getValueBuffer();
  }

  private static ByteBuffer variant(int val) {
    return variant(b -> b.appendInt(val));
  }

  private static ByteBuffer variant(long val) {
    return variant(b -> b.appendLong(val));
  }

  private static ByteBuffer variant(String s) {
    return variant(b -> b.appendString(s));
  }

  private static final GroupType UNSHREDDED_GROUP = Types.buildGroup(Type.Repetition.REQUIRED)
      .as(LogicalTypeAnnotation.variantType((byte) 1))
      .required(PrimitiveTypeName.BINARY)
      .named("metadata")
      .required(PrimitiveTypeName.BINARY)
      .named("value")
      .named("var");

  private static MessageType parquetSchema(GroupType variantGroup) {
    return Types.buildMessage()
        .required(INT32)
        .named("id")
        .addField(variantGroup)
        .named("table");
  }

  private static final MessageType READ_SCHEMA = parquetSchema(UNSHREDDED_GROUP);

  private static final Schema VARIANT_SCHEMA = new AvroSchemaConverter().convert(UNSHREDDED_GROUP);
  private static final Schema SCHEMA = new AvroSchemaConverter().convert(READ_SCHEMA);

  private ByteBuffer TEST_METADATA;
  private ByteBuffer TEST_OBJECT;
  private ByteBuffer SIMILAR_OBJECT;
  private ByteBuffer EMPTY_ARRAY;
  private ByteBuffer STRING_ARRAY;
  private ByteBuffer MIXED_ARRAY;
  private ByteBuffer NESTED_ARRAY;
  private ByteBuffer MIXED_NESTED_ARRAY;
  private ByteBuffer OBJECT_IN_ARRAY;
  private ByteBuffer MIXED_OBJECT_IN_ARRAY;
  private ByteBuffer SIMILAR_OBJECT_IN_ARRAY;
  private ByteBuffer EMPTY_OBJECT;
  private ByteBuffer EMPTY_METADATA = fullVariant(b -> b.appendNull()).getMetadataBuffer();
  private Variant[] VARIANTS;

  public TestWriteVariant() throws Exception {
    TEST_METADATA = fullVariant(b -> {
          VariantObjectBuilder ob = b.startObject();
          ob.appendKey("a");
          ob.appendNull();
          ob.appendKey("b");
          ob.appendNull();
          ob.appendKey("c");
          ob.appendNull();
          ob.appendKey("d");
          ob.appendNull();
          ob.appendKey("e");
          ob.appendNull();
          b.endObject();
        })
        .getMetadataBuffer();

    TEST_OBJECT = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("a");
      ob.appendNull();
      ob.appendKey("d");
      ob.appendString("iceberg");
      b.endObject();
    });

    SIMILAR_OBJECT = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("a");
      ob.appendInt(123456789);
      ob.appendKey("c");
      ob.appendString("string");
      b.endObject();
    });

    EMPTY_ARRAY = variant(b -> {
      b.startArray();
      b.endArray();
    });

    STRING_ARRAY = variant(b -> {
      VariantArrayBuilder ab = b.startArray();
      ab.appendString("parquet");
      b.endArray();
    });

    MIXED_ARRAY = variant(b -> {
      VariantArrayBuilder ab = b.startArray();
      ab.appendString("parquet");
      ab.appendString("string");
      ab.appendInt(34);
      b.endArray();
    });

    NESTED_ARRAY = variant(b -> {
      VariantArrayBuilder ab = b.startArray();
      VariantArrayBuilder inner1 = ab.startArray();
      inner1.appendString("parquet");
      inner1.appendString("string");
      ab.endArray();
      VariantArrayBuilder inner2 = ab.startArray();
      inner2.appendString("parquet");
      inner2.appendString("string");
      ab.endArray();
      b.endArray();
    });

    MIXED_NESTED_ARRAY = variant(b -> {
      VariantArrayBuilder ab = b.startArray();
      VariantArrayBuilder inner1 = ab.startArray();
      inner1.appendString("parquet");
      inner1.appendString("string");
      inner1.appendInt(34);
      ab.endArray();
      VariantArrayBuilder inner2 = ab.startArray();
      inner2.appendInt(34);
      inner2.appendNull();
      ab.endArray();
      ab.startArray();
      ab.endArray();
      VariantArrayBuilder inner4 = ab.startArray();
      inner4.appendString("parquet");
      inner4.appendString("string");
      inner4.appendInt(34);
      ab.endArray();
      b.endArray();
    });

    // The first array element defines the schema.
    OBJECT_IN_ARRAY = variant(TEST_METADATA, b -> {
      VariantArrayBuilder ab = b.startArray();
      VariantObjectBuilder ob = ab.startObject();
      ob.appendKey("a");
      ob.appendNull();
      ob.appendKey("d");
      ob.appendString("iceberg");
      ab.endObject();
      ab.appendInt(123);
      VariantObjectBuilder ob2 = ab.startObject();
      ob2.appendKey("c");
      ob2.appendString("hello");
      ob2.appendKey("d");
      ob2.appendDate(12345);
      ab.endObject();
      b.endArray();
    });

    MIXED_OBJECT_IN_ARRAY = variant(TEST_METADATA, b -> {
      VariantArrayBuilder ab = b.startArray();
      VariantObjectBuilder ob = ab.startObject();
      ob.appendKey("a");
      ob.appendNull();
      ob.appendKey("d");
      ob.appendString("iceberg");
      ab.endObject();
      ab.appendInt(123);
      VariantObjectBuilder ob2 = ab.startObject();
      ob2.appendKey("c");
      ob2.appendString("hello");
      ob2.appendKey("d");
      ob2.appendDate(12345);
      ab.endObject();
      ab.appendString("parquet");
      ab.appendInt(34);
      b.endArray();
    });

    // Change one field name and one type in the first element to change the schema.
    SIMILAR_OBJECT_IN_ARRAY = variant(TEST_METADATA, b -> {
      VariantArrayBuilder ab = b.startArray();
      VariantObjectBuilder ob = ab.startObject();
      ob.appendKey("c");
      ob.appendString("iceberg");
      ob.appendKey("a");
      ob.appendString("parquet");
      ab.endObject();
      ab.appendInt(123);
      VariantObjectBuilder ob2 = ab.startObject();
      ob2.appendKey("c");
      ob2.appendString("hello");
      ob2.appendKey("d");
      ob2.appendDate(12345);
      ab.endObject();
      b.endArray();
    });

    EMPTY_OBJECT = variant(TEST_METADATA, b -> {
      b.startObject();
      b.endObject();
    });

    VARIANTS = new Variant[] {
      fullVariant(b -> b.appendNull()),
      fullVariant(b -> b.appendBoolean(true)),
      fullVariant(b -> b.appendBoolean(false)),
      fullVariant(b -> b.appendByte((byte) 34)),
      fullVariant(b -> b.appendByte((byte) -34)),
      fullVariant(b -> b.appendShort((byte) 1234)),
      fullVariant(b -> b.appendShort((byte) -1234)),
      fullVariant(b -> b.appendInt(12345)),
      fullVariant(b -> b.appendInt(-12345)),
      fullVariant(b -> b.appendLong(9876543210L)),
      fullVariant(b -> b.appendLong(-9876543210L)),
      fullVariant(b -> b.appendFloat(10.11F)),
      fullVariant(b -> b.appendFloat(-10.11F)),
      fullVariant(b -> b.appendDouble(14.3D)),
      fullVariant(b -> b.appendDouble(-14.3D)),
      new Variant(EMPTY_OBJECT, EMPTY_METADATA),
      new Variant(TEST_OBJECT, TEST_METADATA),
      new Variant(SIMILAR_OBJECT, TEST_METADATA),
      new Variant(EMPTY_ARRAY, EMPTY_METADATA),
      new Variant(STRING_ARRAY, EMPTY_METADATA),
      new Variant(MIXED_ARRAY, EMPTY_METADATA),
      new Variant(NESTED_ARRAY, EMPTY_METADATA),
      new Variant(MIXED_NESTED_ARRAY, EMPTY_METADATA),
      new Variant(OBJECT_IN_ARRAY, TEST_METADATA),
      new Variant(MIXED_OBJECT_IN_ARRAY, TEST_METADATA),
      new Variant(SIMILAR_OBJECT_IN_ARRAY, TEST_METADATA),
      fullVariant(b -> b.appendDate(12345)),
      fullVariant(b -> b.appendDate(-12345)),
      fullVariant(b -> b.appendTimestampTz(1234567890L)),
      fullVariant(b -> b.appendTimestampTz(-1234567890L)),
      fullVariant(b -> b.appendTimestampNtz(1234567890L)),
      fullVariant(b -> b.appendTimestampNtz(-1234567890L)),
      fullVariant(b -> b.appendDecimal(new BigDecimal("123456.789"))), // decimal4
      fullVariant(b -> b.appendDecimal(new BigDecimal("-123456.789"))), // decimal4
      fullVariant(b -> b.appendDecimal(new BigDecimal("123456789.987654321"))), // decimal8
      fullVariant(b -> b.appendDecimal(new BigDecimal("-123456789.987654321"))), // decimal8
      fullVariant(b -> b.appendDecimal(new BigDecimal("9876543210.123456789"))), // decimal16
      fullVariant(b -> b.appendDecimal(new BigDecimal("-9876543210.123456789"))), // decimal16
      fullVariant(b -> b.appendBinary(ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0x0c, 0x0d}))),
      fullVariant(b -> b.appendString("iceberg")),
      fullVariant(b -> b.appendTime(1234567890)),
      fullVariant(b -> b.appendTimestampNanosTz(1234567890L)),
      fullVariant(b -> b.appendTimestampNanosTz(-1234567890L)),
      fullVariant(b -> b.appendTimestampNanosNtz(1234567890L)),
      fullVariant(b -> b.appendTimestampNanosNtz(-1234567890L)),
      fullVariant(b -> b.appendUUID(UUID.fromString("f24f9b64-81fa-49d1-b74e-8c09a6e31c56")))
    };
  }

  /**
   * Create a record containing a Variant value using the standard schema.
   */
  GenericRecord createRecord(int i, Variant v) {
    GenericRecord vRecord = new GenericData.Record(VARIANT_SCHEMA);
    vRecord.put(0, v.getMetadataBuffer());
    vRecord.put(1, v.getValueBuffer());
    GenericRecord record = new GenericData.Record(SCHEMA);
    record.put(0, i);
    record.put(1, vRecord);
    return record;
  }

  // Tests in this file are based on Iceberg's TestVariantWriters suite.
  @Test
  public void testUnshreddedValues() throws IOException {
    for (Variant v : VARIANTS) {
      GenericRecord record = createRecord(1, v);
      TestSchema testSchema = new TestSchema(READ_SCHEMA, READ_SCHEMA);

      GenericRecord actual = writeAndRead(testSchema, record);

      assertEquals(record.get(0), actual.get(0));
      assertEquals(((GenericRecord) record.get(1)).get(0), ((GenericRecord) actual.get(1)).get(0));
      assertEquals(((GenericRecord) record.get(1)).get(1), ((GenericRecord) actual.get(1)).get(1));
    }
  }

  @Test
  public void testShreddedValues() throws IOException {
    for (Variant v : VARIANTS) {
      GenericRecord record = createRecord(1, v);
      MessageType writeSchema = shreddingSchema(v);
      TestSchema testSchema = new TestSchema(writeSchema, READ_SCHEMA);

      GenericRecord actual = writeAndRead(testSchema, record);
      assertEquals(record.get(0), actual.get(0));
      Variant actualV = new Variant((ByteBuffer) ((GenericRecord) actual.get(1)).get(1), (ByteBuffer)
          ((GenericRecord) actual.get(1)).get(0));
      AvroTestUtil.assertEquivalent(v, actualV);
    }
  }

  @Test
  public void testMixedShredding() throws IOException {
    for (Variant valueForSchema : VARIANTS) {
      List<GenericRecord> expected = new ArrayList<>();
      for (int i = 0; i < VARIANTS.length; i++) {
        expected.add(createRecord(i, VARIANTS[i]));
      }

      MessageType writeSchema = shreddingSchema(valueForSchema);
      TestSchema testSchema = new TestSchema(writeSchema, READ_SCHEMA);

      List<GenericRecord> actual = writeAndRead(testSchema, expected);
      assertEquals(actual.size(), VARIANTS.length);
      for (int i = 0; i < VARIANTS.length; i++) {
        Variant actualV =
            new Variant((ByteBuffer) ((GenericRecord) actual.get(i).get(1)).get(1), (ByteBuffer)
                ((GenericRecord) actual.get(i).get(1)).get(0));
        AvroTestUtil.assertEquivalent(VARIANTS[i], actualV);
      }
    }
  }

  // Write schema contains the full shredding schema. Read schema should just be a value/metadata pair.
  private static class TestSchema {
    MessageType writeSchema;
    MessageType readSchema;

    TestSchema(MessageType writeSchema, MessageType readSchema) {
      this.writeSchema = writeSchema;
      this.readSchema = readSchema;
    }
  }

  /**
   * This is a custom Parquet writer builder that injects a specific Parquet schema and then uses
   * the Avro object model. This ensures that the Parquet file's schema is exactly what was passed.
   */
  private static class TestWriterBuilder extends ParquetWriter.Builder<GenericRecord, TestWriterBuilder> {
    private MessageType schema = null;

    protected TestWriterBuilder(Path path) {
      super(path);
    }

    TestWriterBuilder withFileType(MessageType schema) {
      this.schema = schema;
      return self();
    }

    @Override
    protected TestWriterBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<GenericRecord> getWriteSupport(Configuration conf) {
      return new AvroWriteSupport<>(schema, new AvroSchemaConverter().convert(schema), GenericData.get());
    }
  }

  GenericRecord writeAndRead(TestSchema testSchema, GenericRecord record) throws IOException {
    List<GenericRecord> result = writeAndRead(testSchema, Arrays.asList(record));
    assert (result.size() == 1);
    return result.get(0);
  }

  private List<GenericRecord> writeAndRead(TestSchema testSchema, List<GenericRecord> records) throws IOException {
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path path = new Path(tmp.getPath());

    try (ParquetWriter<GenericRecord> writer =
        new TestWriterBuilder(path).withFileType(testSchema.writeSchema).build()) {
      for (GenericRecord record : records) {
        writer.write(record);
      }
    }

    Configuration conf = new Configuration();
    AvroReadSupport.setAvroReadSchema(conf, new AvroSchemaConverter().convert(testSchema.readSchema));
    AvroParquetReader<GenericRecord> reader = new AvroParquetReader(conf, path);

    ArrayList<GenericRecord> result = new ArrayList<>();
    GenericRecord next = reader.read();
    while (next != null) {
      result.add(next);
      next = reader.read();
    }
    return result;
  }

  /**
   * Build a shredding schema that will perfectly shred the provided value.
   */
  private static MessageType shreddingSchema(Variant v) {
    Type shreddedType = shreddedType(v);
    Types.GroupBuilder<GroupType> partialType = Types.buildGroup(Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.variantType((byte) 1))
        .required(BINARY)
        .named("metadata")
        .optional(BINARY)
        .named("value");
    Type variantType;
    if (shreddedType == null) {
      variantType = partialType.named("var");
    } else {
      variantType = partialType.addField(shreddedType).named("var");
    }
    return Types.buildMessage()
        .required(INT32)
        .named("id")
        .addField(variantType)
        .named("table");
  }

  private static GroupType shreddedGroup(Variant v, String name) {
    Type shreddedType = shreddedType(v);
    if (shreddedType == null) {
      return Types.buildGroup(Type.Repetition.OPTIONAL)
          .optional(BINARY)
          .named("value")
          .named(name);
    } else {
      return Types.buildGroup(Type.Repetition.OPTIONAL)
          .optional(BINARY)
          .named("value")
          .addField(shreddedType)
          .named(name);
    }
  }

  /**
   * @return A shredded type, or null if there is no valid shredded type.
   */
  private static Type shreddedType(Variant v) {
    switch (v.getType()) {
      case NULL:
        return null;
      case BOOLEAN:
        return Types.optional(BOOLEAN).named("typed_value");
      case BYTE:
        return Types.optional(INT32)
            .as(LogicalTypeAnnotation.intType(8))
            .named("typed_value");
      case SHORT:
        return Types.optional(INT32)
            .as(LogicalTypeAnnotation.intType(16))
            .named("typed_value");
      case INT:
        return Types.optional(INT32).named("typed_value");
      case LONG:
        return Types.optional(INT64).named("typed_value");
      case FLOAT:
        return Types.optional(FLOAT).named("typed_value");
      case DOUBLE:
        return Types.optional(DOUBLE).named("typed_value");
      case DECIMAL4:
        return Types.optional(INT32)
            .as(LogicalTypeAnnotation.decimalType(v.getDecimal().scale(), 9))
            .named("typed_value");
      case DECIMAL8:
        return Types.optional(INT64)
            .as(LogicalTypeAnnotation.decimalType(v.getDecimal().scale(), 18))
            .named("typed_value");
      case DECIMAL16:
        return Types.optional(BINARY)
            .as(LogicalTypeAnnotation.decimalType(v.getDecimal().scale(), 38))
            .named("typed_value");
      case DATE:
        return Types.optional(INT32)
            .as(LogicalTypeAnnotation.dateType())
            .named("typed_value");
      case TIMESTAMP_TZ:
        return Types.optional(INT64)
            .as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS))
            .named("typed_value");
      case TIMESTAMP_NTZ:
        return Types.optional(INT64)
            .as(LogicalTypeAnnotation.timestampType(false, TimeUnit.MICROS))
            .named("typed_value");
      case BINARY:
        return Types.optional(BINARY).named("typed_value");
      case STRING:
        return Types.optional(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("typed_value");
      case TIME:
        return Types.optional(INT64)
            .as(LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS))
            .named("typed_value");
      case TIMESTAMP_NANOS_TZ:
        return Types.optional(INT64)
            .as(LogicalTypeAnnotation.timestampType(true, TimeUnit.NANOS))
            .named("typed_value");
      case TIMESTAMP_NANOS_NTZ:
        return Types.optional(INT64)
            .as(LogicalTypeAnnotation.timestampType(false, TimeUnit.NANOS))
            .named("typed_value");
      case UUID:
        return Types.optional(FIXED_LEN_BYTE_ARRAY)
            .as(LogicalTypeAnnotation.uuidType())
            .named("typed_value");
      case OBJECT:
        return shreddedObjectType(v);
      case ARRAY:
        return shreddedArrayType(v);
      default:
        throw new UnsupportedOperationException("Unsupported shredding type: " + v.getType());
    }
  }

  private static Type shreddedObjectType(Variant v) {
    if (v.numObjectElements() == 0) {
      // Parquet can't represent empty groups.
      return null;
    }
    Types.GroupBuilder<GroupType> builder = Types.optionalGroup();
    for (int i = 0; i < v.numObjectElements(); i++) {
      Variant.ObjectField field = v.getFieldAtIndex(i);
      Types.GroupBuilder<GroupType> fieldBuilder = Types.optionalGroup();
      Type fieldType = shreddedGroup(field.value, field.key);
      builder.addField(fieldType);
    }
    return builder.named("typed_value");
  }

  private static Type shreddedArrayType(Variant v) {
    // Use the first element to determine the array element type
    Variant firstElement;
    if (v.numArrayElements() > 0) {
      firstElement = v.getElementAtIndex(0);
    } else {
      // Use null as a dummy value, which will omit typed_value from the schema.
      firstElement = fullVariant(b -> b.appendNull());
    }

    Type elementType = shreddedGroup(firstElement, "element");
    return Types.optionalList().setElementType(elementType).named("typed_value");
  }
}
