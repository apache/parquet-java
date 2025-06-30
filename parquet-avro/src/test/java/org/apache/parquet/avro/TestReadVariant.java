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

import static org.apache.parquet.avro.AvroTestUtil.field;
import static org.apache.parquet.avro.AvroTestUtil.instance;
import static org.apache.parquet.avro.AvroTestUtil.record;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.DirectWriterTest;
import org.apache.parquet.Preconditions;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.variant.ImmutableMetadata;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantArrayBuilder;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantObjectBuilder;
import org.junit.Test;

public class TestReadVariant extends DirectWriterTest {

  private static final LogicalTypeAnnotation STRING = LogicalTypeAnnotation.stringType();

  // Construct a variant, and return the value binary, dropping metadata.
  private static Variant fullVariant(Consumer<VariantBuilder> appendValue) {
    VariantBuilder builder = new VariantBuilder();
    appendValue.accept(builder);
    return builder.build();
  }

  // Return only the value bytes, which is usually all we want.
  private static ByteBuffer variant(Consumer<VariantBuilder> appendValue) {
    return fullVariant(appendValue).getValueBuffer();
  }

  // Returns a value based on building with fixed metadata.
  private static ByteBuffer variant(ByteBuffer metadata, Consumer<VariantBuilder> appendValue) {
    ImmutableMetadata immutableMetadata = new ImmutableMetadata(metadata);
    VariantBuilder builder = new VariantBuilder(immutableMetadata);
    appendValue.accept(builder);
    return builder.encodedValue();
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

  private static class PrimitiveCase {
    Object avroValue;
    ByteBuffer value;

    PrimitiveCase(Object avroValue, ByteBuffer value) {
      this.avroValue = avroValue;
      this.value = value;
    }
  }

  /**
   * Convert a string to a Decimal that can be written using Avro.
   */
  private static Object avroDecimalValue(String s) {
    BigDecimal v = new BigDecimal(s);
    int precision = v.precision();
    if (precision <= 9) {
      return v.unscaledValue().intValueExact();
    } else if (precision <= 18) {
      return v.unscaledValue().longValueExact();
    } else {
      return v.unscaledValue().toByteArray();
    }
  }

  private static final PrimitiveCase[] PRIMITIVES = new PrimitiveCase[] {
    new PrimitiveCase(null, variant(b -> b.appendNull())),
    new PrimitiveCase(true, variant(b -> b.appendBoolean(true))),
    new PrimitiveCase(false, variant(b -> b.appendBoolean(false))),
    new PrimitiveCase((byte) 34, variant(b -> b.appendByte((byte) 34))),
    new PrimitiveCase((byte) -34, variant(b -> b.appendByte((byte) -34))),
    new PrimitiveCase((short) 1234, variant(b -> b.appendShort((short) 1234))),
    new PrimitiveCase((short) -1234, variant(b -> b.appendShort((short) -1234))),
    new PrimitiveCase(12345, variant(b -> b.appendInt(12345))),
    new PrimitiveCase(-12345, variant(b -> b.appendInt(-12345))),
    new PrimitiveCase(9876543210L, variant(b -> b.appendLong(9876543210L))),
    new PrimitiveCase(-9876543210L, variant(b -> b.appendLong(-9876543210L))),
    new PrimitiveCase(10.11F, variant(b -> b.appendFloat(10.11F))),
    new PrimitiveCase(-10.11F, variant(b -> b.appendFloat(-10.11F))),
    new PrimitiveCase(14.3D, variant(b -> b.appendDouble(14.3D))),
    new PrimitiveCase(-14.3D, variant(b -> b.appendDouble(-14.3D))),
    // Dates and timestamps aren't very interesting in Variant tests, since they are passed
    // to and from the API as integers. So just test arbitrary integer values.
    new PrimitiveCase(12345, variant(b -> b.appendDate(12345))),
    new PrimitiveCase(-12345, variant(b -> b.appendDate(-12345))),
    new PrimitiveCase(9876543210L, variant(b -> b.appendTimestampTz(9876543210L))),
    new PrimitiveCase(-9876543210L, variant(b -> b.appendTimestampTz(-9876543210L))),
    new PrimitiveCase(9876543210L, variant(b -> b.appendTimestampNtz(9876543210L))),
    new PrimitiveCase(-9876543210L, variant(b -> b.appendTimestampNtz(-9876543210L))),
    new PrimitiveCase(9876543210L, variant(b -> b.appendTimestampNanosTz(9876543210L))),
    new PrimitiveCase(-9876543210L, variant(b -> b.appendTimestampNanosTz(-9876543210L))),
    new PrimitiveCase(9876543210L, variant(b -> b.appendTimestampNanosNtz(9876543210L))),
    new PrimitiveCase(-9876543210L, variant(b -> b.appendTimestampNanosNtz(-9876543210L))),
    new PrimitiveCase(
        avroDecimalValue("123456.7890"),
        variant(b -> b.appendDecimal(new BigDecimal("123456.7890")))), // decimal4
    new PrimitiveCase(
        avroDecimalValue("-123456.7890"),
        variant(b -> b.appendDecimal(new BigDecimal("-123456.7890")))), // decimal4
    new PrimitiveCase(
        avroDecimalValue("1234567890.987654321"),
        variant(b -> b.appendDecimal(new BigDecimal("1234567890.987654321")))), // decimal8
    new PrimitiveCase(
        avroDecimalValue("-1234567890.987654321"),
        variant(b -> b.appendDecimal(new BigDecimal("-1234567890.987654321")))), // decimal8
    new PrimitiveCase(
        avroDecimalValue("9876543210.123456789"),
        variant(b -> b.appendDecimal(new BigDecimal("9876543210.123456789")))), // decimal16
    new PrimitiveCase(
        avroDecimalValue("-9876543210.123456789"),
        variant(b -> b.appendDecimal(new BigDecimal("-9876543210.123456789")))), // decimal16
    new PrimitiveCase(
        ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0x0c, 0x0d}),
        variant(b -> b.appendBinary(ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0x0c, 0x0d})))),
    new PrimitiveCase("parquet", variant(b -> b.appendString("parquet"))),
    new PrimitiveCase(
        UUID.fromString("f24f9b64-81fa-49d1-b74e-8c09a6e31c56"),
        variant(b -> b.appendUUID(UUID.fromString("f24f9b64-81fa-49d1-b74e-8c09a6e31c56"))))
  };

  private ByteBuffer EMPTY_METADATA = fullVariant(b -> b.appendNull()).getMetadataBuffer();
  private ByteBuffer NULL_VALUE = PRIMITIVES[0].value;

  private ByteBuffer TEST_METADATA;
  private ByteBuffer TEST_OBJECT;

  private static class TestCase {
    ByteBuffer value;
    ByteBuffer metadata;

    public TestCase(ByteBuffer value, ByteBuffer metadata) {
      this.value = value;
      this.metadata = metadata;
    }
  }

  private ArrayList<TestCase> testCases;

  public TestReadVariant() throws Exception {
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

    testCases = new ArrayList<>();
    for (PrimitiveCase p : PRIMITIVES) {
      testCases.add(new TestCase(p.value, EMPTY_METADATA));
    }
    testCases.add(new TestCase(TEST_OBJECT, TEST_METADATA));
  }

  @Test
  public void testUnshredded() throws Exception {
    // Unshredded Variant should produce exactly the same value and metadata.
    Variant testValue = fullVariant(b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("a");
      ob.appendLong(123);
      ob.appendKey("b");
      VariantArrayBuilder ab = ob.startArray();
      ab.appendString("a");
      ab.appendLong(2);
      ab.appendBoolean(true);
      ab.appendNull();
      ob.endArray();
      b.endObject();
    });
    Binary expectedValue = Binary.fromConstantByteBuffer(testValue.getValueBuffer());
    Binary expectedMetadata = Binary.fromConstantByteBuffer(testValue.getMetadataBuffer());
    // Test with value before metadata in the schema: Spark's initial implementation wrote in this
    // order. The read schema (set below) requires metadata before value, but the order in the file
    // schema shouldn't matter.
    Path test = writeDirect(
        "message VariantMessage {" + "  required group v (VARIANT(1)) {"
            + "    required binary value;"
            + "    required binary metadata;"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("v", 0);

          rc.startGroup();
          rc.startField("value", 0);
          rc.addBinary(expectedValue);
          rc.endField("value", 0);
          rc.startField("metadata", 1);
          rc.addBinary(expectedMetadata);
          rc.endField("metadata", 1);
          rc.endGroup();

          rc.endField("v", 0);
          rc.endMessage();
        });

    Schema variantSchema = record(
        "v",
        field("metadata", Schema.create(Schema.Type.BYTES)),
        field("value", Schema.create(Schema.Type.BYTES)));
    Schema expectedSchema = record("VariantMessage", field("v", variantSchema));

    GenericRecord expectedRecord = instance(
        expectedSchema,
        "v",
        instance(
            variantSchema,
            "metadata",
            expectedMetadata.toByteBuffer(),
            "value",
            expectedValue.toByteBuffer()));

    MessageType readSchema =
        MessageTypeParser.parseMessageType("message VariantMessage {" + "  required group v (VARIANT(1)) {"
            + "    required binary metadata;"
            + "    required binary value;"
            + "  }"
            + "}");
    Configuration conf = new Configuration();
    AvroReadSupport.setRequestedProjection(conf, avroSchema(readSchema));

    // both should behave the same way
    assertReaderContains(new AvroParquetReader(conf, test), expectedSchema, expectedRecord);
  }

  /**
   * Construct a Variant with a single scalar value, and write the same value to the typed_value column
   * of a shredded file, verifying that the reconstructed value is bit-for-bit identical to the original value.
   * and a lambda to append the same corresponding value to the
   *
   * @param type        Type of the shredded field. E.g. int64"
   * @param annotation  Logical annotation of the shredded field, or empty string if none. E.g. "UTF8"
   * @param appendValue Lambda to append a value to a VariantBuilder
   * @param addValue    Lambda to append the logically equivalent value to a RecordConsumer
   * @throws Exception
   */
  public void runOneScalarTest(
      String type, String annotation, Consumer<VariantBuilder> appendValue, Consumer<RecordConsumer> addValue)
      throws Exception {
    VariantBuilder builder = new VariantBuilder();
    appendValue.accept(builder);
    Variant testValue = builder.build();
    Binary expectedValue = Binary.fromConstantByteBuffer(testValue.getValueBuffer());
    Binary expectedMetadata = Binary.fromConstantByteBuffer(testValue.getMetadataBuffer());
    Path test = writeDirect(
        "message VariantMessage {" + "  required group v (VARIANT(1)) {"
            + "    optional binary value;"
            + "    required binary metadata;"
            + "    optional " + type + " typed_value " + annotation + ";"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("v", 0);

          rc.startGroup();
          rc.startField("typed_value", 2);
          addValue.accept(rc);
          rc.endField("typed_value", 2);
          rc.startField("metadata", 1);
          rc.addBinary(expectedMetadata);
          rc.endField("metadata", 1);
          rc.endGroup();

          rc.endField("v", 0);
          rc.endMessage();
        });

    Schema variantSchema = record(
        "v",
        field("metadata", Schema.create(Schema.Type.BYTES)),
        field("value", Schema.create(Schema.Type.BYTES)));
    Schema expectedSchema = record("VariantMessage", field("v", variantSchema));

    GenericRecord expectedRecord = instance(
        expectedSchema,
        "v",
        instance(
            variantSchema,
            "metadata",
            expectedMetadata.toByteBuffer(),
            "value",
            expectedValue.toByteBuffer()));

    // both should behave the same way
    assertReaderContains(new AvroParquetReader(new Configuration(), test), expectedSchema, expectedRecord);
  }

  @Test
  public void testShreddedScalar() throws Exception {
    runOneScalarTest("boolean", "", b -> b.appendBoolean(true), rc -> rc.addBoolean(true));
    // Test true and false, since they have different types in Variant.
    runOneScalarTest("boolean", "", b -> b.appendBoolean(false), rc -> rc.addBoolean(false));
    runOneScalarTest("boolean", "", b -> b.appendBoolean(false), rc -> rc.addBoolean(false));
    runOneScalarTest("int32", "(INT_8)", b -> b.appendByte((byte) 123), rc -> rc.addInteger(123));
    runOneScalarTest("int32", "(INT_16)", b -> b.appendShort((short) -12345), rc -> rc.addInteger(-12345));
    runOneScalarTest("int32", "(INT_32)", b -> b.appendInt(1234567890), rc -> rc.addInteger(1234567890));
    runOneScalarTest("int64", "", b -> b.appendLong(1234567890123L), rc -> rc.addLong(1234567890123L));
    runOneScalarTest("double", "", b -> b.appendDouble(1.2e34), rc -> rc.addDouble(1.2e34));
    runOneScalarTest("float", "", b -> b.appendFloat(1.2e34f), rc -> rc.addFloat(1.2e34f));
    runOneScalarTest(
        "int32", "(DECIMAL(9, 2))", b -> b.appendDecimal(new BigDecimal("1.23")), rc -> rc.addInteger(123));
    runOneScalarTest(
        "int64",
        "(DECIMAL(18, 5))",
        b -> b.appendDecimal(new BigDecimal("123456789.12345")),
        rc -> rc.addLong(12345678912345L));
    BigDecimal decimalVal = new BigDecimal("0.12345678901234567890123456789012345678");
    runOneScalarTest(
        "fixed_len_byte_array(16)",
        "(DECIMAL(38, 38))",
        b -> b.appendDecimal(decimalVal),
        rc -> rc.addBinary(
            Binary.fromConstantByteArray(decimalVal.unscaledValue().toByteArray())));
    // Verify that the parquet type's scale is used when shredding, and not the scale implied by the value.
    runOneScalarTest(
        "int32",
        "(DECIMAL(9, 2))",
        b -> b.appendDecimal(new BigDecimal("1.2").setScale(2)),
        rc -> rc.addInteger(120));
    runOneScalarTest(
        "int64",
        "(DECIMAL(18, 5))",
        b -> b.appendDecimal(new BigDecimal("123456789").setScale(5)),
        rc -> rc.addLong(12345678900000L));
    BigDecimal decimalVal2 = new BigDecimal("9.12345678901234567890123456789").setScale(37);
    runOneScalarTest(
        "fixed_len_byte_array(16)",
        "(DECIMAL(38, 37))",
        b -> b.appendDecimal(decimalVal2),
        rc -> rc.addBinary(
            Binary.fromConstantByteArray(decimalVal2.unscaledValue().toByteArray())));
    runOneScalarTest("int64", "(TIMESTAMP(MICROS, true))", b -> b.appendTimestampTz(123), rc -> rc.addLong(123));
    runOneScalarTest("int64", "(TIMESTAMP(MICROS, false))", b -> b.appendTimestampNtz(123), rc -> rc.addLong(123));
    runOneScalarTest(
        "binary",
        "",
        b -> b.appendBinary(Binary.fromString("hello").toByteBuffer()),
        rc -> rc.addBinary(Binary.fromString("hello")));
    runOneScalarTest(
        "binary", "(UTF8)", b -> b.appendString("hello"), rc -> rc.addBinary(Binary.fromString("hello")));
    runOneScalarTest("int64", "(TIME(MICROS, false))", b -> b.appendTime(123), rc -> rc.addLong(123));
    runOneScalarTest(
        "int64", "(TIMESTAMP(NANOS, true))", b -> b.appendTimestampNanosTz(123), rc -> rc.addLong(123));
    runOneScalarTest(
        "int64", "(TIMESTAMP(NANOS, false))", b -> b.appendTimestampNanosNtz(123), rc -> rc.addLong(123));
    UUID uuid = UUID.randomUUID();
    byte[] uuidBytes = new byte[16];
    ByteBuffer bb = ByteBuffer.wrap(uuidBytes, 0, 16).order(ByteOrder.BIG_ENDIAN);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    runOneScalarTest(
        "fixed_len_byte_array(16)",
        "(UUID)",
        b -> b.appendUUID(uuid),
        rc -> rc.addBinary(Binary.fromConstantByteArray(uuidBytes)));
  }

  @Test
  public void testArray() throws Exception {
    Variant testValue = fullVariant(b -> {
      VariantArrayBuilder ab = b.startArray();
      ab.appendLong(123);
      ab.appendString("Hello");
      ab.appendLong(456);
      b.endArray();
    });
    // The string value will be stored in `value` in Variant binary form.
    ByteBuffer stringValue = variant("Hello");

    Binary expectedValue = Binary.fromConstantByteBuffer(testValue.getValueBuffer());
    Binary expectedMetadata = Binary.fromConstantByteBuffer(testValue.getMetadataBuffer());
    Path test = writeDirect(
        "message VariantMessage {" + "  required group v (VARIANT(1)) {"
            + "    required binary metadata;"
            + "    optional binary value;"
            + "    optional group typed_value (LIST) {"
            + "      repeated group list {"
            + "        required group element {"
            + "          optional int64 typed_value;"
            + "          optional binary value;"
            + "        }"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("v", 0);
          rc.startGroup();
          rc.startField("typed_value", 2);
          rc.startGroup();
          rc.startField("list", 0);

          rc.startGroup();
          rc.startField("element", 0);
          rc.startGroup();
          rc.startField("typed_value", 0);
          rc.addLong(123);
          rc.endField("typed_value", 0);
          rc.endGroup();
          rc.endField("element", 0);
          rc.endGroup();

          rc.startGroup();
          rc.startField("element", 0);
          rc.startGroup();
          rc.startField("value", 1);
          rc.addBinary(Binary.fromConstantByteBuffer(stringValue));
          rc.endField("value", 1);
          rc.endGroup();
          rc.endField("element", 0);
          rc.endGroup();

          rc.startGroup();
          rc.startField("element", 0);
          rc.startGroup();
          rc.startField("typed_value", 0);
          rc.addLong(456);
          rc.endField("typed_value", 0);
          rc.endGroup();
          rc.endField("element", 0);
          rc.endGroup();

          rc.endField("list", 0);
          rc.endGroup();
          rc.endField("typed_value", 2);

          rc.startField("metadata", 0);
          rc.addBinary(expectedMetadata);
          rc.endField("metadata", 0);

          rc.endGroup();
          rc.endField("v", 0);
          rc.endMessage();
        });

    Schema variantSchema = record(
        "v",
        field("metadata", Schema.create(Schema.Type.BYTES)),
        field("value", Schema.create(Schema.Type.BYTES)));
    Schema expectedSchema = record("VariantMessage", field("v", variantSchema));

    GenericRecord expectedRecord = instance(
        expectedSchema,
        "v",
        instance(
            variantSchema,
            "metadata",
            expectedMetadata.toByteBuffer(),
            "value",
            expectedValue.toByteBuffer()));

    // both should behave the same way
    assertReaderContains(new AvroParquetReader(new Configuration(), test), expectedSchema, expectedRecord);
  }

  public <T extends IndexedRecord> void assertReaderContains(
      AvroParquetReader<T> reader, Schema expectedSchema, T... expectedRecords) throws IOException {
    for (T expectedRecord : expectedRecords) {
      T actualRecord = reader.read();
      assertEquals("Should match expected schema", expectedSchema, actualRecord.getSchema());
      assertEquals("Should match the expected record", expectedRecord, actualRecord);
    }
    assertNull(
        "Should only contain " + expectedRecords.length + " record" + (expectedRecords.length == 1 ? "" : "s"),
        reader.read());
  }

  // We need to store two copies of the schema: one without the Variant type annotation that is used to construct the
  // Avro schema for writing, and one with type annotation that is used in the actual written parquet schema, and when
  // reading.
  private static class TestSchema {
    MessageType parquetSchema;
    MessageType unannotatedParquetSchema;
    GroupType variantType;
    GroupType unannotatedVariantType;

    TestSchema(GroupType variantType, GroupType unannotatedVariantType) {
      this.variantType = variantType;
      this.unannotatedVariantType = unannotatedVariantType;
      this.parquetSchema = parquetSchema(variantType);
      this.unannotatedParquetSchema = parquetSchema(unannotatedVariantType);
    }

    TestSchema(Type shreddedType) {
      variantType = variant("var", 2, shreddedType);
      unannotatedVariantType = unannotatedVariant("var", 2, shreddedType);
      parquetSchema = parquetSchema(variantType);
      unannotatedParquetSchema = parquetSchema(unannotatedVariantType);
    }

    TestSchema() {
      variantType = variant("var", 2);
      unannotatedVariantType = unannotatedVariant("var", 2);
      parquetSchema = parquetSchema(variantType);
      unannotatedParquetSchema = parquetSchema(unannotatedVariantType);
    }
  }

  // The remaining tests in this file are based on Iceberg's TestVariantReaders suite.
  @Test
  public void testUnshreddedVariants() throws Exception {
    for (TestCase t : testCases) {
      TestSchema schema = new TestSchema();

      GenericRecord variant = recordFromMap(
          schema.unannotatedVariantType, ImmutableMap.of("metadata", t.metadata, "value", t.value));
      GenericRecord record =
          recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));
      GenericRecord actual = writeAndRead(schema, record);
      assertEquals(actual.get("id"), 1);

      GenericRecord actualVariant = (GenericRecord) actual.get("var");
      assertEquivalent(t.metadata, t.value, actualVariant);
    }
  }

  @Test
  public void testUnshreddedVariantsWithShreddingSchema() throws Exception {
    for (TestCase t : testCases) {
      // Parquet schema has a shredded field, but it is unused by the data.
      TestSchema schema = new TestSchema(shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));

      GenericRecord variant = recordFromMap(
          schema.unannotatedVariantType, ImmutableMap.of("metadata", t.metadata, "value", t.value));
      GenericRecord record =
          recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));
      GenericRecord actual = writeAndRead(schema, record);
      assertEquals(actual.get("id"), 1);

      GenericRecord actualVariant = (GenericRecord) actual.get("var");
      assertEquivalent(t.metadata, t.value, actualVariant);
    }
  }

  @Test
  public void testShreddedVariantPrimitives() throws IOException {
    for (PrimitiveCase p : PRIMITIVES) {
      if (p.avroValue == null) {
        // null isn't a valid type for shredding.
        continue;
      }
      TestSchema schema = new TestSchema(shreddedType(new Variant(p.value, EMPTY_METADATA)));

      GenericRecord variant = recordFromMap(
          schema.unannotatedVariantType,
          ImmutableMap.of("metadata", EMPTY_METADATA, "typed_value", p.avroValue));
      GenericRecord record =
          recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

      GenericRecord actual = writeAndRead(schema, record);
      assertEquals(actual.get("id"), 1);

      GenericRecord actualVariant = (GenericRecord) actual.get("var");
      assertEquivalent(EMPTY_METADATA, p.value, actualVariant);
    }
  }

  @Test
  public void testNullValueAndNullTypedValue() throws IOException {
    TestSchema schema = new TestSchema(shreddedPrimitive(PrimitiveTypeName.INT32));

    GenericRecord variant =
        recordFromMap(schema.unannotatedVariantType, ImmutableMap.of("metadata", EMPTY_METADATA));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);
    assertEquals(actual.get("id"), 1);

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(EMPTY_METADATA, NULL_VALUE, actualVariant);
  }

  @Test
  public void testMissingValueColumn() throws IOException {
    GroupType variantType = Types.buildGroup(Type.Repetition.REQUIRED)
        .as(LogicalTypeAnnotation.variantType((byte) 1))
        .id(2)
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .addField(shreddedPrimitive(PrimitiveTypeName.INT32))
        .named("var");

    GroupType unannotatedVariantType = Types.buildGroup(Type.Repetition.REQUIRED)
        .id(2)
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .addField(shreddedPrimitive(PrimitiveTypeName.INT32))
        .named("var");

    TestSchema schema = new TestSchema(variantType, unannotatedVariantType);

    GenericRecord variant =
        recordFromMap(unannotatedVariantType, ImmutableMap.of("metadata", EMPTY_METADATA, "typed_value", 34));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);
    assertEquals(actual.get("id"), 1);

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(EMPTY_METADATA, variant(34), actualVariant);
  }

  @Test
  public void testValueAndTypedValueConflict() throws IOException {
    TestSchema schema = new TestSchema(shreddedPrimitive(PrimitiveTypeName.INT32));

    GenericRecord variant = recordFromMap(
        schema.unannotatedVariantType,
        ImmutableMap.of("metadata", EMPTY_METADATA, "value", variant("str"), "typed_value", 34));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    assertThrows(
        () -> writeAndRead(schema, record),
        IllegalStateException.class,
        "Cannot call multiple append() methods");
  }

  @Test
  public void testUnsignedInteger() {
    TestSchema schema =
        new TestSchema(shreddedPrimitive(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, false)));

    GenericRecord variant =
        recordFromMap(schema.unannotatedVariantType, ImmutableMap.of("metadata", EMPTY_METADATA));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    assertThrows(
        () -> writeAndRead(schema, record),
        UnsupportedOperationException.class,
        "Unsupported shredded value type: INTEGER(32,false)");
  }

  @Test
  public void testFixedLengthByteArray() {
    Type shreddedType =
        Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(4).named("typed_value");
    TestSchema schema = new TestSchema(shreddedType);

    GenericRecord variant =
        recordFromMap(schema.unannotatedVariantType, ImmutableMap.of("metadata", EMPTY_METADATA));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    assertThrows(
        () -> writeAndRead(schema, record),
        UnsupportedOperationException.class,
        "Unsupported shredded value type: optional fixed_len_byte_array(4) typed_value");
  }

  @Test
  public void testShreddedObject() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    TestSchema schema = new TestSchema(objectFields);

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of("value", NULL_VALUE));
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of("typed_value", ""));
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);
    assertEquals(actual.get("id"), 1);

    ByteBuffer expectedValue = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("a");
      ob.appendNull();
      ob.appendKey("b");
      ob.appendString("");
      b.endObject();
    });

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue, actualVariant);
  }

  @Test
  public void testShreddedObjectMissingValueColumn() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    GroupType variantType = Types.buildGroup(Type.Repetition.REQUIRED)
        .id(2)
        .as(LogicalTypeAnnotation.variantType((byte) 1))
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .addField(objectFields)
        .named("var");

    GroupType unannotatedVariantType = Types.buildGroup(Type.Repetition.REQUIRED)
        .id(2)
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .addField(objectFields)
        .named("var");

    TestSchema schema = new TestSchema(variantType, unannotatedVariantType);

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of("value", variant(1234)));
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of("typed_value", "iceberg"));
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);
    assertEquals(actual.get("id"), 1);

    ByteBuffer expectedValue = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("a");
      ob.appendInt(1234);
      ob.appendKey("b");
      ob.appendString("iceberg");
      b.endObject();
    });

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue, actualVariant);
  }

  @Test
  public void testShreddedObjectMissingField() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    TestSchema schema = new TestSchema(objectFields);

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of("value", variant(b -> b.appendBoolean(false))));
    // value and typed_value are null, but a struct for b is required
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of());
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);

    assertEquals(actual.get("id"), 1);

    ByteBuffer expectedValue = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("a");
      ob.appendBoolean(false);
      b.endObject();
    });

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue, actualVariant);
  }

  @Test
  public void testEmptyShreddedObject() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    TestSchema schema = new TestSchema(objectFields);

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of()); // missing
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of()); // missing
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);

    assertEquals(actual.get("id"), 1);

    ByteBuffer expectedValue = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      b.endObject();
    });

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue, actualVariant);
  }

  @Test
  public void testShreddedObjectMissingFieldValueColumn() throws IOException {
    // field groups do not have value
    GroupType fieldA = Types.buildGroup(Type.Repetition.REQUIRED)
        .addField(shreddedPrimitive(PrimitiveTypeName.INT32))
        .named("a");
    GroupType fieldB = Types.buildGroup(Type.Repetition.REQUIRED)
        .addField(shreddedPrimitive(PrimitiveTypeName.BINARY, STRING))
        .named("b");
    GroupType objectFields = Types.buildGroup(Type.Repetition.OPTIONAL)
        .addFields(fieldA, fieldB)
        .named("typed_value");
    TestSchema schema = new TestSchema(objectFields);

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of()); // typed_value=null
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of("typed_value", "iceberg"));
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);

    assertEquals(actual.get("id"), 1);

    ByteBuffer expectedValue = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("b");
      ob.appendString("iceberg");
      b.endObject();
    });

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue, actualVariant);
  }

  @Test
  public void testShreddedObjectMissingTypedValue() throws IOException {
    // field groups do not have typed_value
    GroupType fieldA = Types.buildGroup(Type.Repetition.REQUIRED)
        .optional(PrimitiveTypeName.BINARY)
        .named("value")
        .named("a");
    GroupType fieldB = Types.buildGroup(Type.Repetition.REQUIRED)
        .optional(PrimitiveTypeName.BINARY)
        .named("value")
        .named("b");
    GroupType objectFields = Types.buildGroup(Type.Repetition.OPTIONAL)
        .addFields(fieldA, fieldB)
        .named("typed_value");
    TestSchema schema = new TestSchema(objectFields);

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of()); // value=null
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of("value", variant("iceberg")));
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);

    assertEquals(actual.get("id"), 1);

    ByteBuffer expectedValue = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("b");
      ob.appendString("iceberg");
      b.endObject();
    });

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue, actualVariant);
  }

  @Test
  public void testShreddedObjectWithinShreddedObject() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType innerFields = objectFields(fieldA, fieldB);
    GroupType fieldC = shreddedField("c", innerFields);
    GroupType fieldD = shreddedField("d", shreddedPrimitive(PrimitiveTypeName.DOUBLE));
    GroupType outerFields = objectFields(fieldC, fieldD);
    TestSchema schema = new TestSchema(outerFields);

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of("typed_value", 34));
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of("typed_value", "iceberg"));
    GenericRecord inner = recordFromMap(innerFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord recordC = recordFromMap(fieldC, ImmutableMap.of("typed_value", inner));
    GenericRecord recordD = recordFromMap(fieldD, ImmutableMap.of("typed_value", -0.0D));
    GenericRecord outer = recordFromMap(outerFields, ImmutableMap.of("c", recordC, "d", recordD));
    GenericRecord variant = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", outer));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);

    assertEquals(actual.get("id"), 1);

    ByteBuffer expectedValue = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("c");
      VariantObjectBuilder innerOb = ob.startObject();
      innerOb.appendKey("a");
      innerOb.appendInt(34);
      innerOb.appendKey("b");
      innerOb.appendString("iceberg");
      ob.endObject();
      ob.appendKey("d");
      ob.appendDouble(-0.0D);
      b.endObject();
    });

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue, actualVariant);
  }

  @Test
  public void testShreddedObjectWithOptionalFieldStructs() throws IOException {
    // fields use an incorrect OPTIONAL struct of value and typed_value to test definition levels
    GroupType fieldA = Types.buildGroup(Type.Repetition.OPTIONAL)
        .optional(PrimitiveTypeName.BINARY)
        .named("value")
        .addField(shreddedPrimitive(PrimitiveTypeName.INT32))
        .named("a");
    GroupType fieldB = Types.buildGroup(Type.Repetition.OPTIONAL)
        .optional(PrimitiveTypeName.BINARY)
        .named("value")
        .addField(shreddedPrimitive(PrimitiveTypeName.BINARY, STRING))
        .named("b");
    GroupType fieldC = Types.buildGroup(Type.Repetition.OPTIONAL)
        .optional(PrimitiveTypeName.BINARY)
        .named("value")
        .addField(shreddedPrimitive(PrimitiveTypeName.DOUBLE))
        .named("c");
    GroupType fieldD = Types.buildGroup(Type.Repetition.OPTIONAL)
        .optional(PrimitiveTypeName.BINARY)
        .named("value")
        .addField(shreddedPrimitive(PrimitiveTypeName.BOOLEAN))
        .named("d");
    GroupType objectFields = Types.buildGroup(Type.Repetition.OPTIONAL)
        .addFields(fieldA, fieldB, fieldC, fieldD)
        .named("typed_value");
    TestSchema schema = new TestSchema(objectFields);

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of("value", variant(34)));
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of("typed_value", "iceberg"));
    GenericRecord recordC = recordFromMap(fieldC, ImmutableMap.of()); // c.value and c.typed_value are missing
    GenericRecord fields =
        recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB, "c", recordC)); // d is missing
    GenericRecord variant = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);

    assertEquals(actual.get("id"), 1);

    ByteBuffer expectedValue = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("a");
      ob.appendInt(34);
      ob.appendKey("b");
      ob.appendString("iceberg");
      b.endObject();
    });

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue, actualVariant);
  }

  @Test
  public void testPartiallyShreddedObjectOutOfOrder() throws IOException {
    // The schema is not in alphabetical order, and the unshredded field is also not.
    // The resulting object should be logically the same (i.e. the offset list must be in
    // alphabetical order), but the layout of the values in the binary may differ.
    GroupType fieldD = shreddedField("d", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldD, fieldA);
    TestSchema schema = new TestSchema(objectFields);

    ByteBuffer baseObject = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("b");
      ob.appendDate(12345);
      b.endObject();
    });

    GenericRecord recordA = recordFromMap(fieldD, ImmutableMap.of("value", NULL_VALUE));
    GenericRecord recordB = recordFromMap(fieldA, ImmutableMap.of("typed_value", "iceberg"));
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("d", recordA, "a", recordB));
    GenericRecord variant = recordFromMap(
        schema.unannotatedVariantType,
        ImmutableMap.of("metadata", TEST_METADATA, "value", baseObject, "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);

    assertEquals(actual.get("id"), 1);

    ByteBuffer expectedValue = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("a");
      ob.appendString("iceberg");
      ob.appendKey("b");
      ob.appendDate(12345);
      ob.appendKey("d");
      ob.appendNull();
      b.endObject();
    });

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue, actualVariant);
  }

  @Test
  public void testPartiallyShreddedObject() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    TestSchema schema = new TestSchema(objectFields);

    ByteBuffer baseObject = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("d");
      ob.appendDate(12345);
      b.endObject();
    });

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of("value", NULL_VALUE));
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of("typed_value", "iceberg"));
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant = recordFromMap(
        schema.unannotatedVariantType,
        ImmutableMap.of("metadata", TEST_METADATA, "value", baseObject, "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);

    assertEquals(actual.get("id"), 1);

    ByteBuffer expectedValue = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("a");
      ob.appendNull();
      ob.appendKey("b");
      ob.appendString("iceberg");
      ob.appendKey("d");
      ob.appendDate(12345);
      b.endObject();
    });

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue, actualVariant);
  }

  @Test
  public void testPartiallyShreddedObjectFieldConflict() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    TestSchema schema = new TestSchema(objectFields);

    ByteBuffer baseObject = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("b");
      ob.appendDate(12345);
      b.endObject();
    });

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of("value", NULL_VALUE));
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of("typed_value", "iceberg"));
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant = recordFromMap(
        schema.unannotatedVariantType,
        ImmutableMap.of("metadata", TEST_METADATA, "value", baseObject, "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);

    assertEquals(actual.get("id"), 1);

    // The reader is expected to ignore fields in value that are present in the typed_value schema.
    // This matches Iceberg behaviour, but we could also consider failing with an error.
    ByteBuffer expectedValue = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("a");
      ob.appendNull();
      ob.appendKey("b");
      ob.appendString("iceberg");
      b.endObject();
    });

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue, actualVariant);
  }

  @Test
  public void testPartiallyShreddedObjectMissingFieldConflict() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    TestSchema schema = new TestSchema(objectFields);

    ByteBuffer baseObject = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("b");
      ob.appendDate(12345);
      b.endObject();
    });

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of("value", NULL_VALUE));
    // value and typed_value are null, but a struct for b is required
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of());
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant = recordFromMap(
        schema.unannotatedVariantType,
        ImmutableMap.of("metadata", TEST_METADATA, "value", baseObject, "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);

    assertEquals(actual.get("id"), 1);

    // The reader is expected to ignore fields in value that are present in the typed_value schema, even if they are
    // missing in typed_value.
    ByteBuffer expectedValue = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("a");
      ob.appendNull();
      b.endObject();
    });

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, expectedValue, actualVariant);
  }

  @Test
  public void testNonObjectWithNullShreddedFields() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    TestSchema schema = new TestSchema(objectFields);

    GenericRecord variant = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "value", variant(34)));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    GenericRecord actual = writeAndRead(schema, record);
    assertEquals(actual.get("id"), 1);

    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(TEST_METADATA, variant(34), actualVariant);
  }

  @Test
  public void testNonObjectWithNonNullShreddedFields() {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    TestSchema schema = new TestSchema(objectFields);

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of("value", NULL_VALUE));
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of("value", variant(9876543210L)));
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant = recordFromMap(
        schema.unannotatedVariantType,
        ImmutableMap.of("metadata", TEST_METADATA, "value", variant(34), "typed_value", fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    assertThrows(
        () -> writeAndRead(schema, record),
        IllegalStateException.class,
        "Invalid variant, conflicting value and typed_value");
  }

  @Test
  public void testEmptyPartiallyShreddedObjectConflict() {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    TestSchema schema = new TestSchema(objectFields);

    GenericRecord recordA = recordFromMap(fieldA, ImmutableMap.of()); // missing
    GenericRecord recordB = recordFromMap(fieldB, ImmutableMap.of()); // missing
    GenericRecord fields = recordFromMap(objectFields, ImmutableMap.of("a", recordA, "b", recordB));
    GenericRecord variant = recordFromMap(
        schema.unannotatedVariantType,
        ImmutableMap.of(
            "metadata",
            TEST_METADATA,
            "value",
            NULL_VALUE, // conflicting non-object
            "typed_value",
            fields));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    assertThrows(
        () -> writeAndRead(schema, record),
        IllegalStateException.class,
        "Invalid variant, conflicting value and typed_value");
  }

  @Test
  public void testMixedRecords() throws IOException {
    // tests multiple rows to check that Parquet columns are correctly advanced
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType innerFields = objectFields(fieldA, fieldB);
    GroupType fieldC = shreddedField("c", innerFields);
    GroupType fieldD = shreddedField("d", shreddedPrimitive(PrimitiveTypeName.DOUBLE));
    GroupType outerFields = objectFields(fieldC, fieldD);
    TestSchema schema = new TestSchema(outerFields);

    GenericRecord zero = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 0));

    GenericRecord a1 = recordFromMap(fieldA, ImmutableMap.of()); // missing
    GenericRecord b1 = recordFromMap(fieldB, ImmutableMap.of("typed_value", "iceberg"));
    GenericRecord inner1 = recordFromMap(innerFields, ImmutableMap.of("a", a1, "b", b1));
    GenericRecord c1 = recordFromMap(fieldC, ImmutableMap.of("typed_value", inner1));
    GenericRecord d1 = recordFromMap(fieldD, ImmutableMap.of()); // missing
    GenericRecord outer1 = recordFromMap(outerFields, ImmutableMap.of("c", c1, "d", d1));
    GenericRecord variant1 = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", outer1));
    GenericRecord one = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant1));

    ByteBuffer expectedOne = variant(TEST_METADATA, b -> {
      VariantObjectBuilder outerObj = b.startObject();
      outerObj.appendKey("c");
      VariantObjectBuilder innerObj = outerObj.startObject();
      innerObj.appendKey("b");
      innerObj.appendString("iceberg");
      outerObj.endObject();
      b.endObject();
    });

    GenericRecord c2 = recordFromMap(fieldC, ImmutableMap.of("value", variant(8)));
    GenericRecord d2 = recordFromMap(fieldD, ImmutableMap.of("typed_value", -0.0D));
    GenericRecord outer2 = recordFromMap(outerFields, ImmutableMap.of("c", c2, "d", d2));
    GenericRecord variant2 = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", outer2));
    GenericRecord two = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 2, "var", variant2));

    ByteBuffer expectedTwo = variant(TEST_METADATA, b -> {
      VariantObjectBuilder outerObj = b.startObject();
      outerObj.appendKey("c");
      outerObj.appendInt(8);
      outerObj.appendKey("d");
      outerObj.appendDouble(-0.0D);
      b.endObject();
    });

    GenericRecord a3 = recordFromMap(fieldA, ImmutableMap.of("typed_value", 34));
    GenericRecord b3 = recordFromMap(fieldB, ImmutableMap.of("value", variant("")));
    GenericRecord inner3 = recordFromMap(innerFields, ImmutableMap.of("a", a3, "b", b3));
    GenericRecord c3 = recordFromMap(fieldC, ImmutableMap.of("typed_value", inner3));
    GenericRecord d3 = recordFromMap(fieldD, ImmutableMap.of("typed_value", 0.0D));
    GenericRecord outer3 = recordFromMap(outerFields, ImmutableMap.of("c", c3, "d", d3));
    GenericRecord variant3 = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", outer3));
    GenericRecord three = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 3, "var", variant3));

    ByteBuffer expectedThree = variant(TEST_METADATA, b -> {
      VariantObjectBuilder outerObj = b.startObject();
      outerObj.appendKey("c");
      VariantObjectBuilder innerObj = outerObj.startObject();
      innerObj.appendKey("a");
      innerObj.appendInt(34);
      innerObj.appendKey("b");
      innerObj.appendString("");
      outerObj.endObject();
      outerObj.appendKey("d");
      outerObj.appendDouble(0.0D);
      b.endObject();
    });

    List<GenericRecord> records = writeAndRead(schema, Arrays.asList(zero, one, two, three));
    assertEquals(records.size(), 4);

    GenericRecord actualZero = records.get(0);
    assertEquals(actualZero.get("id"), 0);
    assertEquals(actualZero.get("var"), null);

    GenericRecord actualOne = records.get(1);
    assertEquals(actualOne.get("id"), 1);
    GenericRecord actualOneVariant = (GenericRecord) actualOne.get("var");
    assertEquivalent(TEST_METADATA, expectedOne, actualOneVariant);

    GenericRecord actualTwo = records.get(2);
    assertEquals(actualTwo.get("id"), 2);
    GenericRecord actualTwoVariant = (GenericRecord) actualTwo.get("var");
    assertEquivalent(TEST_METADATA, expectedTwo, actualTwoVariant);

    GenericRecord actualThree = records.get(3);
    assertEquals(actualThree.get("id"), 3);
    GenericRecord actualThreeVariant = (GenericRecord) actualThree.get("var");
    assertEquivalent(TEST_METADATA, expectedThree, actualThreeVariant);
  }

  @Test
  public void testSimpleArray() throws IOException {
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    GroupType elementType = element(shreddedType);
    TestSchema schema = new TestSchema(list(elementType));

    List<GenericRecord> arr = Arrays.asList(
        recordFromMap(elementType, ImmutableMap.of("typed_value", "comedy")),
        recordFromMap(elementType, ImmutableMap.of("typed_value", "drama")));

    GenericRecord var = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", EMPTY_METADATA, "typed_value", arr));
    GenericRecord row = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", var));

    ByteBuffer expected = variant(b -> {
      VariantArrayBuilder ab = b.startArray();
      ab.appendString("comedy");
      ab.appendString("drama");
      b.endArray();
    });

    GenericRecord actual = writeAndRead(schema, row);
    assertEquals(actual.get("id"), 1);
    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(EMPTY_METADATA, expected, actualVariant);
  }

  @Test
  public void testNullArray() throws IOException {
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    TestSchema schema = new TestSchema(list(element(shreddedType)));

    GenericRecord var = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", EMPTY_METADATA, "value", NULL_VALUE));
    GenericRecord row = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", var));

    GenericRecord actual = writeAndRead(schema, row);
    assertEquals(actual.get("id"), 1);
    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(EMPTY_METADATA, NULL_VALUE, actualVariant);
  }

  @Test
  public void testEmptyArray() throws IOException {
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    TestSchema schema = new TestSchema(list(element(shreddedType)));

    List<GenericRecord> arr = Arrays.asList();
    GenericRecord var = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", EMPTY_METADATA, "typed_value", arr));
    GenericRecord row = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", var));

    GenericRecord actual = writeAndRead(schema, row);

    ByteBuffer emptyArray = variant(b -> {
      b.startArray();
      b.endArray();
    });

    assertEquals(actual.get("id"), 1);
    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(EMPTY_METADATA, emptyArray, actualVariant);
  }

  @Test
  public void testArrayWithNull() throws IOException {
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    GroupType elementType = element(shreddedType);
    TestSchema schema = new TestSchema(list(elementType));

    List<GenericRecord> arr = Arrays.asList(
        recordFromMap(elementType, ImmutableMap.of("typed_value", "comedy")),
        recordFromMap(elementType, ImmutableMap.of("value", NULL_VALUE)),
        recordFromMap(elementType, ImmutableMap.of("typed_value", "drama")));

    GenericRecord var = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", EMPTY_METADATA, "typed_value", arr));
    GenericRecord row = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", var));

    ByteBuffer expected = variant(b -> {
      VariantArrayBuilder ab = b.startArray();
      ab.appendString("comedy");
      ab.appendNull();
      ab.appendString("drama");
      b.endArray();
    });

    GenericRecord actual = writeAndRead(schema, row);
    assertEquals(actual.get("id"), 1);
    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(EMPTY_METADATA, expected, actualVariant);
  }

  @Test
  public void testNestedArray() throws IOException {
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    GroupType elementType = element(shreddedType);
    GroupType outerElementType = element(list(elementType));
    TestSchema schema = new TestSchema(list(outerElementType));

    List<GenericRecord> inner1 = Arrays.asList(
        recordFromMap(elementType, ImmutableMap.of("typed_value", "comedy")),
        recordFromMap(elementType, ImmutableMap.of("typed_value", "drama")));
    List<GenericRecord> outer1 = Arrays.asList(
        recordFromMap(outerElementType, ImmutableMap.of("typed_value", inner1)),
        recordFromMap(outerElementType, ImmutableMap.of("typed_value", Arrays.asList())));
    GenericRecord var = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", EMPTY_METADATA, "typed_value", outer1));
    GenericRecord row = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", var));

    ByteBuffer expected = variant(b -> {
      VariantArrayBuilder ab = b.startArray();
      VariantArrayBuilder inner = ab.startArray();
      inner.appendString("comedy");
      inner.appendString("drama");
      ab.endArray();
      ab.startArray();
      ab.endArray();
      b.endArray();
    });

    GenericRecord actual = writeAndRead(schema, row);
    assertEquals(actual.get("id"), 1);
    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(EMPTY_METADATA, expected, actualVariant);
  }

  @Test
  public void testArrayWithNestedObject() throws IOException {
    GroupType fieldA = shreddedField("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = shreddedField("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType shreddedFields = objectFields(fieldA, fieldB);
    GroupType elementType = element(shreddedFields);
    GroupType listType = list(elementType);
    TestSchema schema = new TestSchema(listType);

    // Row 1 with nested fully shredded object
    GenericRecord shredded1 = recordFromMap(
        shreddedFields,
        ImmutableMap.of(
            "a",
            recordFromMap(fieldA, ImmutableMap.of("typed_value", 1)),
            "b",
            recordFromMap(fieldB, ImmutableMap.of("typed_value", "comedy"))));
    GenericRecord shredded2 = recordFromMap(
        shreddedFields,
        ImmutableMap.of(
            "a",
            recordFromMap(fieldA, ImmutableMap.of("typed_value", 2)),
            "b",
            recordFromMap(fieldB, ImmutableMap.of("typed_value", "drama"))));
    List<GenericRecord> arr1 = Arrays.asList(
        recordFromMap(elementType, ImmutableMap.of("typed_value", shredded1)),
        recordFromMap(elementType, ImmutableMap.of("typed_value", shredded2)));
    GenericRecord var1 = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", arr1));
    GenericRecord row1 = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", var1));

    ByteBuffer expected1 = variant(TEST_METADATA, b -> {
      VariantArrayBuilder ab = b.startArray();
      VariantObjectBuilder ob1 = ab.startObject();
      ob1.appendKey("a");
      ob1.appendInt(1);
      ob1.appendKey("b");
      ob1.appendString("comedy");
      ab.endObject();

      VariantObjectBuilder ob2 = ab.startObject();
      ob2.appendKey("a");
      ob2.appendInt(2);
      ob2.appendKey("b");
      ob2.appendString("drama");
      ab.endObject();
      b.endArray();
    });

    // Row 2 with nested partially shredded object
    GenericRecord shredded3 = recordFromMap(
        shreddedFields,
        ImmutableMap.of(
            "a",
            recordFromMap(fieldA, ImmutableMap.of("typed_value", 3)),
            "b",
            recordFromMap(fieldB, ImmutableMap.of("typed_value", "action"))));

    ByteBuffer baseObject3 = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("c");
      ob.appendString("str");
      b.endObject();
    });

    GenericRecord shredded4 = recordFromMap(
        shreddedFields,
        ImmutableMap.of(
            "a",
            recordFromMap(fieldA, ImmutableMap.of("typed_value", 4)),
            "b",
            recordFromMap(fieldB, ImmutableMap.of("typed_value", "horror"))));

    ByteBuffer baseObject4 = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("d");
      ob.appendDate(12345);
      b.endObject();
    });

    List<GenericRecord> arr2 = Arrays.asList(
        recordFromMap(elementType, ImmutableMap.of("value", baseObject3, "typed_value", shredded3)),
        recordFromMap(elementType, ImmutableMap.of("value", baseObject4, "typed_value", shredded4)));
    GenericRecord var2 = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", arr2));
    GenericRecord row2 = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 2, "var", var2));

    ByteBuffer expected2 = variant(TEST_METADATA, b -> {
      VariantArrayBuilder ab = b.startArray();

      VariantObjectBuilder ob1 = ab.startObject();
      ob1.appendKey("a");
      ob1.appendInt(3);
      ob1.appendKey("b");
      ob1.appendString("action");
      ob1.appendKey("c");
      ob1.appendString("str");
      ab.endObject();

      VariantObjectBuilder ob2 = ab.startObject();
      ob2.appendKey("a");
      ob2.appendInt(4);
      ob2.appendKey("b");
      ob2.appendString("horror");
      ob2.appendKey("d");
      ob2.appendDate(12345);
      ab.endObject();

      b.endArray();
    });

    // verify
    List<GenericRecord> actual = writeAndRead(schema, Arrays.asList(row1, row2));
    GenericRecord actual1 = actual.get(0);
    assertEquals(actual1.get("id"), 1);
    GenericRecord actualVariant1 = (GenericRecord) actual1.get("var");
    assertEquivalent(TEST_METADATA, expected1, actualVariant1);

    GenericRecord actual2 = actual.get(1);
    assertEquals(actual2.get("id"), 2);
    GenericRecord actualVariant2 = (GenericRecord) actual2.get("var");
    assertEquivalent(TEST_METADATA, expected2, actualVariant2);
  }

  @Test
  public void testArrayWithNonArray() throws IOException {
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    GroupType elementType = element(shreddedType);
    TestSchema schema = new TestSchema(list(elementType));

    List<GenericRecord> arr1 = Arrays.asList(
        recordFromMap(elementType, ImmutableMap.of("typed_value", "comedy")),
        recordFromMap(elementType, ImmutableMap.of("typed_value", "drama")));
    GenericRecord var1 = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", EMPTY_METADATA, "typed_value", arr1));
    GenericRecord row1 = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", var1));

    ByteBuffer expectedArray1 = variant(b -> {
      VariantArrayBuilder ab = b.startArray();
      ab.appendString("comedy");
      ab.appendString("drama");
      b.endArray();
    });

    GenericRecord var2 = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", EMPTY_METADATA, "value", variant(34)));
    GenericRecord row2 = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 2, "var", var2));

    ByteBuffer expectedValue2 = variant(34);

    GenericRecord var3 = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "value", TEST_OBJECT));
    GenericRecord row3 = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 3, "var", var3));

    ByteBuffer expectedObject3 = variant(TEST_METADATA, b -> {
      VariantObjectBuilder ob = b.startObject();
      ob.appendKey("a");
      ob.appendNull();
      ob.appendKey("d");
      ob.appendString("iceberg");
      b.endObject();
    });

    // Test array is read properly after a non-array
    List<GenericRecord> arr4 = Arrays.asList(
        recordFromMap(elementType, ImmutableMap.of("typed_value", "action")),
        recordFromMap(elementType, ImmutableMap.of("typed_value", "horror")));
    GenericRecord var4 = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", TEST_METADATA, "typed_value", arr4));
    GenericRecord row4 = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 4, "var", var4));

    ByteBuffer expectedArray4 = variant(TEST_METADATA, b -> {
      VariantArrayBuilder ab = b.startArray();
      ab.appendString("action");
      ab.appendString("horror");
      b.endArray();
    });

    List<GenericRecord> actual = writeAndRead(schema, Arrays.asList(row1, row2, row3, row4));
    GenericRecord actual1 = actual.get(0);
    assertEquals(actual1.get("id"), 1);
    GenericRecord actualVariant1 = (GenericRecord) actual1.get("var");
    assertEquivalent(EMPTY_METADATA, expectedArray1, actualVariant1);

    GenericRecord actual2 = actual.get(1);
    assertEquals(actual2.get("id"), 2);
    GenericRecord actualVariant2 = (GenericRecord) actual2.get("var");
    assertEquivalent(EMPTY_METADATA, expectedValue2, actualVariant2);

    GenericRecord actual3 = actual.get(2);
    assertEquals(actual3.get("id"), 3);
    GenericRecord actualVariant3 = (GenericRecord) actual3.get("var");
    assertEquivalent(TEST_METADATA, expectedObject3, actualVariant3);

    GenericRecord actual4 = actual.get(3);
    assertEquals(actual4.get("id"), 4);
    GenericRecord actualVariant4 = (GenericRecord) actual4.get("var");
    assertEquivalent(TEST_METADATA, expectedArray4, actualVariant4);
  }

  @Test
  public void testArrayMissingValueColumn() throws IOException {
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    GroupType elementType = element(shreddedType);
    GroupType unannotatedVariantType = Types.buildGroup(Type.Repetition.OPTIONAL)
        .id(2)
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .addField(list(elementType))
        .named("var");

    GroupType variantType = Types.buildGroup(Type.Repetition.OPTIONAL)
        .id(2)
        .as(LogicalTypeAnnotation.variantType((byte) 1))
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .addField(list(elementType))
        .named("var");

    TestSchema schema = new TestSchema(variantType, unannotatedVariantType);

    List<GenericRecord> arr = Arrays.asList(
        recordFromMap(elementType, ImmutableMap.of("typed_value", "comedy")),
        recordFromMap(elementType, ImmutableMap.of("typed_value", "drama")));
    GenericRecord var = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", EMPTY_METADATA, "typed_value", arr));
    GenericRecord row = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", var));

    ByteBuffer expectedArray = variant(b -> {
      VariantArrayBuilder ab = b.startArray();
      ab.appendString("comedy");
      ab.appendString("drama");
      b.endArray();
    });

    GenericRecord actual = writeAndRead(schema, row);
    assertEquals(actual.get("id"), 1);
    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(EMPTY_METADATA, expectedArray, actualVariant);
  }

  @Test
  public void testArrayMissingElementValueColumn() throws IOException {
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    GroupType elementType = Types.buildGroup(Type.Repetition.REQUIRED)
        .addField(shreddedType)
        .named("element");

    TestSchema schema = new TestSchema(list(elementType));

    List<GenericRecord> arr = Arrays.asList(
        recordFromMap(elementType, ImmutableMap.of("typed_value", "comedy")),
        recordFromMap(elementType, ImmutableMap.of("typed_value", "drama")));
    GenericRecord var = recordFromMap(
        schema.unannotatedVariantType, ImmutableMap.of("metadata", EMPTY_METADATA, "typed_value", arr));
    GenericRecord row = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", var));

    ByteBuffer expectedArray = variant(b -> {
      VariantArrayBuilder ab = b.startArray();
      ab.appendString("comedy");
      ab.appendString("drama");
      b.endArray();
    });

    GenericRecord actual = writeAndRead(schema, row);
    assertEquals(actual.get("id"), 1);
    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(EMPTY_METADATA, expectedArray, actualVariant);
  }

  @Test
  public void testArrayWithElementNullValueAndNullTypedValue() throws IOException {
    // Test the invalid case that both value and typed_value of an element are null
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    GroupType elementType = element(shreddedType);

    TestSchema schema = new TestSchema(list(elementType));

    GenericRecord element = recordFromMap(elementType, ImmutableMap.of());
    GenericRecord variant = recordFromMap(
        schema.unannotatedVariantType,
        ImmutableMap.of("metadata", EMPTY_METADATA, "typed_value", Arrays.asList(element)));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    ByteBuffer expectedArray = variant(b -> {
      VariantArrayBuilder ab = b.startArray();
      ab.appendNull();
      b.endArray();
    });

    GenericRecord actual = writeAndRead(schema, record);
    assertEquals(actual.get("id"), 1);
    GenericRecord actualVariant = (GenericRecord) actual.get("var");
    assertEquivalent(EMPTY_METADATA, expectedArray, actualVariant);
  }

  @Test
  public void testArrayWithElementValueTypedValueConflict() {
    // Test the invalid case that both value and typed_value of an element are not null
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    GroupType elementType = element(shreddedType);
    TestSchema schema = new TestSchema(list(elementType));

    GenericRecord element =
        recordFromMap(elementType, ImmutableMap.of("value", variant(3), "typed_value", "comedy"));
    GenericRecord variant = recordFromMap(
        schema.unannotatedVariantType,
        ImmutableMap.of("metadata", EMPTY_METADATA, "typed_value", Arrays.asList(element)));
    GenericRecord record = recordFromMap(schema.unannotatedParquetSchema, ImmutableMap.of("id", 1, "var", variant));

    assertThrows(
        () -> writeAndRead(schema, record),
        IllegalStateException.class,
        "Invalid variant, conflicting value and typed_value");
  }

  /**
   * This is a custom Parquet writer builder that injects a specific Parquet schema and then uses
   * the Avro object model. This ensures that the Parquet file's schema is exactly what was passed.
   */
  private static class TestWriterBuilder extends ParquetWriter.Builder<GenericRecord, TestWriterBuilder> {
    private TestSchema schema = null;

    protected TestWriterBuilder(Path path) {
      super(path);
    }

    TestWriterBuilder withFileType(TestSchema schema) {
      this.schema = schema;
      return self();
    }

    @Override
    protected TestWriterBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<GenericRecord> getWriteSupport(Configuration conf) {
      return new AvroWriteSupport<>(
          schema.parquetSchema, avroSchema(schema.unannotatedParquetSchema), GenericData.get());
    }
  }

  GenericRecord writeAndRead(TestSchema testSchema, GenericRecord record) throws IOException {
    List<GenericRecord> result = writeAndRead(testSchema, Arrays.asList(record));
    assert (result.size() == 1);
    return result.get(0);
  }

  List<GenericRecord> writeAndRead(TestSchema testSchema, List<GenericRecord> records) throws IOException {
    // Create a temporary file for testing
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path path = new Path(tmp.getPath());

    try (ParquetWriter<GenericRecord> writer = new TestWriterBuilder(path)
        .withFileType(testSchema)
        .withConf(CONF)
        .build()) {
      for (GenericRecord record : records) {
        writer.write(record);
      }
    }

    Configuration conf = new Configuration();
    // We need to set an explicit read schema because Avro wrote the shredding schema as the Avro
    // schema in the write, and it will use that by default. If we write using a proper shredding
    // writer, the Avro schema should just contain a <metadata, value> record, and we won't need this.
    AvroReadSupport.setAvroReadSchema(conf, avroSchema(testSchema.parquetSchema));
    AvroParquetReader<GenericRecord> reader = new AvroParquetReader(conf, path);

    ArrayList<GenericRecord> result = new ArrayList<>();
    GenericRecord next = reader.read();
    while (next != null) {
      result.add(next);
      next = reader.read();
    }
    return result;
  }

  private static MessageType parquetSchema(Type variantType) {
    return Types.buildMessage()
        .required(PrimitiveTypeName.INT32)
        .id(1)
        .named("id")
        .addField(variantType)
        .named("table");
  }

  private static Type shreddedType(Variant value) {
    switch (value.getType()) {
      case BOOLEAN:
        return shreddedPrimitive(PrimitiveTypeName.BOOLEAN);
      case BYTE:
        return shreddedPrimitive(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8));
      case SHORT:
        return shreddedPrimitive(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(16));
      case INT:
        return shreddedPrimitive(PrimitiveTypeName.INT32);
      case LONG:
        return shreddedPrimitive(PrimitiveTypeName.INT64);
      case FLOAT:
        return shreddedPrimitive(PrimitiveTypeName.FLOAT);
      case DOUBLE:
        return shreddedPrimitive(PrimitiveTypeName.DOUBLE);
      case DECIMAL4:
        return shreddedPrimitive(
            PrimitiveTypeName.INT32,
            LogicalTypeAnnotation.decimalType(value.getDecimal().scale(), 9));
      case DECIMAL8:
        return shreddedPrimitive(
            PrimitiveTypeName.INT64,
            LogicalTypeAnnotation.decimalType(value.getDecimal().scale(), 18));
      case DECIMAL16:
        return shreddedPrimitive(
            PrimitiveTypeName.BINARY,
            LogicalTypeAnnotation.decimalType(value.getDecimal().scale(), 38));
      case DATE:
        return shreddedPrimitive(PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType());
      case TIMESTAMP_TZ:
        return shreddedPrimitive(
            PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS));
      case TIMESTAMP_NTZ:
        return shreddedPrimitive(
            PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(false, TimeUnit.MICROS));
      case BINARY:
        return shreddedPrimitive(PrimitiveTypeName.BINARY);
      case STRING:
        return shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
      case TIME:
        return shreddedPrimitive(
            PrimitiveTypeName.INT64, LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS));
      case TIMESTAMP_NANOS_TZ:
        return shreddedPrimitive(
            PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(true, TimeUnit.NANOS));
      case TIMESTAMP_NANOS_NTZ:
        return shreddedPrimitive(
            PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(false, TimeUnit.NANOS));
      case UUID:
        return shreddedPrimitive(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, LogicalTypeAnnotation.uuidType());
    }

    throw new UnsupportedOperationException("Unsupported shredding type: " + value.getType());
  }

  private static GroupType variant(String name, int fieldId) {
    return Types.buildGroup(Type.Repetition.REQUIRED)
        .id(fieldId)
        .as(LogicalTypeAnnotation.variantType((byte) 1))
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .required(PrimitiveTypeName.BINARY)
        .named("value")
        .named(name);
  }

  private static GroupType unannotatedVariant(String name, int fieldId) {
    return Types.buildGroup(Type.Repetition.REQUIRED)
        .id(fieldId)
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .required(PrimitiveTypeName.BINARY)
        .named("value")
        .named(name);
  }

  private static GroupType variant(String name, int fieldId, Type shreddedType) {
    checkShreddedType(shreddedType);
    return Types.buildGroup(Type.Repetition.OPTIONAL)
        .id(fieldId)
        .as(LogicalTypeAnnotation.variantType((byte) 1))
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .optional(PrimitiveTypeName.BINARY)
        .named("value")
        .addField(shreddedType)
        .named(name);
  }

  // Shredding schema with no Variant logical annotation. Needed in order to construct the Avro schema.
  private static GroupType unannotatedVariant(String name, int fieldId, Type shreddedType) {
    checkShreddedType(shreddedType);
    return Types.buildGroup(Type.Repetition.OPTIONAL)
        .id(fieldId)
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .optional(PrimitiveTypeName.BINARY)
        .named("value")
        .addField(shreddedType)
        .named(name);
  }

  private static void checkField(GroupType fieldType) {
    Preconditions.checkArgument(
        fieldType.isRepetition(Type.Repetition.REQUIRED),
        "Invalid field type repetition: %s should be REQUIRED",
        fieldType.getRepetition());
  }

  private static GroupType objectFields(GroupType... fields) {
    for (GroupType fieldType : fields) {
      checkField(fieldType);
    }

    return Types.buildGroup(Type.Repetition.OPTIONAL).addFields(fields).named("typed_value");
  }

  private static Type shreddedPrimitive(PrimitiveTypeName primitive) {
    return Types.optional(primitive).named("typed_value");
  }

  private static Type shreddedPrimitive(PrimitiveTypeName primitive, LogicalTypeAnnotation annotation) {
    return Types.optional(primitive).as(annotation).named("typed_value");
  }

  /**
   * Creates an Avro record from a map of field name to value.
   */
  private static GenericRecord recordFromMap(GroupType type, Map<String, Object> fields) {
    GenericRecord record = new GenericData.Record(avroSchema(type));
    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      record.put(entry.getKey(), entry.getValue());
    }
    return record;
  }

  // Required configuration to convert between Avro and Parquet schemas with 3-level list structure
  private static final ParquetConfiguration CONF = new PlainParquetConfiguration(ImmutableMap.of(
      AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false", AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS, "false"));

  private static GroupType shreddedField(String name, Type shreddedType) {
    checkShreddedType(shreddedType);
    return Types.buildGroup(Type.Repetition.REQUIRED)
        .optional(PrimitiveTypeName.BINARY)
        .named("value")
        .addField(shreddedType)
        .named(name);
  }

  private static GroupType element(Type shreddedType) {
    return shreddedField("element", shreddedType);
  }

  private static GroupType list(GroupType elementType) {
    return Types.optionalList().element(elementType).named("typed_value");
  }

  private static void checkListType(GroupType listType) {
    // Check the list is a 3-level structure
    Preconditions.checkArgument(
        listType.getFieldCount() == 1 && listType.getFields().get(0).isRepetition(Type.Repetition.REPEATED),
        "Invalid list type: does not contain single repeated field: %s",
        listType);

    GroupType repeated = listType.getFields().get(0).asGroupType();
    Preconditions.checkArgument(
        repeated.getFieldCount() == 1 && repeated.getFields().get(0).isRepetition(Type.Repetition.REQUIRED),
        "Invalid list type: does not contain single required subfield: %s",
        listType);
  }

  private static org.apache.avro.Schema avroSchema(GroupType schema) {
    if (schema instanceof MessageType) {
      return new AvroSchemaConverter(CONF).convert((MessageType) schema);
    } else {
      MessageType wrapped = Types.buildMessage().addField(schema).named("table");
      org.apache.avro.Schema avro = new AvroSchemaConverter(CONF)
          .convert(wrapped)
          .getFields()
          .get(0)
          .schema();
      switch (avro.getType()) {
        case RECORD:
          return avro;
        case UNION:
          return avro.getTypes().get(1);
      }

      throw new IllegalArgumentException("Invalid converted type: " + avro);
    }
  }

  private static void checkShreddedType(Type shreddedType) {
    Preconditions.checkArgument(
        shreddedType.getName().equals("typed_value"),
        "Invalid shredded type name: %s should be typed_value",
        shreddedType.getName());
    Preconditions.checkArgument(
        shreddedType.isRepetition(Type.Repetition.OPTIONAL),
        "Invalid shredded type repetition: %s should be OPTIONAL",
        shreddedType.getRepetition());
  }

  /**
   * Check for the given exception with message, possibly wrapped by a ParquetDecodingException.
   */
  void assertThrows(Callable callable, Class<? extends Exception> exception, String msg) {
    try {
      callable.call();
      fail("No exception was thrown. Expected: " + exception.getName());
    } catch (Exception actual) {
      try {
        if (actual.getClass().equals(ParquetDecodingException.class)) {
          assertTrue(actual.getCause().getMessage().contains(msg));
          assertEquals(actual.getCause().getClass(), exception);
        } else {
          assertTrue(actual.getMessage().contains(msg));
          assertEquals(actual.getClass(), exception);
        }
      } catch (AssertionError e) {
        e.addSuppressed(actual);
        throw e;
      }
    }
  }

  /**
   * Assert that metadata contains identical bytes to expected, and value is logically equivalent.
   * E.g. object fields may be ordered differently in the binary.
   */
  void assertEquivalent(ByteBuffer expectedMetadata, ByteBuffer expectedValue, GenericRecord actual) {
    assertEquals(expectedMetadata, (ByteBuffer) actual.get("metadata"));
    assertEquals(expectedMetadata, (ByteBuffer) actual.get("metadata"));
    AvroTestUtil.assertEquivalent(
        new Variant(expectedValue, expectedMetadata),
        new Variant(((ByteBuffer) actual.get("value")), expectedMetadata));
  }
}
