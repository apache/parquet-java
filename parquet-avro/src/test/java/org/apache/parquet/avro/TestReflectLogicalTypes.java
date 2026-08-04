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

import static org.apache.parquet.avro.AvroTestUtil.read;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.AvroSchema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * This class is based on org.apache.avro.reflect.TestReflectLogicalTypes
 *
 * Tests various logical types
 * * string => UUID
 * * fixed and bytes => Decimal
 * * record => Pair
 */
public class TestReflectLogicalTypes {
  @TempDir
  private Path tempDir;

  public static final ReflectData REFLECT = new ReflectData();

  @BeforeAll
  public static void addUUID() {
    REFLECT.addLogicalTypeConversion(new Conversions.UUIDConversion());
    REFLECT.addLogicalTypeConversion(new Conversions.DecimalConversion());
  }

  @Test
  public void testReflectedSchema() {
    Schema expected = SchemaBuilder.record(RecordWithUUIDList.class.getName())
        .fields()
        .name("uuids")
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .endRecord();
    expected.getField("uuids").schema().addProp(SpecificData.CLASS_PROP, List.class.getName());
    LogicalTypes.uuid().addToSchema(expected.getField("uuids").schema().getElementType());

    Schema actual = REFLECT.getSchema(RecordWithUUIDList.class);

    assertThat(actual).as("Should use the UUID logical type").isEqualTo(expected);
  }

  // this can be static because the schema only comes from reflection
  public static class DecimalRecordBytes {
    // scale is required and will not be set by the conversion
    @AvroSchema("{" + "\"type\": \"bytes\","
        + "\"logicalType\": \"decimal\","
        + "\"precision\": 9,"
        + "\"scale\": 2"
        + "}")
    private BigDecimal decimal;

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }

      if (other == null || getClass() != other.getClass()) {
        return false;
      }

      DecimalRecordBytes that = (DecimalRecordBytes) other;
      if (decimal == null) {
        return (that.decimal == null);
      }

      return decimal.equals(that.decimal);
    }

    @Override
    public int hashCode() {
      return decimal != null ? decimal.hashCode() : 0;
    }
  }

  @Test
  public void testDecimalBytes() throws IOException {
    Schema schema = REFLECT.getSchema(DecimalRecordBytes.class);
    assertThat(schema.getNamespace())
        .as("Should have the correct record name")
        .isEqualTo("org.apache.parquet.avro.TestReflectLogicalTypes");
    assertThat(schema.getName()).as("Should have the correct record name").isEqualTo("DecimalRecordBytes");
    assertThat(LogicalTypes.fromSchema(schema.getField("decimal").schema()))
        .as("Should have the correct logical type")
        .isEqualTo(LogicalTypes.decimal(9, 2));

    DecimalRecordBytes record = new DecimalRecordBytes();
    record.decimal = new BigDecimal("3.14");

    File test = write(REFLECT, schema, record);
    assertThat(read(REFLECT, schema, test))
        .as("Should match the decimal after round trip")
        .containsExactly(record);
  }

  // this can be static because the schema only comes from reflection
  public static class DecimalRecordFixed {
    // scale is required and will not be set by the conversion
    @AvroSchema("{" + "\"name\": \"decimal_9\","
        + "\"type\": \"fixed\","
        + "\"size\": 4,"
        + "\"logicalType\": \"decimal\","
        + "\"precision\": 9,"
        + "\"scale\": 2"
        + "}")
    private BigDecimal decimal;

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }

      if (other == null || getClass() != other.getClass()) {
        return false;
      }

      DecimalRecordFixed that = (DecimalRecordFixed) other;
      if (decimal == null) {
        return (that.decimal == null);
      }

      return decimal.equals(that.decimal);
    }

    @Override
    public int hashCode() {
      return decimal != null ? decimal.hashCode() : 0;
    }
  }

  @Test
  public void testDecimalFixed() throws IOException {
    Schema schema = REFLECT.getSchema(DecimalRecordFixed.class);
    assertThat(schema.getNamespace())
        .as("Should have the correct record name")
        .isEqualTo("org.apache.parquet.avro.TestReflectLogicalTypes");
    assertThat(schema.getName()).as("Should have the correct record name").isEqualTo("DecimalRecordFixed");
    assertThat(LogicalTypes.fromSchema(schema.getField("decimal").schema()))
        .as("Should have the correct logical type")
        .isEqualTo(LogicalTypes.decimal(9, 2));

    DecimalRecordFixed record = new DecimalRecordFixed();
    record.decimal = new BigDecimal("3.14");

    File test = write(REFLECT, schema, record);
    assertThat(read(REFLECT, schema, test))
        .as("Should match the decimal after round trip")
        .containsExactly(record);
  }

  public static class Pair<X, Y> {
    private final X first;
    private final Y second;

    private Pair(X first, Y second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }

      if (other == null || getClass() != other.getClass()) {
        return false;
      }

      Pair<?, ?> that = (Pair<?, ?>) other;
      if (first == null) {
        if (that.first != null) {
          return false;
        }
      } else if (first.equals(that.first)) {
        return false;
      }

      if (second == null) {
        if (that.second != null) {
          return false;
        }
      } else if (second.equals(that.second)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(new Object[] {first, second});
    }

    public static <X, Y> Pair<X, Y> of(X first, Y second) {
      return new Pair<X, Y>(first, second);
    }
  }

  public static class PairRecord {
    @AvroSchema("{" + "\"name\": \"Pair\","
        + "\"type\": \"record\","
        + "\"fields\": ["
        + "    {\"name\": \"x\", \"type\": \"long\"},"
        + "    {\"name\": \"y\", \"type\": \"long\"}"
        + "  ],"
        + "\"logicalType\": \"pair\""
        + "}")
    Pair<Long, Long> pair;
  }

  @Test
  public void testPairRecord() throws IOException {
    ReflectData model = new ReflectData();
    model.addLogicalTypeConversion(new Conversion<Pair>() {
      @Override
      public Class<Pair> getConvertedType() {
        return Pair.class;
      }

      @Override
      public String getLogicalTypeName() {
        return "pair";
      }

      @Override
      public Pair fromRecord(IndexedRecord value, Schema schema, LogicalType type) {
        return Pair.of(value.get(0), value.get(1));
      }

      @Override
      public IndexedRecord toRecord(Pair value, Schema schema, LogicalType type) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put(0, value.first);
        record.put(1, value.second);
        return record;
      }
    });

    LogicalTypes.register("pair", new LogicalTypes.LogicalTypeFactory() {
      private final LogicalType PAIR = new LogicalType("pair");

      @Override
      public LogicalType fromSchema(Schema schema) {
        return PAIR;
      }
    });

    Schema schema = model.getSchema(PairRecord.class);
    assertThat(schema.getNamespace())
        .as("Should have the correct record name")
        .isEqualTo("org.apache.parquet.avro.TestReflectLogicalTypes");
    assertThat(schema.getName()).as("Should have the correct record name").isEqualTo("PairRecord");
    assertThat(LogicalTypes.fromSchema(schema.getField("pair").schema()).getName())
        .as("Should have the correct logical type")
        .isEqualTo("pair");

    PairRecord record = new PairRecord();
    record.pair = Pair.of(34L, 35L);
    List<PairRecord> expected = new ArrayList<PairRecord>();
    expected.add(record);

    File test = write(model, schema, record);
    Pair<Long, Long> actual =
        AvroTestUtil.<PairRecord>read(model, schema, test).get(0).pair;
    assertThat((long) actual.first)
        .as("Data should match after serialization round-trip")
        .isEqualTo(34L);
    assertThat((long) actual.second)
        .as("Data should match after serialization round-trip")
        .isEqualTo(35L);
  }

  @Test
  public void testReadUUID() throws IOException {
    Schema uuidSchema = SchemaBuilder.record(RecordWithUUID.class.getName())
        .fields()
        .requiredString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(uuidSchema.getField("uuid").schema());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    RecordWithStringUUID r1 = new RecordWithStringUUID();
    r1.uuid = u1.toString();
    RecordWithStringUUID r2 = new RecordWithStringUUID();
    r2.uuid = u2.toString();

    List<RecordWithUUID> expected = Arrays.asList(new RecordWithUUID(), new RecordWithUUID());
    expected.get(0).uuid = u1;
    expected.get(1).uuid = u2;

    File test = write(ReflectData.get().getSchema(RecordWithStringUUID.class), r1, r2);

    assertThat(read(REFLECT, uuidSchema, test))
        .as("Should convert Strings to UUIDs")
        .containsExactlyElementsOf(expected);

    // verify that the field's type overrides the logical type
    Schema uuidStringSchema = SchemaBuilder.record(RecordWithStringUUID.class.getName())
        .fields()
        .requiredString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(uuidStringSchema.getField("uuid").schema());

    assertThat(read(REFLECT, uuidStringSchema, test))
        .as("Should not convert to UUID if accessor is String")
        .containsExactly(r1, r2);
  }

  @Test
  public void testReadUUIDWithParquetUUID() throws IOException {
    Schema uuidSchema = SchemaBuilder.record(RecordWithUUID.class.getName())
        .fields()
        .requiredString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(uuidSchema.getField("uuid").schema());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    RecordWithStringUUID r1 = new RecordWithStringUUID();
    r1.uuid = u1.toString();
    RecordWithStringUUID r2 = new RecordWithStringUUID();
    r2.uuid = u2.toString();

    List<RecordWithUUID> expected = Arrays.asList(new RecordWithUUID(), new RecordWithUUID());
    expected.get(0).uuid = u1;
    expected.get(1).uuid = u2;

    File test = write(AvroTestUtil.conf(AvroWriteSupport.WRITE_PARQUET_UUID, true), uuidSchema, r1, r2);

    assertThat(read(REFLECT, uuidSchema, test))
        .as("Should convert Strings to UUIDs")
        .containsExactlyElementsOf(expected);

    // verify that the field's type overrides the logical type
    Schema uuidStringSchema = SchemaBuilder.record(RecordWithStringUUID.class.getName())
        .fields()
        .requiredString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(uuidStringSchema.getField("uuid").schema());

    assertThat(read(REFLECT, uuidStringSchema, test))
        .as("Should not convert to UUID if accessor is String")
        .containsExactly(r1, r2);
  }

  @Test
  public void testWriteUUID() throws IOException {
    Schema uuidSchema = SchemaBuilder.record(RecordWithUUID.class.getName())
        .fields()
        .requiredString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(uuidSchema.getField("uuid").schema());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    RecordWithUUID r1 = new RecordWithUUID();
    r1.uuid = u1;
    RecordWithUUID r2 = new RecordWithUUID();
    r2.uuid = u2;

    List<RecordWithStringUUID> expected = Arrays.asList(new RecordWithStringUUID(), new RecordWithStringUUID());
    expected.get(0).uuid = u1.toString();
    expected.get(1).uuid = u2.toString();

    File test = write(REFLECT, uuidSchema, r1, r2);

    // verify that the field's type overrides the logical type
    Schema uuidStringSchema = SchemaBuilder.record(RecordWithStringUUID.class.getName())
        .fields()
        .requiredString("uuid")
        .endRecord();

    assertThat(read(REFLECT, uuidStringSchema, test))
        .as("Should read uuid as String without UUID conversion")
        .containsExactlyElementsOf(expected);

    LogicalTypes.uuid().addToSchema(uuidStringSchema.getField("uuid").schema());
    assertThat(read(ReflectData.get(), uuidStringSchema, test))
        .as("Should read uuid as String without UUID logical type")
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void testWriteUUIDWithParuetUUID() throws IOException {
    Schema uuidSchema = SchemaBuilder.record(RecordWithUUID.class.getName())
        .fields()
        .requiredString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(uuidSchema.getField("uuid").schema());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    RecordWithUUID r1 = new RecordWithUUID();
    r1.uuid = u1;
    RecordWithUUID r2 = new RecordWithUUID();
    r2.uuid = u2;

    List<RecordWithStringUUID> expected = Arrays.asList(new RecordWithStringUUID(), new RecordWithStringUUID());
    expected.get(0).uuid = u1.toString();
    expected.get(1).uuid = u2.toString();

    File test = write(AvroTestUtil.conf(AvroWriteSupport.WRITE_PARQUET_UUID, true), REFLECT, uuidSchema, r1, r2);

    assertThat(read(REFLECT, uuidSchema, test))
        .as("Should read UUID objects")
        .containsExactly(r1, r2);

    Schema uuidStringSchema = SchemaBuilder.record(RecordWithStringUUID.class.getName())
        .fields()
        .requiredString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(uuidStringSchema.getField("uuid").schema());
    assertThat(read(ReflectData.get(), uuidStringSchema, test))
        .as("Should read uuid as Strings")
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void testWriteNullableUUID() throws IOException {
    Schema nullableUuidSchema = SchemaBuilder.record(RecordWithUUID.class.getName())
        .fields()
        .optionalString("uuid")
        .endRecord();
    LogicalTypes.uuid()
        .addToSchema(
            nullableUuidSchema.getField("uuid").schema().getTypes().get(1));

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    RecordWithUUID r1 = new RecordWithUUID();
    r1.uuid = u1;
    RecordWithUUID r2 = new RecordWithUUID();
    r2.uuid = u2;

    List<RecordWithStringUUID> expected = Arrays.asList(new RecordWithStringUUID(), new RecordWithStringUUID());
    expected.get(0).uuid = u1.toString();
    expected.get(1).uuid = u2.toString();

    File test = write(REFLECT, nullableUuidSchema, r1, r2);

    // verify that the field's type overrides the logical type
    Schema nullableUuidStringSchema = SchemaBuilder.record(RecordWithStringUUID.class.getName())
        .fields()
        .optionalString("uuid")
        .endRecord();

    assertThat(read(REFLECT, nullableUuidStringSchema, test))
        .as("Should read uuid as String without UUID conversion")
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void testWriteNullableUUIDWithParquetUUID() throws IOException {
    Schema nullableUuidSchema = SchemaBuilder.record(RecordWithUUID.class.getName())
        .fields()
        .optionalString("uuid")
        .endRecord();
    LogicalTypes.uuid()
        .addToSchema(
            nullableUuidSchema.getField("uuid").schema().getTypes().get(1));

    UUID u1 = UUID.randomUUID();
    UUID u2 = null;

    RecordWithUUID r1 = new RecordWithUUID();
    r1.uuid = u1;
    RecordWithUUID r2 = new RecordWithUUID();
    r2.uuid = u2;

    List<RecordWithStringUUID> expected = Arrays.asList(new RecordWithStringUUID(), new RecordWithStringUUID());
    expected.get(0).uuid = u1.toString();
    expected.get(1).uuid = null;

    File test = write(
        AvroTestUtil.conf(AvroWriteSupport.WRITE_PARQUET_UUID, true), REFLECT, nullableUuidSchema, r1, r2);

    assertThat(read(REFLECT, nullableUuidSchema, test))
        .as("Should read uuid as UUID objects")
        .containsExactly(r1, r2);

    Schema nullableUuidStringSchema = SchemaBuilder.record(RecordWithStringUUID.class.getName())
        .fields()
        .optionalString("uuid")
        .endRecord();
    LogicalTypes.uuid()
        .addToSchema(nullableUuidStringSchema
            .getField("uuid")
            .schema()
            .getTypes()
            .get(1));

    assertThat(read(REFLECT, nullableUuidStringSchema, test))
        .as("Should read uuid as String without UUID conversion")
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void testWriteUUIDMissingLogicalType() throws IOException {
    Schema uuidSchema = SchemaBuilder.record(RecordWithUUID.class.getName())
        .fields()
        .requiredString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(uuidSchema.getField("uuid").schema());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    RecordWithUUID r1 = new RecordWithUUID();
    r1.uuid = u1;
    RecordWithUUID r2 = new RecordWithUUID();
    r2.uuid = u2;

    List<RecordWithStringUUID> expected = Arrays.asList(new RecordWithStringUUID(), new RecordWithStringUUID());
    expected.get(0).uuid = u1.toString();
    expected.get(1).uuid = u2.toString();

    // write without using REFLECT, which has the logical type
    File test = write(uuidSchema, r1, r2);

    // verify that the field's type overrides the logical type
    Schema uuidStringSchema = SchemaBuilder.record(RecordWithStringUUID.class.getName())
        .fields()
        .requiredString("uuid")
        .endRecord();

    assertThat(read(REFLECT, uuidStringSchema, test))
        .as("Should read uuid as String without UUID conversion")
        .containsExactlyElementsOf(expected);

    assertThat(read(ReflectData.get(), uuidStringSchema, test))
        .as("Should read uuid as String without UUID logical type")
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void testReadUUIDGenericRecord() throws IOException {
    Schema uuidSchema = SchemaBuilder.record("RecordWithUUID")
        .fields()
        .requiredString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(uuidSchema.getField("uuid").schema());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    RecordWithStringUUID r1 = new RecordWithStringUUID();
    r1.uuid = u1.toString();
    RecordWithStringUUID r2 = new RecordWithStringUUID();
    r2.uuid = u2.toString();

    List<GenericData.Record> expected =
        Arrays.asList(new GenericData.Record(uuidSchema), new GenericData.Record(uuidSchema));
    expected.get(0).put("uuid", u1);
    expected.get(1).put("uuid", u2);

    File test = write(ReflectData.get().getSchema(RecordWithStringUUID.class), r1, r2);

    assertThat(read(REFLECT, uuidSchema, test))
        .as("Should convert Strings to UUIDs")
        .containsExactlyElementsOf(expected);

    // verify that the field's type overrides the logical type
    Schema uuidStringSchema = SchemaBuilder.record(RecordWithStringUUID.class.getName())
        .fields()
        .requiredString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(uuidSchema.getField("uuid").schema());

    assertThat(read(REFLECT, uuidStringSchema, test))
        .as("Should not convert to UUID if accessor is String")
        .containsExactly(r1, r2);
  }

  @Test
  public void testReadUUIDGenericRecordWithParquetUUID() throws IOException {
    Schema uuidSchema = SchemaBuilder.record("RecordWithUUID")
        .fields()
        .requiredString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(uuidSchema.getField("uuid").schema());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    RecordWithStringUUID r1 = new RecordWithStringUUID();
    r1.uuid = u1.toString();
    RecordWithStringUUID r2 = new RecordWithStringUUID();
    r2.uuid = u2.toString();

    List<GenericData.Record> expected =
        Arrays.asList(new GenericData.Record(uuidSchema), new GenericData.Record(uuidSchema));
    expected.get(0).put("uuid", u1);
    expected.get(1).put("uuid", u2);

    File test = write(AvroTestUtil.conf(AvroWriteSupport.WRITE_PARQUET_UUID, true), uuidSchema, r1, r2);

    assertThat(read(REFLECT, uuidSchema, test))
        .as("Should convert Strings to UUIDs")
        .containsExactlyElementsOf(expected);

    Schema uuidStringSchema = SchemaBuilder.record(RecordWithStringUUID.class.getName())
        .fields()
        .requiredString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(uuidStringSchema.getField("uuid").schema());

    assertThat(read(REFLECT, uuidStringSchema, test))
        .as("Should not convert to UUID if accessor is String")
        .containsExactly(r1, r2);
  }

  @Test
  public void testReadUUIDArray() throws IOException {
    Schema uuidArraySchema = SchemaBuilder.record(RecordWithUUIDArray.class.getName())
        .fields()
        .name("uuids")
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .endRecord();
    LogicalTypes.uuid()
        .addToSchema(uuidArraySchema.getField("uuids").schema().getElementType());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    GenericRecord r = new GenericData.Record(uuidArraySchema);
    r.put("uuids", Arrays.asList(u1.toString(), u2.toString()));

    RecordWithUUIDArray expected = new RecordWithUUIDArray();
    expected.uuids = new UUID[] {u1, u2};

    File test = write(uuidArraySchema, r);

    assertThat(read(REFLECT, uuidArraySchema, test).get(0))
        .as("Should convert Strings to UUIDs")
        .isEqualTo(expected);
  }

  @Test
  public void testReadUUIDArrayWithParquetUUID() throws IOException {
    Schema uuidArraySchema = SchemaBuilder.record(RecordWithUUIDArray.class.getName())
        .fields()
        .name("uuids")
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .endRecord();
    LogicalTypes.uuid()
        .addToSchema(uuidArraySchema.getField("uuids").schema().getElementType());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    GenericRecord r = new GenericData.Record(uuidArraySchema);
    r.put("uuids", Arrays.asList(u1.toString(), u2.toString()));

    RecordWithUUIDArray expected = new RecordWithUUIDArray();
    expected.uuids = new UUID[] {u1, u2};

    File test = write(AvroTestUtil.conf(AvroWriteSupport.WRITE_PARQUET_UUID, true), uuidArraySchema, r);

    assertThat(read(REFLECT, uuidArraySchema, test).get(0))
        .as("Should convert Strings to UUIDs")
        .isEqualTo(expected);
  }

  @Test
  public void testWriteUUIDArray() throws IOException {
    Schema uuidArraySchema = SchemaBuilder.record(RecordWithUUIDArray.class.getName())
        .fields()
        .name("uuids")
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .endRecord();
    LogicalTypes.uuid()
        .addToSchema(uuidArraySchema.getField("uuids").schema().getElementType());

    Schema stringArraySchema = SchemaBuilder.record("RecordWithUUIDArray")
        .fields()
        .name("uuids")
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .endRecord();
    stringArraySchema.getField("uuids").schema().addProp(SpecificData.CLASS_PROP, List.class.getName());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    GenericRecord expected = new GenericData.Record(stringArraySchema);
    List<String> uuids = new ArrayList<String>();
    uuids.add(u1.toString());
    uuids.add(u2.toString());
    expected.put("uuids", uuids);

    RecordWithUUIDArray r = new RecordWithUUIDArray();
    r.uuids = new UUID[] {u1, u2};

    File test = write(REFLECT, uuidArraySchema, r);

    assertThat(read(ReflectData.get(), stringArraySchema, test).get(0))
        .as("Should read UUIDs as Strings")
        .isEqualTo(expected);
  }

  @Test
  public void testWriteUUIDArrayWithParquetUUID() throws IOException {
    Schema uuidArraySchema = SchemaBuilder.record(RecordWithUUIDArray.class.getName())
        .fields()
        .name("uuids")
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .endRecord();
    LogicalTypes.uuid()
        .addToSchema(uuidArraySchema.getField("uuids").schema().getElementType());

    Schema stringArraySchema = SchemaBuilder.record("RecordWithUUIDArray")
        .fields()
        .name("uuids")
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .endRecord();
    LogicalTypes.uuid()
        .addToSchema(stringArraySchema.getField("uuids").schema().getElementType());
    stringArraySchema.getField("uuids").schema().addProp(SpecificData.CLASS_PROP, List.class.getName());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    GenericRecord expected = new GenericData.Record(stringArraySchema);
    List<String> uuids = new ArrayList<String>();
    uuids.add(u1.toString());
    uuids.add(u2.toString());
    expected.put("uuids", uuids);

    RecordWithUUIDArray r = new RecordWithUUIDArray();
    r.uuids = new UUID[] {u1, u2};

    File test = write(AvroTestUtil.conf(AvroWriteSupport.WRITE_PARQUET_UUID, true), REFLECT, uuidArraySchema, r);

    assertThat(read(ReflectData.get(), stringArraySchema, test).get(0))
        .as("Should read UUIDs as Strings")
        .isEqualTo(expected);
  }

  @Test
  public void testReadUUIDList() throws IOException {
    Schema uuidListSchema = SchemaBuilder.record(RecordWithUUIDList.class.getName())
        .fields()
        .name("uuids")
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .endRecord();
    uuidListSchema.getField("uuids").schema().addProp(SpecificData.CLASS_PROP, List.class.getName());
    LogicalTypes.uuid()
        .addToSchema(uuidListSchema.getField("uuids").schema().getElementType());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    GenericRecord r = new GenericData.Record(uuidListSchema);
    r.put("uuids", Arrays.asList(u1.toString(), u2.toString()));

    RecordWithUUIDList expected = new RecordWithUUIDList();
    expected.uuids = Arrays.asList(u1, u2);

    File test = write(uuidListSchema, r);

    assertThat(read(REFLECT, uuidListSchema, test).get(0))
        .as("Should convert Strings to UUIDs")
        .isEqualTo(expected);
  }

  @Test
  public void testReadUUIDListWithParquetUUID() throws IOException {
    Schema uuidListSchema = SchemaBuilder.record(RecordWithUUIDList.class.getName())
        .fields()
        .name("uuids")
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .endRecord();
    uuidListSchema.getField("uuids").schema().addProp(SpecificData.CLASS_PROP, List.class.getName());
    LogicalTypes.uuid()
        .addToSchema(uuidListSchema.getField("uuids").schema().getElementType());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    GenericRecord r = new GenericData.Record(uuidListSchema);
    r.put("uuids", Arrays.asList(u1.toString(), u2.toString()));

    RecordWithUUIDList expected = new RecordWithUUIDList();
    expected.uuids = Arrays.asList(u1, u2);

    File test = write(AvroTestUtil.conf(AvroWriteSupport.WRITE_PARQUET_UUID, true), uuidListSchema, r);

    assertThat(read(REFLECT, uuidListSchema, test).get(0))
        .as("Should convert Strings to UUIDs")
        .isEqualTo(expected);
  }

  @Test
  public void testWriteUUIDList() throws IOException {
    Schema uuidListSchema = SchemaBuilder.record(RecordWithUUIDList.class.getName())
        .fields()
        .name("uuids")
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .endRecord();
    uuidListSchema.getField("uuids").schema().addProp(SpecificData.CLASS_PROP, List.class.getName());
    LogicalTypes.uuid()
        .addToSchema(uuidListSchema.getField("uuids").schema().getElementType());

    Schema stringArraySchema = SchemaBuilder.record("RecordWithUUIDArray")
        .fields()
        .name("uuids")
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .endRecord();
    stringArraySchema.getField("uuids").schema().addProp(SpecificData.CLASS_PROP, List.class.getName());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    GenericRecord expected = new GenericData.Record(stringArraySchema);
    expected.put("uuids", Arrays.asList(u1.toString(), u2.toString()));

    RecordWithUUIDList r = new RecordWithUUIDList();
    r.uuids = Arrays.asList(u1, u2);

    File test = write(REFLECT, uuidListSchema, r);

    assertThat(read(REFLECT, stringArraySchema, test).get(0))
        .as("Should read UUIDs as Strings")
        .isEqualTo(expected);
  }

  @Test
  public void testWriteUUIDListWithParquetUUID() throws IOException {
    Schema uuidListSchema = SchemaBuilder.record(RecordWithUUIDList.class.getName())
        .fields()
        .name("uuids")
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .endRecord();
    uuidListSchema.getField("uuids").schema().addProp(SpecificData.CLASS_PROP, List.class.getName());
    LogicalTypes.uuid()
        .addToSchema(uuidListSchema.getField("uuids").schema().getElementType());

    Schema reflectSchema = SchemaBuilder.record("RecordWithUUIDArray")
        .fields()
        .name("uuids")
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .endRecord();
    reflectSchema.getField("uuids").schema().addProp(SpecificData.CLASS_PROP, List.class.getName());
    LogicalTypes.uuid().addToSchema(reflectSchema.getField("uuids").schema().getElementType());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    GenericRecord expected = new GenericData.Record(reflectSchema);
    expected.put("uuids", Arrays.asList(u1, u2));

    RecordWithUUIDList r = new RecordWithUUIDList();
    r.uuids = Arrays.asList(u1, u2);

    File test = write(AvroTestUtil.conf(AvroWriteSupport.WRITE_PARQUET_UUID, true), REFLECT, uuidListSchema, r);

    assertThat(read(REFLECT, reflectSchema, test).get(0))
        .as("Should read UUID objects")
        .isEqualTo(expected);
  }

  @SuppressWarnings("unchecked")
  private <D> File write(Schema schema, D... data) throws IOException {
    return write(ReflectData.get(), schema, data);
  }

  @SuppressWarnings("unchecked")
  private <D> File write(Configuration conf, Schema schema, D... data) throws IOException {
    return write(conf, ReflectData.get(), schema, data);
  }

  @SuppressWarnings("unchecked")
  private <D> File write(GenericData model, Schema schema, D... data) throws IOException {
    return AvroTestUtil.write(tempDir, model, schema, data);
  }

  @SuppressWarnings("unchecked")
  private <D> File write(Configuration conf, GenericData model, Schema schema, D... data) throws IOException {
    return AvroTestUtil.write(tempDir, conf, model, schema, data);
  }
}

class RecordWithUUID {
  UUID uuid;

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof RecordWithUUID)) {
      return false;
    }
    RecordWithUUID that = (RecordWithUUID) obj;
    return Objects.equals(this.uuid, that.uuid);
  }
}

class RecordWithStringUUID {
  String uuid;

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof RecordWithStringUUID)) {
      return false;
    }
    RecordWithStringUUID that = (RecordWithStringUUID) obj;
    return Objects.equals(this.uuid, that.uuid);
  }
}

class RecordWithUUIDArray {
  UUID[] uuids;

  @Override
  public int hashCode() {
    return Arrays.hashCode(uuids);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof RecordWithUUIDArray)) {
      return false;
    }
    RecordWithUUIDArray that = (RecordWithUUIDArray) obj;
    return Arrays.equals(this.uuids, that.uuids);
  }
}

class RecordWithUUIDList {
  List<UUID> uuids;

  @Override
  public int hashCode() {
    return uuids.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof RecordWithUUIDList)) {
      return false;
    }
    RecordWithUUIDList that = (RecordWithUUIDList) obj;
    return this.uuids.equals(that.uuids);
  }
}
