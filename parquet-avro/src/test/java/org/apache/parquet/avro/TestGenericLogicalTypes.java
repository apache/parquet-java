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

import static org.apache.avro.Schema.Type.STRING;
import static org.apache.parquet.avro.AvroTestUtil.conf;
import static org.apache.parquet.avro.AvroTestUtil.field;
import static org.apache.parquet.avro.AvroTestUtil.instance;
import static org.apache.parquet.avro.AvroTestUtil.optionalField;
import static org.apache.parquet.avro.AvroTestUtil.read;
import static org.apache.parquet.avro.AvroTestUtil.record;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * This class is based on org.apache.avro.generic.TestGenericLogicalTypes
 */
public class TestGenericLogicalTypes {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public static final GenericData GENERIC = new GenericData();
  public static final LogicalType DECIMAL_9_2 = LogicalTypes.decimal(9, 2);
  public static final BigDecimal D1 = new BigDecimal("-34.34");
  public static final BigDecimal D2 = new BigDecimal("117230.00");

  @BeforeClass
  public static void addDecimalAndUUID() {
    GENERIC.addLogicalTypeConversion(new Conversions.DecimalConversion());
    GENERIC.addLogicalTypeConversion(new Conversions.UUIDConversion());
  }

  private <T> List<T> getFieldValues(Collection<GenericRecord> records, String field, Class<T> expectedClass) {
    List<T> values = new ArrayList<T>();
    for (GenericRecord record : records) {
      values.add(expectedClass.cast(record.get(field)));
    }
    return values;
  }

  @Test
  public void testReadUUID() throws IOException {
    Schema uuidSchema = record("R", field("uuid", LogicalTypes.uuid().addToSchema(Schema.create(STRING))));
    GenericRecord u1 = instance(uuidSchema, "uuid", UUID.randomUUID());
    GenericRecord u2 = instance(uuidSchema, "uuid", UUID.randomUUID());

    Schema stringSchema = record("R", field("uuid", Schema.create(STRING)));
    GenericRecord s1 = instance(stringSchema, "uuid", u1.get("uuid").toString());
    GenericRecord s2 = instance(stringSchema, "uuid", u2.get("uuid").toString());

    File test = write(stringSchema, s1, s2);
    Assert.assertEquals("Should convert Strings to UUIDs", Arrays.asList(u1, u2), read(GENERIC, uuidSchema, test));
  }

  @Test
  public void testReadUUIDWithParquetUUID() throws IOException {
    Schema uuidSchema = record("R", field("uuid", LogicalTypes.uuid().addToSchema(Schema.create(STRING))));
    GenericRecord u1 = instance(uuidSchema, "uuid", UUID.randomUUID());
    GenericRecord u2 = instance(uuidSchema, "uuid", UUID.randomUUID());
    File test = write(conf(AvroWriteSupport.WRITE_PARQUET_UUID, true), uuidSchema, u1, u2);

    Assert.assertEquals("Should read UUID objects", Arrays.asList(u1, u2), read(GENERIC, uuidSchema, test));

    GenericRecord s1 = instance(uuidSchema, "uuid", u1.get("uuid").toString());
    GenericRecord s2 = instance(uuidSchema, "uuid", u2.get("uuid").toString());

    Assert.assertEquals(
        "Should read UUID as Strings", Arrays.asList(s1, s2), read(GenericData.get(), uuidSchema, test));
  }

  @Test
  public void testWriteUUIDReadStringSchema() throws IOException {
    Schema uuidSchema = record("R", field("uuid", LogicalTypes.uuid().addToSchema(Schema.create(STRING))));
    GenericRecord u1 = instance(uuidSchema, "uuid", UUID.randomUUID());
    GenericRecord u2 = instance(uuidSchema, "uuid", UUID.randomUUID());

    Schema stringUuidSchema = Schema.create(STRING);
    stringUuidSchema.addProp(GenericData.STRING_PROP, "String");
    Schema stringSchema = record("R", field("uuid", stringUuidSchema));
    GenericRecord s1 = instance(stringSchema, "uuid", u1.get("uuid").toString());
    GenericRecord s2 = instance(stringSchema, "uuid", u2.get("uuid").toString());

    File test = write(GENERIC, uuidSchema, u1, u2);
    Assert.assertEquals("Should read UUIDs as Strings", Arrays.asList(s1, s2), read(GENERIC, stringSchema, test));
  }

  @Test
  public void testWriteUUIDReadStringMissingLogicalType() throws IOException {
    Schema uuidSchema = record("R", field("uuid", LogicalTypes.uuid().addToSchema(Schema.create(STRING))));
    GenericRecord u1 = instance(uuidSchema, "uuid", UUID.randomUUID());
    GenericRecord u2 = instance(uuidSchema, "uuid", UUID.randomUUID());

    GenericRecord s1 = instance(uuidSchema, "uuid", new Utf8(u1.get("uuid").toString()));
    GenericRecord s2 = instance(uuidSchema, "uuid", new Utf8(u2.get("uuid").toString()));

    File test = write(GENERIC, uuidSchema, u1, u2);
    Assert.assertEquals(
        "Should read UUIDs as Strings", Arrays.asList(s1, s2), read(GenericData.get(), uuidSchema, test));
  }

  @Test
  public void testWriteNullableUUID() throws IOException {
    Schema nullableUuidSchema =
        record("R", optionalField("uuid", LogicalTypes.uuid().addToSchema(Schema.create(STRING))));
    GenericRecord u1 = instance(nullableUuidSchema, "uuid", UUID.randomUUID());
    GenericRecord u2 = instance(nullableUuidSchema, "uuid", null);

    Schema stringUuidSchema = Schema.create(STRING);
    stringUuidSchema.addProp(GenericData.STRING_PROP, "String");
    Schema nullableStringSchema = record("R", optionalField("uuid", stringUuidSchema));
    GenericRecord s1 = instance(nullableStringSchema, "uuid", u1.get("uuid").toString());
    GenericRecord s2 = instance(nullableStringSchema, "uuid", null);

    File test = write(GENERIC, nullableUuidSchema, u1, u2);
    Assert.assertEquals(
        "Should read UUIDs as Strings", Arrays.asList(s1, s2), read(GENERIC, nullableStringSchema, test));
  }

  @Test
  public void testWriteNullableUUIDWithParuqetUUID() throws IOException {
    Schema nullableUuidSchema =
        record("R", optionalField("uuid", LogicalTypes.uuid().addToSchema(Schema.create(STRING))));
    GenericRecord u1 = instance(nullableUuidSchema, "uuid", UUID.randomUUID());
    GenericRecord u2 = instance(nullableUuidSchema, "uuid", null);

    GenericRecord s1 = instance(nullableUuidSchema, "uuid", u1.get("uuid").toString());
    GenericRecord s2 = instance(nullableUuidSchema, "uuid", null);

    File test = write(GENERIC, nullableUuidSchema, u1, u2);
    Assert.assertEquals(
        "Should read UUIDs as Strings",
        Arrays.asList(s1, s2),
        read(GenericData.get(), nullableUuidSchema, test));
  }

  @Test
  public void testReadDecimalFixed() throws IOException {
    Schema fixedSchema = Schema.createFixed("aFixed", null, null, 4);
    Schema fixedRecord = record("R", field("dec", fixedSchema));
    Schema decimalSchema = DECIMAL_9_2.addToSchema(Schema.createFixed("aFixed", null, null, 4));
    Schema decimalRecord = record("R", field("dec", decimalSchema));

    GenericRecord r1 = instance(decimalRecord, "dec", D1);
    GenericRecord r2 = instance(decimalRecord, "dec", D2);
    List<GenericRecord> expected = Arrays.asList(r1, r2);

    Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();

    // use the conversion directly instead of relying on the write side
    GenericRecord r1fixed = instance(fixedRecord, "dec", conversion.toFixed(D1, fixedSchema, DECIMAL_9_2));
    GenericRecord r2fixed = instance(fixedRecord, "dec", conversion.toFixed(D2, fixedSchema, DECIMAL_9_2));

    File test = write(fixedRecord, r1fixed, r2fixed);
    Assert.assertEquals("Should convert fixed to BigDecimals", expected, read(GENERIC, decimalRecord, test));
  }

  @Test
  public void testWriteDecimalFixed() throws IOException {
    Schema fixedSchema = Schema.createFixed("aFixed", null, null, 4);
    Schema fixedRecord = record("R", field("dec", fixedSchema));
    Schema decimalSchema = DECIMAL_9_2.addToSchema(Schema.createFixed("aFixed", null, null, 4));
    Schema decimalRecord = record("R", field("dec", decimalSchema));

    GenericRecord r1 = instance(decimalRecord, "dec", D1);
    GenericRecord r2 = instance(decimalRecord, "dec", D2);

    Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();

    // use the conversion directly instead of relying on the write side
    GenericRecord r1fixed = instance(fixedRecord, "dec", conversion.toFixed(D1, fixedSchema, DECIMAL_9_2));
    GenericRecord r2fixed = instance(fixedRecord, "dec", conversion.toFixed(D2, fixedSchema, DECIMAL_9_2));
    List<GenericRecord> expected = Arrays.asList(r1fixed, r2fixed);

    File test = write(GENERIC, decimalRecord, r1, r2);
    Assert.assertEquals("Should read BigDecimals as fixed", expected, read(GENERIC, fixedRecord, test));
  }

  @Test
  public void testReadDecimalBytes() throws IOException {
    Schema bytesSchema = Schema.create(Schema.Type.BYTES);
    Schema bytesRecord = record("R", field("dec", bytesSchema));
    Schema decimalSchema = DECIMAL_9_2.addToSchema(Schema.create(Schema.Type.BYTES));
    Schema decimalRecord = record("R", field("dec", decimalSchema));

    GenericRecord r1 = instance(decimalRecord, "dec", D1);
    GenericRecord r2 = instance(decimalRecord, "dec", D2);
    List<GenericRecord> expected = Arrays.asList(r1, r2);

    Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();

    // use the conversion directly instead of relying on the write side
    GenericRecord r1bytes = instance(bytesRecord, "dec", conversion.toBytes(D1, bytesSchema, DECIMAL_9_2));
    GenericRecord r2bytes = instance(bytesRecord, "dec", conversion.toBytes(D2, bytesSchema, DECIMAL_9_2));

    File test = write(bytesRecord, r1bytes, r2bytes);
    Assert.assertEquals("Should convert bytes to BigDecimals", expected, read(GENERIC, decimalRecord, test));
  }

  @Test
  public void testWriteDecimalBytes() throws IOException {
    Schema bytesSchema = Schema.create(Schema.Type.BYTES);
    Schema bytesRecord = record("R", field("dec", bytesSchema));
    Schema decimalSchema = DECIMAL_9_2.addToSchema(Schema.create(Schema.Type.BYTES));
    Schema decimalRecord = record("R", field("dec", decimalSchema));

    GenericRecord r1 = instance(decimalRecord, "dec", D1);
    GenericRecord r2 = instance(decimalRecord, "dec", D2);

    Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();

    // use the conversion directly instead of relying on the write side
    GenericRecord r1bytes = instance(bytesRecord, "dec", conversion.toBytes(D1, bytesSchema, DECIMAL_9_2));
    GenericRecord r2bytes = instance(bytesRecord, "dec", conversion.toBytes(D2, bytesSchema, DECIMAL_9_2));

    List<GenericRecord> expected = Arrays.asList(r1bytes, r2bytes);

    File test = write(GENERIC, decimalRecord, r1, r2);
    Assert.assertEquals("Should read BigDecimals as bytes", expected, read(GENERIC, bytesRecord, test));
  }

  private <D> File write(Schema schema, D... data) throws IOException {
    return write(GenericData.get(), schema, data);
  }

  private <D> File write(Configuration conf, Schema schema, D... data) throws IOException {
    return write(conf, GenericData.get(), schema, data);
  }

  private <D> File write(GenericData model, Schema schema, D... data) throws IOException {
    return AvroTestUtil.write(temp, model, schema, data);
  }

  private <D> File write(Configuration conf, GenericData model, Schema schema, D... data) throws IOException {
    return AvroTestUtil.write(temp, conf, model, schema, data);
  }
}
