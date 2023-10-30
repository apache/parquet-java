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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.parquet.avro.AvroTestUtil.optional;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class TestReadWrite {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] {
        { false, false, false },  // use the new converters with hadoop config
        { true, false, false },   // use the old converters with hadoop config
        { false, true, false },   // use a local disk location with hadoop config
        { false, false, true },   // use the new converters with parquet config interface
        { true, false, true },    // use the old converters with parquet config interface
        { false, true, true } };  // use a local disk location with parquet config interface
    return Arrays.asList(data);
  }

  private final boolean compat;
  private final boolean local;
  private final boolean confInterface;
  private final Configuration testConf = new Configuration();
  private final ParquetConfiguration parquetConf = new HadoopParquetConfiguration(true);

  public TestReadWrite(boolean compat, boolean local, boolean confInterface) {
    this.compat = compat;
    this.local = local;
    this.confInterface = confInterface;
    this.testConf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, compat);
    this.testConf.setBoolean("parquet.avro.add-list-element-records", false);
    this.testConf.setBoolean("parquet.avro.write-old-list-structure", false);
    this.parquetConf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, compat);
    this.parquetConf.setBoolean("parquet.avro.add-list-element-records", false);
    this.parquetConf.setBoolean("parquet.avro.write-old-list-structure", false);
  }

  @Test
  public void testEmptyArray() throws Exception {
    Schema schema = new Schema.Parser().parse(
      Resources.getResource("array.avsc").openStream());

    // Write a record with an empty array.
    List<Integer> emptyArray = new ArrayList<>();

    String file = createTempFile().getPath();

    try(ParquetWriter<GenericRecord> writer = writer(file, schema)) {
      GenericData.Record record = new GenericRecordBuilder(schema)
        .set("myarray", emptyArray).build();
      writer.write(record);
    }

    try (ParquetReader<GenericRecord> reader = reader(file)) {
      GenericRecord nextRecord = reader.read();

      assertNotNull(nextRecord);
      assertEquals(emptyArray, nextRecord.get("myarray"));
    }
  }

  @Test
  public void testEmptyMap() throws Exception {
    Schema schema = new Schema.Parser().parse(
      Resources.getResource("map.avsc").openStream());

    String file = createTempFile().getPath();
    ImmutableMap<String, Integer> emptyMap = new ImmutableMap.Builder<String, Integer>().build();

    try (ParquetWriter<GenericRecord> writer = writer(file, schema)) {

      // Write a record with an empty map.
      GenericData.Record record = new GenericRecordBuilder(schema)
        .set("mymap", emptyMap).build();
      writer.write(record);
    }

    try(ParquetReader<GenericRecord> reader = reader(file)) {
      GenericRecord nextRecord = reader.read();

      assertNotNull(nextRecord);
      assertEquals(emptyMap, nextRecord.get("mymap"));
    }
  }

  @Test
  public void testMapWithNulls() throws Exception {
    Schema schema = new Schema.Parser().parse(
        Resources.getResource("map_with_nulls.avsc").openStream());

    Path file = new Path(createTempFile().getPath());

    // Write a record with a null value
    Map<CharSequence, Integer> map = new HashMap<>();
    map.put(str("thirty-four"), 34);
    map.put(str("eleventy-one"), null);
    map.put(str("one-hundred"), 100);

    try(ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(file)
        .withSchema(schema)
        .withConf(testConf)
        .build()) {

      GenericData.Record record = new GenericRecordBuilder(schema)
        .set("mymap", map).build();
      writer.write(record);
    }

    try(AvroParquetReader<GenericRecord> reader = new AvroParquetReader<>(testConf, file)) {
      GenericRecord nextRecord = reader.read();

      assertNotNull(nextRecord);
      assertEquals(map, nextRecord.get("mymap"));
    }
  }

  @Test(expected=RuntimeException.class)
  public void testMapRequiredValueWithNull() throws Exception {
    Schema schema = Schema.createRecord("record1", null, null, false);
    schema.setFields(Lists.newArrayList(
        new Schema.Field("mymap", Schema.createMap(Schema.create(Schema.Type.INT)), null, null)));

    Path file = new Path(createTempFile().getPath());

    try(ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(file)
        .withSchema(schema)
        .withConf(testConf)
        .build()) {

      // Write a record with a null value
      Map<String, Integer> map = new HashMap<String, Integer>();
      map.put("thirty-four", 34);
      map.put("eleventy-one", null);
      map.put("one-hundred", 100);

      GenericData.Record record = new GenericRecordBuilder(schema)
        .set("mymap", map).build();
      writer.write(record);
    }
  }

  @Test
  public void testMapWithUtf8Key() throws Exception {
    Schema schema = new Schema.Parser().parse(
        Resources.getResource("map.avsc").openStream());

    Path file = new Path(createTempFile().getPath());

    try(ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(file)
        .withSchema(schema)
        .withConf(testConf)
        .build()) {

      // Write a record with a map with Utf8 keys.
      GenericData.Record record = new GenericRecordBuilder(schema)
        .set("mymap", ImmutableMap.of(new Utf8("a"), 1, new Utf8("b"), 2))
        .build();
      writer.write(record);
    }

    try(AvroParquetReader<GenericRecord> reader = new AvroParquetReader<>(testConf, file)) {
      GenericRecord nextRecord = reader.read();

      assertNotNull(nextRecord);
      assertEquals(ImmutableMap.of(str("a"), 1, str("b"), 2), nextRecord.get("mymap"));
    }
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testDecimalValues() throws Exception {
    Schema decimalSchema = Schema.createRecord("myrecord", null, null, false);
    Schema decimal = LogicalTypes.decimal(9, 2).addToSchema(
        Schema.create(Schema.Type.BYTES));
    decimalSchema.setFields(Collections.singletonList(
        new Schema.Field("dec", decimal, null, null)));

    // add the decimal conversion to a generic data model
    GenericData decimalSupport = new GenericData();
    decimalSupport.addLogicalTypeConversion(new Conversions.DecimalConversion());

    File file = temp.newFile("decimal.parquet");
    file.delete();
    Path path = new Path(file.toString());
    List<GenericRecord> expected = Lists.newArrayList();

    try(ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(path)
        .withDataModel(decimalSupport)
        .withSchema(decimalSchema)
        .build()) {

      Random random = new Random(34L);
      GenericRecordBuilder builder = new GenericRecordBuilder(decimalSchema);
      for (int i = 0; i < 1000; i += 1) {
        // Generating Integers between -(2^29) and (2^29 - 1) to ensure the number of digits <= 9
        BigDecimal dec = new BigDecimal(new BigInteger(30, random).subtract(BigInteger.valueOf(1L << 28)), 2);
        builder.set("dec", dec);

        GenericRecord rec = builder.build();
        expected.add(rec);
        writer.write(builder.build());
      }
    }
    List<GenericRecord> records = Lists.newArrayList();

    try(ParquetReader<GenericRecord> reader = AvroParquetReader
        .<GenericRecord>builder(path)
        .withDataModel(decimalSupport)
        .disableCompatibility()
        .build()) {
      GenericRecord rec;
      while ((rec = reader.read()) != null) {
        records.add(rec);
      }
    }

    Assert.assertTrue("dec field should be a BigDecimal instance",
        records.get(0).get("dec") instanceof BigDecimal);
    Assert.assertEquals("Content should match", expected, records);
  }

  @Test
  public void testFixedDecimalValues() throws Exception {
    Schema decimalSchema = Schema.createRecord("myrecord", null, null, false);
    Schema decimal = LogicalTypes.decimal(9, 2).addToSchema(
        Schema.createFixed("dec", null, null, 4));
    decimalSchema.setFields(Collections.singletonList(
        new Schema.Field("dec", decimal, null, null)));

    // add the decimal conversion to a generic data model
    GenericData decimalSupport = new GenericData();
    decimalSupport.addLogicalTypeConversion(new Conversions.DecimalConversion());

    File file = temp.newFile("decimal.parquet");
    file.delete();
    Path path = new Path(file.toString());
    List<GenericRecord> expected = Lists.newArrayList();

    try(ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(path)
        .withDataModel(decimalSupport)
        .withSchema(decimalSchema)
        .build()) {

      Random random = new Random(34L);
      GenericRecordBuilder builder = new GenericRecordBuilder(decimalSchema);
      for (int i = 0; i < 1000; i += 1) {
        // Generating Integers between -(2^29) and (2^29 - 1) to ensure the number of digits <= 9
        BigDecimal dec = new BigDecimal(new BigInteger(30, random).subtract(BigInteger.valueOf(1L << 28)), 2);
        builder.set("dec", dec);

        GenericRecord rec = builder.build();
        expected.add(rec);
        writer.write(builder.build());
      }
    }
    List<GenericRecord> records = Lists.newArrayList();

    try(ParquetReader<GenericRecord> reader = AvroParquetReader
        .<GenericRecord>builder(path)
        .withDataModel(decimalSupport)
        .disableCompatibility()
        .build()) {
      GenericRecord rec;
      while ((rec = reader.read()) != null) {
        records.add(rec);
      }
    }

    Assert.assertTrue("dec field should be a BigDecimal instance",
        records.get(0).get("dec") instanceof BigDecimal);
    Assert.assertEquals("Content should match", expected, records);
  }

  @Test
  public void testAll() throws Exception {
    Schema schema = new Schema.Parser().parse(
        Resources.getResource("all.avsc").openStream());

    Path file = new Path(createTempFile().getPath());
    List<Integer> integerArray = Arrays.asList(1, 2, 3);
    GenericData.Record nestedRecord = new GenericRecordBuilder(
      schema.getField("mynestedrecord").schema())
      .set("mynestedint", 1).build();
    List<Integer> emptyArray = new ArrayList<Integer>();
    Schema arrayOfOptionalIntegers = Schema.createArray(
      optional(Schema.create(Schema.Type.INT)));
    GenericData.Array<Integer> genericIntegerArrayWithNulls =
      new GenericData.Array<Integer>(
        arrayOfOptionalIntegers,
        Arrays.asList(1, null, 2, null, 3));
    GenericFixed genericFixed = new GenericData.Fixed(
      Schema.createFixed("fixed", null, null, 1), new byte[]{(byte) 65});
    ImmutableMap<String, Integer> emptyMap = new ImmutableMap.Builder<String, Integer>().build();

    try(ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(file)
        .withSchema(schema)
        .withConf(testConf)
        .build()) {

      GenericData.Array<Integer> genericIntegerArray = new GenericData.Array<Integer>(
        Schema.createArray(Schema.create(Schema.Type.INT)), integerArray);

      GenericData.Record record = new GenericRecordBuilder(schema)
        .set("mynull", null)
        .set("myboolean", true)
        .set("myint", 1)
        .set("mylong", 2L)
        .set("myfloat", 3.1f)
        .set("mydouble", 4.1)
        .set("mybytes", ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)))
        .set("mystring", "hello")
        .set("mynestedrecord", nestedRecord)
        .set("myenum", "a")
        .set("myarray", genericIntegerArray)
        .set("myemptyarray", emptyArray)
        .set("myoptionalarray", genericIntegerArray)
        .set("myarrayofoptional", genericIntegerArrayWithNulls)
        .set("mymap", ImmutableMap.of("a", 1, "b", 2))
        .set("myemptymap", emptyMap)
        .set("myfixed", genericFixed)
        .build();

      writer.write(record);
    }

    final GenericRecord nextRecord;
    try(AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(testConf, file)) {
      nextRecord = reader.read();
    }

    Object expectedEnumSymbol = compat ? "a" :
        new GenericData.EnumSymbol(schema.getField("myenum").schema(), "a");

    assertNotNull(nextRecord);
    assertEquals(null, nextRecord.get("mynull"));
    assertEquals(true, nextRecord.get("myboolean"));
    assertEquals(1, nextRecord.get("myint"));
    assertEquals(2L, nextRecord.get("mylong"));
    assertEquals(3.1f, nextRecord.get("myfloat"));
    assertEquals(4.1, nextRecord.get("mydouble"));
    assertEquals(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)), nextRecord.get("mybytes"));
    assertEquals(str("hello"), nextRecord.get("mystring"));
    assertEquals(expectedEnumSymbol, nextRecord.get("myenum"));
    assertEquals(nestedRecord, nextRecord.get("mynestedrecord"));
    assertEquals(integerArray, nextRecord.get("myarray"));
    assertEquals(emptyArray, nextRecord.get("myemptyarray"));
    assertEquals(integerArray, nextRecord.get("myoptionalarray"));
    assertEquals(genericIntegerArrayWithNulls, nextRecord.get("myarrayofoptional"));
    assertEquals(ImmutableMap.of(str("a"), 1, str("b"), 2), nextRecord.get("mymap"));
    assertEquals(emptyMap, nextRecord.get("myemptymap"));
    assertEquals(genericFixed, nextRecord.get("myfixed"));
  }

  @Test
  public void testAllUsingDefaultAvroSchema() throws Exception {
    Path file = new Path(createTempFile().getPath());

    // write file using Parquet APIs
    try(ParquetWriter<Map<String, Object>> parquetWriter = new ParquetWriter<>(file,
        new WriteSupport<Map<String, Object>>() {

      private RecordConsumer recordConsumer;

      @Override
      public WriteContext init(Configuration configuration) {
        return init(new HadoopParquetConfiguration(configuration));
      }

      @Override
      public WriteContext init(ParquetConfiguration configuration) {
        return new WriteContext(MessageTypeParser.parseMessageType(TestAvroSchemaConverter.ALL_PARQUET_SCHEMA),
            new HashMap<String, String>());
      }

      @Override
      public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
      }

      @Override
      public void write(Map<String, Object> record) {
        recordConsumer.startMessage();

        int index = 0;

        recordConsumer.startField("myboolean", index);
        recordConsumer.addBoolean((Boolean) record.get("myboolean"));
        recordConsumer.endField("myboolean", index++);

        recordConsumer.startField("myint", index);
        recordConsumer.addInteger((Integer) record.get("myint"));
        recordConsumer.endField("myint", index++);

        recordConsumer.startField("mylong", index);
        recordConsumer.addLong((Long) record.get("mylong"));
        recordConsumer.endField("mylong", index++);

        recordConsumer.startField("myfloat", index);
        recordConsumer.addFloat((Float) record.get("myfloat"));
        recordConsumer.endField("myfloat", index++);

        recordConsumer.startField("mydouble", index);
        recordConsumer.addDouble((Double) record.get("mydouble"));
        recordConsumer.endField("mydouble", index++);

        recordConsumer.startField("mybytes", index);
        recordConsumer.addBinary(
            Binary.fromReusedByteBuffer((ByteBuffer) record.get("mybytes")));
        recordConsumer.endField("mybytes", index++);

        recordConsumer.startField("mystring", index);
        recordConsumer.addBinary(Binary.fromString((String) record.get("mystring")));
        recordConsumer.endField("mystring", index++);

        recordConsumer.startField("mynestedrecord", index);
        recordConsumer.startGroup();
        recordConsumer.startField("mynestedint", 0);
        recordConsumer.addInteger((Integer) record.get("mynestedint"));
        recordConsumer.endField("mynestedint", 0);
        recordConsumer.endGroup();
        recordConsumer.endField("mynestedrecord", index++);

        recordConsumer.startField("myenum", index);
        recordConsumer.addBinary(Binary.fromString((String) record.get("myenum")));
        recordConsumer.endField("myenum", index++);

        recordConsumer.startField("myarray", index);
        recordConsumer.startGroup();
        recordConsumer.startField("array", 0);
        for (int val : (int[]) record.get("myarray")) {
          recordConsumer.addInteger(val);
        }
        recordConsumer.endField("array", 0);
        recordConsumer.endGroup();
        recordConsumer.endField("myarray", index++);

        recordConsumer.startField("myoptionalarray", index);
        recordConsumer.startGroup();
        recordConsumer.startField("array", 0);
        for (int val : (int[]) record.get("myoptionalarray")) {
          recordConsumer.addInteger(val);
        }
        recordConsumer.endField("array", 0);
        recordConsumer.endGroup();
        recordConsumer.endField("myoptionalarray", index++);

        recordConsumer.startField("myarrayofoptional", index);
        recordConsumer.startGroup();
        recordConsumer.startField("list", 0);
        for (Integer val : (Integer[]) record.get("myarrayofoptional")) {
          recordConsumer.startGroup();
          if (val != null) {
            recordConsumer.startField("element", 0);
            recordConsumer.addInteger(val);
            recordConsumer.endField("element", 0);
          }
          recordConsumer.endGroup();
        }
        recordConsumer.endField("list", 0);
        recordConsumer.endGroup();
        recordConsumer.endField("myarrayofoptional", index++);

        recordConsumer.startField("myrecordarray", index);
        recordConsumer.startGroup();
        recordConsumer.startField("array", 0);
        recordConsumer.startGroup();
        recordConsumer.startField("a", 0);
        for (int val : (int[]) record.get("myrecordarraya")) {
          recordConsumer.addInteger(val);
        }
        recordConsumer.endField("a", 0);
        recordConsumer.startField("b", 1);
        for (int val : (int[]) record.get("myrecordarrayb")) {
          recordConsumer.addInteger(val);
        }
        recordConsumer.endField("b", 1);
        recordConsumer.endGroup();
        recordConsumer.endField("array", 0);
        recordConsumer.endGroup();
        recordConsumer.endField("myrecordarray", index++);

        recordConsumer.startField("mymap", index);
        recordConsumer.startGroup();
        recordConsumer.startField("key_value", 0);
        recordConsumer.startGroup();
        Map<String, Integer> mymap = (Map<String, Integer>) record.get("mymap");
        recordConsumer.startField("key", 0);
        for (String key : mymap.keySet()) {
          recordConsumer.addBinary(Binary.fromString(key));
        }
        recordConsumer.endField("key", 0);
        recordConsumer.startField("value", 1);
        for (int val : mymap.values()) {
          recordConsumer.addInteger(val);
        }
        recordConsumer.endField("value", 1);
        recordConsumer.endGroup();
        recordConsumer.endField("key_value", 0);
        recordConsumer.endGroup();
        recordConsumer.endField("mymap", index++);

        recordConsumer.startField("myfixed", index);
        recordConsumer.addBinary(Binary.fromReusedByteArray((byte[]) record.get("myfixed")));
        recordConsumer.endField("myfixed", index++);

        recordConsumer.endMessage();
      }
    })) {
      Map<String, Object> record = new HashMap<String, Object>();
      record.put("myboolean", true);
      record.put("myint", 1);
      record.put("mylong", 2L);
      record.put("myfloat", 3.1f);
      record.put("mydouble", 4.1);
      record.put("mybytes", ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)));
      record.put("mystring", "hello");
      record.put("myenum", "a");
      record.put("mynestedint", 1);
      record.put("myarray", new int[]{1, 2, 3});
      record.put("myoptionalarray", new int[]{1, 2, 3});
      record.put("myarrayofoptional", new Integer[]{1, null, 2, null, 3});
      record.put("myrecordarraya", new int[]{1, 2, 3});
      record.put("myrecordarrayb", new int[]{4, 5, 6});
      record.put("mymap", ImmutableMap.of("a", 1, "b", 2));
      record.put("myfixed", new byte[]{(byte) 65});
      parquetWriter.write(record);
    }

    Schema nestedRecordSchema = Schema.createRecord("mynestedrecord", null, null, false);
    nestedRecordSchema.setFields(Arrays.asList(
        new Schema.Field("mynestedint", Schema.create(Schema.Type.INT), null, null)
    ));
    GenericData.Record nestedRecord = new GenericRecordBuilder(nestedRecordSchema)
        .set("mynestedint", 1).build();

    List<Integer> integerArray = Arrays.asList(1, 2, 3);
    List<Integer> ingeterArrayWithNulls = Arrays.asList(1, null, 2, null, 3);

    Schema recordArraySchema = Schema.createRecord("array", null, null, false);
    recordArraySchema.setFields(Arrays.asList(
        new Schema.Field("a", Schema.create(Schema.Type.INT), null, null),
        new Schema.Field("b", Schema.create(Schema.Type.INT), null, null)
    ));
    GenericRecordBuilder builder = new GenericRecordBuilder(recordArraySchema);
    List<GenericData.Record> recordArray = new ArrayList<GenericData.Record>();
    recordArray.add(builder.set("a", 1).set("b", 4).build());
    recordArray.add(builder.set("a", 2).set("b", 5).build());
    recordArray.add(builder.set("a", 3).set("b", 6).build());
    GenericData.Array<GenericData.Record> genericRecordArray = new GenericData.Array<GenericData.Record>(
        Schema.createArray(recordArraySchema), recordArray);

    GenericFixed genericFixed = new GenericData.Fixed(
        Schema.createFixed("fixed", null, null, 1), new byte[] { (byte) 65 });

    try(AvroParquetReader<GenericRecord> reader = new AvroParquetReader<>(testConf, file)) {
      GenericRecord nextRecord = reader.read();
      assertNotNull(nextRecord);
      assertEquals(true, nextRecord.get("myboolean"));
      assertEquals(1, nextRecord.get("myint"));
      assertEquals(2L, nextRecord.get("mylong"));
      assertEquals(3.1f, nextRecord.get("myfloat"));
      assertEquals(4.1, nextRecord.get("mydouble"));
      assertEquals(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)), nextRecord.get("mybytes"));
      assertEquals(str("hello"), nextRecord.get("mystring"));
      assertEquals(str("a"), nextRecord.get("myenum")); // enum symbols are unknown
      assertEquals(nestedRecord, nextRecord.get("mynestedrecord"));
      assertEquals(integerArray, nextRecord.get("myarray"));
      assertEquals(integerArray, nextRecord.get("myoptionalarray"));
      assertEquals(ingeterArrayWithNulls, nextRecord.get("myarrayofoptional"));
      assertEquals(genericRecordArray, nextRecord.get("myrecordarray"));
      assertEquals(ImmutableMap.of(str("a"), 1, str("b"), 2), nextRecord.get("mymap"));
      assertEquals(genericFixed, nextRecord.get("myfixed"));
    }
  }

  @Test
  public void testUnionWithSingleNonNullType() throws Exception {
    Schema avroSchema = Schema.createRecord("SingleStringUnionRecord", null, null, false);
    avroSchema.setFields(
      Collections.singletonList(new Schema.Field("value",
        Schema.createUnion(Schema.create(Schema.Type.STRING)), null, null)));

    Path file = new Path(createTempFile().getPath());

    // Parquet writer
    try(ParquetWriter parquetWriter = AvroParquetWriter.builder(file).withSchema(avroSchema)
      .withConf(new Configuration())
      .build()) {

      GenericRecord record = new GenericRecordBuilder(avroSchema)
        .set("value", "theValue")
        .build();

      parquetWriter.write(record);
    }

    try(AvroParquetReader<GenericRecord> reader = new AvroParquetReader<>(testConf, file)) {
      GenericRecord nextRecord = reader.read();

      assertNotNull(nextRecord);
      assertEquals(str("theValue"), nextRecord.get("value"));
    }
  }

  @Test
  public void testDuplicatedValuesWithDictionary() throws Exception {
    Schema schema = SchemaBuilder.record("spark_schema")
      .fields().optionalBytes("value").endRecord();

    Path file = new Path(createTempFile().getPath());

    String[] records = {"one", "two", "three", "three", "two", "one", "zero"};
    try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
      .<GenericData.Record>builder(file)
      .withSchema(schema)
      .withConf(testConf)
      .build()) {
      for (String record : records) {
        writer.write(new GenericRecordBuilder(schema)
          .set("value", record.getBytes()).build());
      }
    }

    try (ParquetReader<GenericRecord> reader = AvroParquetReader
      .<GenericRecord>builder(file)
      .withConf(testConf).build()) {
      GenericRecord rec;
      int i = 0;
      while ((rec = reader.read()) != null) {
        ByteBuffer buf = (ByteBuffer) rec.get("value");
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        assertEquals(records[i++], new String(bytes));
      }
    }
  }

  @Test
  public void testNestedLists() throws Exception {
    Schema schema = new Schema.Parser().parse(
      Resources.getResource("nested_array.avsc").openStream());
    String file = createTempFile().getPath();

    // Parquet writer
    ParquetWriter parquetWriter = writer(file, schema);

    Schema innerRecordSchema = schema.getField("l1").schema().getTypes()
      .get(1).getElementType().getTypes().get(1);

    GenericRecord record = new GenericRecordBuilder(schema)
      .set("l1", Collections.singletonList(
        new GenericRecordBuilder(innerRecordSchema).set("l2", Collections.singletonList("hello")).build()
      ))
      .build();

    parquetWriter.write(record);
    parquetWriter.close();

    ParquetReader<GenericRecord> reader = reader(file);
    GenericRecord nextRecord = reader.read();

    assertNotNull(nextRecord);
    assertNotNull(nextRecord.get("l1"));
    List l1List = (List) nextRecord.get("l1");
    assertNotNull(l1List.get(0));
    List l2List = (List) ((GenericRecord) l1List.get(0)).get("l2");
    assertEquals(str("hello"), l2List.get(0));
  }

  /**
   * A test demonstrating the most simple way to write and read Parquet files
   * using Avro {@link GenericRecord}.
   */
  @Test
  public void testSimpleGeneric() throws IOException {
    final Schema schema =
        Schema.createRecord("Person", null, "org.apache.parquet", false);
    schema.setFields(Arrays.asList(
        new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("weight", Schema.create(Schema.Type.INT), null,
            null)));

    final Path file = new Path(createTempFile().getPath());

    try (final ParquetWriter<GenericData.Record> parquetWriter =
        AvroParquetWriter.<GenericData.Record> builder(file).withSchema(schema)
            .build()) {

      final GenericData.Record fooRecord = new GenericData.Record(schema);
      fooRecord.put("name", "foo");
      fooRecord.put("weight", 123);

      final GenericData.Record oofRecord = new GenericData.Record(schema);
      oofRecord.put("name", "oof");
      oofRecord.put("weight", 321);

      parquetWriter.write(fooRecord);
      parquetWriter.write(oofRecord);
    }

    // Read the file. String data is returned as org.apache.avro.util.Utf8 so it
    // must be converting to a String before checking equality
    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.genericRecordReader(file)) {

      final GenericRecord r1 = reader.read();
      assertEquals("foo", r1.get("name").toString());
      assertEquals(123, r1.get("weight"));

      final GenericRecord r2 = reader.read();
      assertEquals("oof", r2.get("name").toString());
      assertEquals(321, r2.get("weight"));
    }
  }

  public static class CustomDataModel implements AvroDataSupplier {
    @Override
    public GenericData get() {
      GenericData genericData = new GenericData();
      genericData.addLogicalTypeConversion(new Conversion<LocalDate>() {
        private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        @Override
        public Class<LocalDate> getConvertedType() {
          return LocalDate.class;
        }

        @Override
        public String getLogicalTypeName() {
          return "date";
        }

        public LocalDate fromInt(Integer localDate, Schema schema, LogicalType type) {
          return LocalDate.parse(String.valueOf(localDate), dateTimeFormatter);
        }

        public Integer toInt(LocalDate date, Schema schema, LogicalType type) {
          return Integer.parseInt(dateTimeFormatter.format(date));
        }
      });
      return genericData;
    }
  }
  @Test
  public void testParsesDataModelFromConf() throws Exception {
    Schema datetimeSchema = Schema.createRecord("myrecord", null, null, false);
    Schema date = LogicalTypes.date().addToSchema(
      Schema.create(Schema.Type.INT));
    datetimeSchema.setFields(Collections.singletonList(
      new Schema.Field("date", date, null, null)));

    File file = temp.newFile("datetime.parquet");
    file.delete();
    Path path = new Path(file.toString());
    List<GenericRecord> expected = Lists.newArrayList();

    Configuration conf = new Configuration();
    AvroWriteSupport.setAvroDataSupplier(conf, CustomDataModel.class);

    // .withDataModel is not set; AvroWriteSupport should parse it from the Configuration
    try(ParquetWriter<GenericRecord> writer = AvroParquetWriter
      .<GenericRecord>builder(path)
      .withConf(conf)
      .withSchema(datetimeSchema)
      .build()) {

      GenericRecordBuilder builder = new GenericRecordBuilder(datetimeSchema);
      for (int i = 0; i < 100; i += 1) {
        builder.set("date", LocalDate.now().minusDays(i));

        GenericRecord rec = builder.build();
        expected.add(rec);
        writer.write(builder.build());
      }
    }
    List<GenericRecord> records = Lists.newArrayList();

    AvroReadSupport.setAvroDataSupplier(conf, CustomDataModel.class);

    try(ParquetReader<GenericRecord> reader = AvroParquetReader
      .<GenericRecord>builder(path)
      .disableCompatibility()
      .withConf(conf)
      .build()) {
      GenericRecord rec;
      while ((rec = reader.read()) != null) {
        records.add(rec);
      }
    }

    Assert.assertTrue("date field should be a LocalDate instance",
      records.get(0).get("date") instanceof LocalDate);
    Assert.assertEquals("Content should match", expected, records);
  }

  private File createTempFile() throws IOException {
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    return tmp;
  }

  private ParquetWriter<GenericRecord> writer(String file, Schema schema) throws IOException {
    AvroParquetWriter.Builder<GenericRecord> writerBuilder;
    if (local) {
      writerBuilder = AvroParquetWriter
        .<GenericRecord>builder(new LocalOutputFile(Paths.get(file)))
        .withSchema(schema);
    } else {
      writerBuilder = AvroParquetWriter
        .<GenericRecord>builder(new Path(file))
        .withSchema(schema);
    }
    if (confInterface) {
      return writerBuilder
        .withConf(parquetConf)
        .build();
    } else {
      return writerBuilder
        .withConf(testConf)
        .build();
    }
  }

  private ParquetReader<GenericRecord> reader(String file) throws IOException {
    AvroParquetReader.Builder<GenericRecord> readerBuilder;
    if (local) {
      readerBuilder = AvroParquetReader
        .<GenericRecord>builder(new LocalInputFile(Paths.get(file)))
        .withDataModel(GenericData.get());
    } else {
      return new AvroParquetReader<>(testConf, new Path(file));
    }
    if (confInterface) {
      return readerBuilder
        .withConf(parquetConf)
        .build();
    } else {
      return readerBuilder
        .withConf(testConf)
        .build();
    }
  }

  /**
   * Return a String or Utf8 depending on whether compatibility is on
   */
  public CharSequence str(String value) {
    return compat ? value : new Utf8(value);
  }
}
