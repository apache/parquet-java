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

import static org.apache.parquet.avro.AvroTestUtil.optional;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
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
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopCodecs;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestReadWrite {

  enum Converters {
    COMPATIBLE(true),
    NEW(false);

    final boolean compat;

    Converters(boolean compatible) {
      compat = compatible;
    }

    public boolean isCompatible() {
      return compat;
    }
  }

  enum FileLocation {
    LOCAL,
    HADOOP
  }

  enum ConfigurationType {
    HADOOP_CONFIGURATION,
    HADOOP_PARQUET_INTERFACE,
    PLAIN_PARQUET_INTERFACE
  }

  enum CodecFactory {
    IMPLICIT,
    EXPLICIT
  }

  @TempDir
  private java.nio.file.Path tempDir;

  static Stream<Arguments> data() {
    return Stream.of(
        Arguments.of(
            Converters.COMPATIBLE,
            FileLocation.HADOOP,
            ConfigurationType.HADOOP_CONFIGURATION,
            CodecFactory.IMPLICIT),
        Arguments.of(
            Converters.COMPATIBLE,
            FileLocation.HADOOP,
            ConfigurationType.HADOOP_PARQUET_INTERFACE,
            CodecFactory.IMPLICIT),
        Arguments.of(
            Converters.NEW,
            FileLocation.HADOOP,
            ConfigurationType.HADOOP_CONFIGURATION,
            CodecFactory.IMPLICIT),
        Arguments.of(
            Converters.NEW,
            FileLocation.LOCAL,
            ConfigurationType.HADOOP_CONFIGURATION,
            CodecFactory.IMPLICIT),
        Arguments.of(
            Converters.NEW,
            FileLocation.HADOOP,
            ConfigurationType.HADOOP_PARQUET_INTERFACE,
            CodecFactory.IMPLICIT),
        Arguments.of(
            Converters.NEW,
            FileLocation.LOCAL,
            ConfigurationType.HADOOP_PARQUET_INTERFACE,
            CodecFactory.IMPLICIT),
        Arguments.of(
            Converters.NEW,
            FileLocation.HADOOP,
            ConfigurationType.PLAIN_PARQUET_INTERFACE,
            CodecFactory.IMPLICIT),
        Arguments.of(
            Converters.NEW,
            FileLocation.LOCAL,
            ConfigurationType.PLAIN_PARQUET_INTERFACE,
            CodecFactory.IMPLICIT),
        Arguments.of(
            Converters.COMPATIBLE,
            FileLocation.HADOOP,
            ConfigurationType.HADOOP_PARQUET_INTERFACE,
            CodecFactory.EXPLICIT),
        Arguments.of(
            Converters.NEW,
            FileLocation.HADOOP,
            ConfigurationType.HADOOP_PARQUET_INTERFACE,
            CodecFactory.EXPLICIT),
        Arguments.of(
            Converters.NEW,
            FileLocation.LOCAL,
            ConfigurationType.HADOOP_PARQUET_INTERFACE,
            CodecFactory.EXPLICIT),
        Arguments.of(
            Converters.NEW,
            FileLocation.HADOOP,
            ConfigurationType.PLAIN_PARQUET_INTERFACE,
            CodecFactory.EXPLICIT),
        Arguments.of(
            Converters.NEW,
            FileLocation.LOCAL,
            ConfigurationType.PLAIN_PARQUET_INTERFACE,
            CodecFactory.EXPLICIT));
  }

  private Converters converter;
  private FileLocation fileLocation;
  private ConfigurationType conf;
  private CodecFactory codecType;

  private final Configuration testConf = new Configuration();
  private final ParquetConfiguration hadoopConfWithInterface = new HadoopParquetConfiguration();
  private final ParquetConfiguration plainParquetConf = new PlainParquetConfiguration();

  private void init(Converters converter, FileLocation fileLocation, ConfigurationType conf, CodecFactory codecs) {
    this.converter = converter;
    this.fileLocation = fileLocation;
    this.conf = conf;
    this.codecType = codecs;
    this.testConf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, converter.isCompatible());
    this.testConf.setBoolean("parquet.avro.add-list-element-records", false);
    this.testConf.setBoolean("parquet.avro.write-old-list-structure", false);
    this.hadoopConfWithInterface.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, converter.isCompatible());
    this.hadoopConfWithInterface.setBoolean("parquet.avro.add-list-element-records", false);
    this.hadoopConfWithInterface.setBoolean("parquet.avro.write-old-list-structure", false);
    this.plainParquetConf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, converter.isCompatible());
    this.plainParquetConf.setBoolean("parquet.avro.add-list-element-records", false);
    this.plainParquetConf.setBoolean("parquet.avro.write-old-list-structure", false);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testEmptyArray(
      Converters converter, FileLocation fileLocation, ConfigurationType conf, CodecFactory codecs)
      throws Exception {
    init(converter, fileLocation, conf, codecs);
    Schema schema =
        new Schema.Parser().parse(Resources.getResource("array.avsc").openStream());

    // Write a record with an empty array.
    List<Integer> emptyArray = new ArrayList<>();

    String file = createTempFile().toUri().getPath();

    try (ParquetWriter<GenericRecord> writer = writer(file, schema)) {
      GenericData.Record record =
          new GenericRecordBuilder(schema).set("myarray", emptyArray).build();
      writer.write(record);
    }

    try (ParquetReader<GenericRecord> reader = reader(file)) {
      GenericRecord nextRecord = reader.read();

      assertThat(nextRecord).isNotNull();
      assertThat(nextRecord.get("myarray")).isEqualTo(emptyArray);
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testEmptyMap(
      Converters converter, FileLocation fileLocation, ConfigurationType conf, CodecFactory codecs)
      throws Exception {
    init(converter, fileLocation, conf, codecs);
    Schema schema =
        new Schema.Parser().parse(Resources.getResource("map.avsc").openStream());

    String file = createTempFile().toUri().getPath();
    ImmutableMap<String, Integer> emptyMap = new ImmutableMap.Builder<String, Integer>().build();

    try (ParquetWriter<GenericRecord> writer = writer(file, schema)) {

      // Write a record with an empty map.
      GenericData.Record record =
          new GenericRecordBuilder(schema).set("mymap", emptyMap).build();
      writer.write(record);
    }

    try (ParquetReader<GenericRecord> reader = reader(file)) {
      GenericRecord nextRecord = reader.read();

      assertThat(nextRecord).isNotNull();
      assertThat(nextRecord.get("mymap")).isEqualTo(emptyMap);
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testMapWithNulls(
      Converters converter, FileLocation fileLocation, ConfigurationType conf, CodecFactory codecs)
      throws Exception {
    init(converter, fileLocation, conf, codecs);
    Schema schema = new Schema.Parser()
        .parse(Resources.getResource("map_with_nulls.avsc").openStream());

    Path file = createTempFile();

    // Write a record with a null value
    Map<CharSequence, Integer> map = new HashMap<>();
    map.put(str("thirty-four"), 34);
    map.put(str("eleventy-one"), null);
    map.put(str("one-hundred"), 100);

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(file)
        .withSchema(schema)
        .withConf(testConf)
        .build()) {

      GenericData.Record record =
          new GenericRecordBuilder(schema).set("mymap", map).build();
      writer.write(record);
    }

    try (AvroParquetReader<GenericRecord> reader = new AvroParquetReader<>(testConf, file)) {
      GenericRecord nextRecord = reader.read();

      assertThat(nextRecord).isNotNull();
      assertThat(nextRecord.get("mymap")).isEqualTo(map);
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testMapRequiredValueWithNull(
      Converters converter, FileLocation fileLocation, ConfigurationType conf, CodecFactory codecs)
      throws Exception {
    init(converter, fileLocation, conf, codecs);
    Schema schema = Schema.createRecord("record1", null, null, false);
    schema.setFields(Lists.newArrayList(
        new Schema.Field("mymap", Schema.createMap(Schema.create(Schema.Type.INT)), null, null)));

    Path file = createTempFile();

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(file)
        .withSchema(schema)
        .withConf(testConf)
        .build()) {

      // Write a record with a null value
      Map<String, Integer> map = new HashMap<String, Integer>();
      map.put("thirty-four", 34);
      map.put("eleventy-one", null);
      map.put("one-hundred", 100);

      GenericData.Record record =
          new GenericRecordBuilder(schema).set("mymap", map).build();
      assertThatThrownBy(() -> writer.write(record))
          .isInstanceOf(RuntimeException.class)
          .hasMessage("Null map value for map");
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testMapWithUtf8Key(
      Converters converter, FileLocation fileLocation, ConfigurationType conf, CodecFactory codecs)
      throws Exception {
    init(converter, fileLocation, conf, codecs);
    Schema schema =
        new Schema.Parser().parse(Resources.getResource("map.avsc").openStream());

    Path file = createTempFile();

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(file)
        .withSchema(schema)
        .withConf(testConf)
        .build()) {

      // Write a record with a map with Utf8 keys.
      GenericData.Record record = new GenericRecordBuilder(schema)
          .set("mymap", ImmutableMap.of(new Utf8("a"), 1, new Utf8("b"), 2))
          .build();
      writer.write(record);
    }

    try (AvroParquetReader<GenericRecord> reader = new AvroParquetReader<>(testConf, file)) {
      GenericRecord nextRecord = reader.read();

      assertThat(nextRecord).isNotNull();
      assertThat(nextRecord.get("mymap")).isEqualTo(ImmutableMap.of(str("a"), 1, str("b"), 2));
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testDecimalValues(
      Converters converter, FileLocation fileLocation, ConfigurationType conf, CodecFactory codecs)
      throws Exception {
    init(converter, fileLocation, conf, codecs);
    Schema decimalSchema = Schema.createRecord("myrecord", null, null, false);
    Schema decimal = LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    decimalSchema.setFields(Collections.singletonList(new Schema.Field("dec", decimal, null, null)));

    // add the decimal conversion to a generic data model
    GenericData decimalSupport = new GenericData();
    decimalSupport.addLogicalTypeConversion(new Conversions.DecimalConversion());

    Path path = new Path(tempDir.resolve("decimal.parquet").toUri());
    List<GenericRecord> expected = Lists.newArrayList();

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
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

    try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(path)
        .withDataModel(decimalSupport)
        .disableCompatibility()
        .build()) {
      GenericRecord rec;
      while ((rec = reader.read()) != null) {
        records.add(rec);
      }
    }

    assertThat(records.get(0).get("dec"))
        .as("dec field should be a BigDecimal instance")
        .isInstanceOf(BigDecimal.class);
    assertThat(records).as("Content should match").containsExactlyElementsOf(expected);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testFixedDecimalValues(
      Converters converter, FileLocation fileLocation, ConfigurationType conf, CodecFactory codecs)
      throws Exception {
    init(converter, fileLocation, conf, codecs);
    Schema decimalSchema = Schema.createRecord("myrecord", null, null, false);
    Schema decimal = LogicalTypes.decimal(9, 2).addToSchema(Schema.createFixed("dec", null, null, 4));
    decimalSchema.setFields(Collections.singletonList(new Schema.Field("dec", decimal, null, null)));

    // add the decimal conversion to a generic data model
    GenericData decimalSupport = new GenericData();
    decimalSupport.addLogicalTypeConversion(new Conversions.DecimalConversion());

    Path path = new Path(tempDir.resolve("decimal.parquet").toUri());
    List<GenericRecord> expected = Lists.newArrayList();

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
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

    try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(path)
        .withDataModel(decimalSupport)
        .disableCompatibility()
        .build()) {
      GenericRecord rec;
      while ((rec = reader.read()) != null) {
        records.add(rec);
      }
    }

    assertThat(records.get(0).get("dec"))
        .as("dec field should be a BigDecimal instance")
        .isInstanceOf(BigDecimal.class);
    assertThat(records).as("Content should match").containsExactlyElementsOf(expected);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testDecimalIntegerValues(
      Converters converter, FileLocation fileLocation, ConfigurationType conf, CodecFactory codecs)
      throws Exception {
    init(converter, fileLocation, conf, codecs);
    Path path =
        new Path(tempDir.resolve("test_decimal_integer_values.parquet").toUri());

    MessageType parquetSchema = new MessageType(
        "test_decimal_integer_values",
        new PrimitiveType(REQUIRED, INT32, "decimal_age")
            .withLogicalTypeAnnotation(LogicalTypeAnnotation.decimalType(2, 5)),
        new PrimitiveType(REQUIRED, INT64, "decimal_salary")
            .withLogicalTypeAnnotation(LogicalTypeAnnotation.decimalType(1, 10)));

    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(path).withType(parquetSchema).build()) {

      GroupFactory factory = new SimpleGroupFactory(parquetSchema);

      Group group1 = factory.newGroup();
      group1.add("decimal_age", 2534);
      group1.add("decimal_salary", 234L);
      writer.write(group1);

      Group group2 = factory.newGroup();
      group2.add("decimal_age", 4267);
      group2.add("decimal_salary", 1203L);
      writer.write(group2);
    }

    GenericData decimalSupport = new GenericData();
    decimalSupport.addLogicalTypeConversion(new Conversions.DecimalConversion());

    List<GenericRecord> records = Lists.newArrayList();
    try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(path)
        .withDataModel(decimalSupport)
        .build()) {
      GenericRecord rec;
      while ((rec = reader.read()) != null) {
        records.add(rec);
      }
    }

    assertThat(records).as("Should read 2 records").hasSize(2);

    // INT32 values
    Object firstAge = records.get(0).get("decimal_age");
    Object secondAge = records.get(1).get("decimal_age");

    assertThat(firstAge)
        .as("Should be BigDecimal, but is " + firstAge.getClass())
        .isInstanceOf(BigDecimal.class);
    assertThat(firstAge).as("Should be 25.34, but is " + firstAge).isEqualTo(new BigDecimal("25.34"));
    assertThat(secondAge).as("Should be 42.67, but is " + secondAge).isEqualTo(new BigDecimal("42.67"));

    // INT64 values
    Object firstSalary = records.get(0).get("decimal_salary");
    Object secondSalary = records.get(1).get("decimal_salary");

    assertThat(firstSalary)
        .as("Should be BigDecimal, but is " + firstSalary.getClass())
        .isInstanceOf(BigDecimal.class);
    assertThat(firstSalary).as("Should be 23.4, but is " + firstSalary).isEqualTo(new BigDecimal("23.4"));
    assertThat(secondSalary).as("Should be 120.3, but is " + secondSalary).isEqualTo(new BigDecimal("120.3"));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testAll(Converters converter, FileLocation fileLocation, ConfigurationType conf, CodecFactory codecs)
      throws Exception {
    init(converter, fileLocation, conf, codecs);
    Schema schema =
        new Schema.Parser().parse(Resources.getResource("all.avsc").openStream());

    Path file = createTempFile();
    List<Integer> integerArray = Arrays.asList(1, 2, 3);
    GenericData.Record nestedRecord = new GenericRecordBuilder(
            schema.getField("mynestedrecord").schema())
        .set("mynestedint", 1)
        .build();
    List<Integer> emptyArray = new ArrayList<Integer>();
    Schema arrayOfOptionalIntegers = Schema.createArray(optional(Schema.create(Schema.Type.INT)));
    GenericData.Array<Integer> genericIntegerArrayWithNulls =
        new GenericData.Array<Integer>(arrayOfOptionalIntegers, Arrays.asList(1, null, 2, null, 3));
    GenericFixed genericFixed =
        new GenericData.Fixed(Schema.createFixed("fixed", null, null, 1), new byte[] {(byte) 65});
    ImmutableMap<String, Integer> emptyMap = new ImmutableMap.Builder<String, Integer>().build();

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(file)
        .withSchema(schema)
        .withConf(testConf)
        .build()) {

      GenericData.Array<Integer> genericIntegerArray =
          new GenericData.Array<Integer>(Schema.createArray(Schema.create(Schema.Type.INT)), integerArray);

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
    try (AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(testConf, file)) {
      nextRecord = reader.read();
    }

    Object expectedEnumSymbol = converter.isCompatible()
        ? "a"
        : new GenericData.EnumSymbol(schema.getField("myenum").schema(), "a");

    assertThat(nextRecord).isNotNull();
    assertThat(nextRecord.get("mynull")).isEqualTo(null);
    assertThat(nextRecord.get("myboolean")).isEqualTo(true);
    assertThat(nextRecord.get("myint")).isEqualTo(1);
    assertThat(nextRecord.get("mylong")).isEqualTo(2L);
    assertThat(nextRecord.get("myfloat")).isEqualTo(3.1f);
    assertThat(nextRecord.get("mydouble")).isEqualTo(4.1);
    assertThat(nextRecord.get("mybytes")).isEqualTo(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)));
    assertThat(nextRecord.get("mystring")).isEqualTo(str("hello"));
    assertThat(nextRecord.get("myenum")).isEqualTo(expectedEnumSymbol);
    assertThat(nextRecord.get("mynestedrecord")).isEqualTo(nestedRecord);
    assertThat(nextRecord.get("myarray")).isEqualTo(integerArray);
    assertThat(nextRecord.get("myemptyarray")).isEqualTo(emptyArray);
    assertThat(nextRecord.get("myoptionalarray")).isEqualTo(integerArray);
    assertThat(nextRecord.get("myarrayofoptional")).isEqualTo(genericIntegerArrayWithNulls);
    assertThat(nextRecord.get("mymap")).isEqualTo(ImmutableMap.of(str("a"), 1, str("b"), 2));
    assertThat(nextRecord.get("myemptymap")).isEqualTo(emptyMap);
    assertThat(nextRecord.get("myfixed")).isEqualTo(genericFixed);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testAllUsingDefaultAvroSchema(
      Converters converter, FileLocation fileLocation, ConfigurationType conf, CodecFactory codecs)
      throws Exception {
    init(converter, fileLocation, conf, codecs);
    Path file = createTempFile();

    // write file using Parquet APIs
    try (ParquetWriter<Map<String, Object>> parquetWriter =
        new ParquetWriter<>(file, new WriteSupport<Map<String, Object>>() {

          private RecordConsumer recordConsumer;

          @Override
          public WriteContext init(Configuration configuration) {
            return init(new HadoopParquetConfiguration(configuration));
          }

          @Override
          public WriteContext init(ParquetConfiguration configuration) {
            return new WriteContext(
                MessageTypeParser.parseMessageType(TestAvroSchemaConverter.ALL_PARQUET_SCHEMA),
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
            recordConsumer.addBinary(Binary.fromReusedByteBuffer((ByteBuffer) record.get("mybytes")));
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
      record.put("myarray", new int[] {1, 2, 3});
      record.put("myoptionalarray", new int[] {1, 2, 3});
      record.put("myarrayofoptional", new Integer[] {1, null, 2, null, 3});
      record.put("myrecordarraya", new int[] {1, 2, 3});
      record.put("myrecordarrayb", new int[] {4, 5, 6});
      record.put("mymap", ImmutableMap.of("a", 1, "b", 2));
      record.put("myfixed", new byte[] {(byte) 65});
      parquetWriter.write(record);
    }

    Schema nestedRecordSchema = Schema.createRecord("mynestedrecord", null, null, false);
    nestedRecordSchema.setFields(
        Arrays.asList(new Schema.Field("mynestedint", Schema.create(Schema.Type.INT), null, null)));
    GenericData.Record nestedRecord = new GenericRecordBuilder(nestedRecordSchema)
        .set("mynestedint", 1)
        .build();

    List<Integer> integerArray = Arrays.asList(1, 2, 3);
    List<Integer> ingeterArrayWithNulls = Arrays.asList(1, null, 2, null, 3);

    Schema recordArraySchema = Schema.createRecord("array", null, null, false);
    recordArraySchema.setFields(Arrays.asList(
        new Schema.Field("a", Schema.create(Schema.Type.INT), null, null),
        new Schema.Field("b", Schema.create(Schema.Type.INT), null, null)));
    GenericRecordBuilder builder = new GenericRecordBuilder(recordArraySchema);
    List<GenericData.Record> recordArray = new ArrayList<GenericData.Record>();
    recordArray.add(builder.set("a", 1).set("b", 4).build());
    recordArray.add(builder.set("a", 2).set("b", 5).build());
    recordArray.add(builder.set("a", 3).set("b", 6).build());
    GenericData.Array<GenericData.Record> genericRecordArray =
        new GenericData.Array<GenericData.Record>(Schema.createArray(recordArraySchema), recordArray);

    GenericFixed genericFixed =
        new GenericData.Fixed(Schema.createFixed("fixed", null, null, 1), new byte[] {(byte) 65});

    try (AvroParquetReader<GenericRecord> reader = new AvroParquetReader<>(testConf, file)) {
      GenericRecord nextRecord = reader.read();
      assertThat(nextRecord).isNotNull();
      assertThat(nextRecord.get("myboolean")).isEqualTo(true);
      assertThat(nextRecord.get("myint")).isEqualTo(1);
      assertThat(nextRecord.get("mylong")).isEqualTo(2L);
      assertThat(nextRecord.get("myfloat")).isEqualTo(3.1f);
      assertThat(nextRecord.get("mydouble")).isEqualTo(4.1);
      assertThat(nextRecord.get("mybytes")).isEqualTo(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)));
      assertThat(nextRecord.get("mystring")).isEqualTo(str("hello"));
      assertThat(nextRecord.get("myenum")).isEqualTo(str("a")); // enum symbols are unknown
      assertThat(nextRecord.get("mynestedrecord")).isEqualTo(nestedRecord);
      assertThat(nextRecord.get("myarray")).isEqualTo(integerArray);
      assertThat(nextRecord.get("myoptionalarray")).isEqualTo(integerArray);
      assertThat(nextRecord.get("myarrayofoptional")).isEqualTo(ingeterArrayWithNulls);
      assertThat(nextRecord.get("myrecordarray")).isEqualTo(genericRecordArray);
      assertThat(nextRecord.get("mymap")).isEqualTo(ImmutableMap.of(str("a"), 1, str("b"), 2));
      assertThat(nextRecord.get("myfixed")).isEqualTo(genericFixed);
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testUnionWithSingleNonNullType(
      Converters converter, FileLocation fileLocation, ConfigurationType conf, CodecFactory codecs)
      throws Exception {
    init(converter, fileLocation, conf, codecs);
    Schema avroSchema = Schema.createRecord("SingleStringUnionRecord", null, null, false);
    avroSchema.setFields(Collections.singletonList(
        new Schema.Field("value", Schema.createUnion(Schema.create(Schema.Type.STRING)), null, null)));

    Path file = createTempFile();

    // Parquet writer
    try (ParquetWriter parquetWriter = AvroParquetWriter.builder(file)
        .withSchema(avroSchema)
        .withConf(new Configuration())
        .build()) {

      GenericRecord record = new GenericRecordBuilder(avroSchema)
          .set("value", "theValue")
          .build();

      parquetWriter.write(record);
    }

    try (AvroParquetReader<GenericRecord> reader = new AvroParquetReader<>(testConf, file)) {
      GenericRecord nextRecord = reader.read();

      assertThat(nextRecord).isNotNull();
      assertThat(nextRecord.get("value")).isEqualTo(str("theValue"));
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testDuplicatedValuesWithDictionary(
      Converters converter, FileLocation fileLocation, ConfigurationType conf, CodecFactory codecs)
      throws Exception {
    init(converter, fileLocation, conf, codecs);
    Schema schema = SchemaBuilder.record("spark_schema")
        .fields()
        .optionalBytes("value")
        .endRecord();

    Path file = createTempFile();

    String[] records = {"one", "two", "three", "three", "two", "one", "zero"};
    try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(file)
        .withSchema(schema)
        .withConf(testConf)
        .build()) {
      for (String record : records) {
        writer.write(new GenericRecordBuilder(schema)
            .set("value", record.getBytes())
            .build());
      }
    }

    try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file)
        .withConf(testConf)
        .build()) {
      GenericRecord rec;
      int i = 0;
      while ((rec = reader.read()) != null) {
        ByteBuffer buf = (ByteBuffer) rec.get("value");
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        assertThat(new String(bytes)).isEqualTo(records[i++]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testNestedLists(
      Converters converter, FileLocation fileLocation, ConfigurationType conf, CodecFactory codecs)
      throws Exception {
    init(converter, fileLocation, conf, codecs);
    Schema schema = new Schema.Parser()
        .parse(Resources.getResource("nested_array.avsc").openStream());
    String file = createTempFile().toUri().getPath();

    // Parquet writer
    ParquetWriter parquetWriter = writer(file, schema);

    Schema innerRecordSchema = schema.getField("l1")
        .schema()
        .getTypes()
        .get(1)
        .getElementType()
        .getTypes()
        .get(1);

    GenericRecord record = new GenericRecordBuilder(schema)
        .set(
            "l1",
            Collections.singletonList(new GenericRecordBuilder(innerRecordSchema)
                .set("l2", Collections.singletonList("hello"))
                .build()))
        .build();

    parquetWriter.write(record);
    parquetWriter.close();

    ParquetReader<GenericRecord> reader = reader(file);
    GenericRecord nextRecord = reader.read();

    assertThat(nextRecord).isNotNull();
    assertThat(nextRecord.get("l1")).isNotNull();
    List l1List = (List) nextRecord.get("l1");
    assertThat(l1List.get(0)).isNotNull();
    List l2List = (List) ((GenericRecord) l1List.get(0)).get("l2");
    assertThat(l2List.get(0)).isEqualTo(str("hello"));
  }

  /**
   * A test demonstrating the most simple way to write and read Parquet files
   * using Avro {@link GenericRecord}.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testSimpleGeneric(
      Converters converter, FileLocation fileLocation, ConfigurationType conf, CodecFactory codecs)
      throws IOException {
    init(converter, fileLocation, conf, codecs);
    final Schema schema = Schema.createRecord("Person", null, "org.apache.parquet", false);
    schema.setFields(Arrays.asList(
        new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("weight", Schema.create(Schema.Type.INT), null, null)));

    final Path file = createTempFile();

    try (final ParquetWriter<GenericData.Record> parquetWriter = AvroParquetWriter.<GenericData.Record>builder(file)
        .withSchema(schema)
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
    try (ParquetReader<GenericRecord> reader = AvroParquetReader.genericRecordReader(file)) {

      final GenericRecord r1 = reader.read();
      assertThat(r1.get("name")).asString().isEqualTo("foo");
      assertThat(r1.get("weight")).isEqualTo(123);

      final GenericRecord r2 = reader.read();
      assertThat(r2.get("name")).asString().isEqualTo("oof");
      assertThat(r2.get("weight")).isEqualTo(321);
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

  @ParameterizedTest
  @MethodSource("data")
  public void testParsesDataModelFromConf(
      Converters converter, FileLocation fileLocation, ConfigurationType conf, CodecFactory codecs)
      throws Exception {
    init(converter, fileLocation, conf, codecs);
    Schema datetimeSchema = Schema.createRecord("myrecord", null, null, false);
    Schema date = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    datetimeSchema.setFields(Collections.singletonList(new Schema.Field("date", date, null, null)));

    Path path = new Path(tempDir.resolve("datetime.parquet").toUri());
    List<GenericRecord> expected = Lists.newArrayList();

    Configuration configuration = new Configuration();
    AvroWriteSupport.setAvroDataSupplier(configuration, CustomDataModel.class);

    // .withDataModel is not set; AvroWriteSupport should parse it from the Configuration
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withConf(configuration)
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

    AvroReadSupport.setAvroDataSupplier(configuration, CustomDataModel.class);

    try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(path)
        .disableCompatibility()
        .withConf(configuration)
        .build()) {
      GenericRecord rec;
      while ((rec = reader.read()) != null) {
        records.add(rec);
      }
    }

    assertThat(records.get(0).get("date"))
        .as("date field should be a LocalDate instance")
        .isInstanceOf(LocalDate.class);
    assertThat(records).as("Content should match").containsExactlyElementsOf(expected);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testConstructor(
      Converters converter, FileLocation fileLocation, ConfigurationType conf, CodecFactory codecs)
      throws IOException {
    init(converter, fileLocation, conf, codecs);
    String testFile =
        URI.create(Resources.getResource("strings-2.parquet").getFile()).getRawPath();
    InputFile inputFile = new LocalInputFile(Paths.get(testFile));
    ParquetReader<Group> reader =
        AvroParquetReader.<Group>builder(inputFile).build();
    assertThat(reader).isNotNull();

    reader = AvroParquetReader.<Group>builder(inputFile, new HadoopParquetConfiguration(new Configuration()))
        .build();
    assertThat(reader).isNotNull();

    reader = AvroParquetReader.builder(new GroupReadSupport(), new Path(testFile))
        .build();
    assertThat(reader).isNotNull();
  }

  private Path createTempFile() {
    return new Path(tempDir.resolve(java.util.UUID.randomUUID() + ".tmp").toUri());
  }

  private ParquetWriter<GenericRecord> writer(String file, Schema schema) throws IOException {
    AvroParquetWriter.Builder<GenericRecord> writerBuilder;
    if (fileLocation == FileLocation.LOCAL) {
      writerBuilder = AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(Paths.get(file)))
          .withSchema(schema);
    } else {
      writerBuilder =
          AvroParquetWriter.<GenericRecord>builder(new Path(file)).withSchema(schema);
    }
    if (codecType == CodecFactory.EXPLICIT) {
      writerBuilder =
          writerBuilder.withCodecFactory(HadoopCodecs.newFactory(ParquetProperties.DEFAULT_PAGE_SIZE));
    }
    if (conf == ConfigurationType.PLAIN_PARQUET_INTERFACE) {
      return writerBuilder.withConf(plainParquetConf).build();
    } else if (conf == ConfigurationType.HADOOP_PARQUET_INTERFACE) {
      return writerBuilder.withConf(hadoopConfWithInterface).build();
    } else {
      return writerBuilder.withConf(testConf).build();
    }
  }

  private ParquetReader<GenericRecord> reader(String file) throws IOException {
    AvroParquetReader.Builder<GenericRecord> readerBuilder;
    if (fileLocation == FileLocation.LOCAL) {
      readerBuilder = AvroParquetReader.<GenericRecord>builder(new LocalInputFile(Paths.get(file)))
          .withDataModel(GenericData.get());
    } else {
      return new AvroParquetReader<>(testConf, new Path(file));
    }
    if (codecType == CodecFactory.EXPLICIT) {
      readerBuilder = (AvroParquetReader.Builder<GenericRecord>)
          readerBuilder.withCodecFactory(HadoopCodecs.newFactory(ParquetProperties.DEFAULT_PAGE_SIZE));
    }
    if (conf == ConfigurationType.PLAIN_PARQUET_INTERFACE) {
      return readerBuilder.withConf(plainParquetConf).build();
    } else if (conf == ConfigurationType.HADOOP_PARQUET_INTERFACE) {
      return readerBuilder.withConf(hadoopConfWithInterface).build();
    } else {
      return readerBuilder.withConf(testConf).build();
    }
  }

  /**
   * Return a String or Utf8 depending on whether compatibility is on
   */
  public CharSequence str(String value) {
    return converter.isCompatible() ? value : new Utf8(value);
  }
}
