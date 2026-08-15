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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.AvroSchema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.Stringable;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestStringBehavior {
  public static Schema SCHEMA = null;
  public static BigDecimal BIG_DECIMAL = new BigDecimal("3.14");

  @TempDir
  private java.nio.file.Path tempDir;

  public org.apache.hadoop.fs.Path parquetFile;
  public File avroFile;

  @BeforeAll
  public static void readSchemaFile() throws IOException {
    TestStringBehavior.SCHEMA = new Schema.Parser()
        .parse(Resources.getResource("stringBehavior.avsc").openStream());
  }

  @BeforeEach
  public void writeDataFiles() throws IOException {
    // convert BIG_DECIMAL to string by hand so generic can write it
    GenericRecord record = new GenericRecordBuilder(SCHEMA)
        .set("default_class", "default")
        .set("string_class", "string")
        .set("stringable_class", BIG_DECIMAL.toString())
        .set("default_map", ImmutableMap.of("default_key", 34))
        .set("string_map", ImmutableMap.of("string_key", 35))
        .set("stringable_map", ImmutableMap.of(BIG_DECIMAL.toString(), 36))
        .build();

    parquetFile = new Path(tempDir.resolve("parquet").toUri());
    try (ParquetWriter<GenericRecord> parquet = AvroParquetWriter.<GenericRecord>builder(parquetFile)
        .withDataModel(GenericData.get())
        .withSchema(SCHEMA)
        .build()) {
      parquet.write(record);
    }

    avroFile = tempDir.resolve("avro").toFile();
    try (DataFileWriter<GenericRecord> avro =
        new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(SCHEMA)).create(SCHEMA, avroFile)) {
      avro.append(record);
    }
  }

  @Test
  public void testGeneric() throws IOException {
    GenericRecord avroRecord;
    try (DataFileReader<GenericRecord> avro = new DataFileReader<>(avroFile, new GenericDatumReader<>(SCHEMA))) {
      avroRecord = avro.next();
    }

    GenericRecord parquetRecord;
    Configuration conf = new Configuration();
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
    AvroReadSupport.setAvroDataSupplier(conf, GenericDataSupplier.class);
    AvroReadSupport.setAvroReadSchema(conf, SCHEMA);
    try (ParquetReader<GenericRecord> parquet = AvroParquetReader.<GenericRecord>builder(parquetFile)
        .withConf(conf)
        .build()) {
      parquetRecord = parquet.read();
    }

    assertThat(avroRecord.get("default_class").getClass())
        .as("Avro default string class should be Utf8")
        .isEqualTo(Utf8.class);
    assertThat(parquetRecord.get("default_class").getClass())
        .as("Parquet default string class should be Utf8")
        .isEqualTo(Utf8.class);

    assertThat(avroRecord.get("string_class").getClass())
        .as("Avro avro.java.string=String class should be String")
        .isEqualTo(String.class);
    assertThat(parquetRecord.get("string_class").getClass())
        .as("Parquet avro.java.string=String class should be String")
        .isEqualTo(String.class);

    assertThat(avroRecord.get("stringable_class").getClass())
        .as("Avro stringable class should be Utf8")
        .isEqualTo(Utf8.class);
    assertThat(parquetRecord.get("stringable_class").getClass())
        .as("Parquet stringable class should be Utf8")
        .isEqualTo(Utf8.class);

    assertThat(keyClass(avroRecord.get("default_map")))
        .as("Avro map default string class should be Utf8")
        .isEqualTo(Utf8.class);
    assertThat(keyClass(parquetRecord.get("default_map")))
        .as("Parquet map default string class should be Utf8")
        .isEqualTo(Utf8.class);

    assertThat(keyClass(avroRecord.get("string_map")))
        .as("Avro map avro.java.string=String class should be String")
        .isEqualTo(String.class);
    assertThat(keyClass(parquetRecord.get("string_map")))
        .as("Parquet map avro.java.string=String class should be String")
        .isEqualTo(String.class);

    assertThat(keyClass(avroRecord.get("stringable_map")))
        .as("Avro map stringable class should be Utf8")
        .isEqualTo(Utf8.class);
    assertThat(keyClass(parquetRecord.get("stringable_map")))
        .as("Parquet map stringable class should be Utf8")
        .isEqualTo(Utf8.class);
  }

  @Test
  public void testSpecific() throws IOException {
    org.apache.parquet.avro.StringBehaviorTest avroRecord;
    try (DataFileReader<org.apache.parquet.avro.StringBehaviorTest> avro = new DataFileReader<>(
        avroFile, new SpecificDatumReader<>(org.apache.parquet.avro.StringBehaviorTest.getClassSchema()))) {
      avroRecord = avro.next();
    }

    org.apache.parquet.avro.StringBehaviorTest parquetRecord;
    try (ParquetReader<org.apache.parquet.avro.StringBehaviorTest> parquet =
        AvroParquetReader.<org.apache.parquet.avro.StringBehaviorTest>builder(parquetFile)
            .withDataModel(SpecificData.get())
            .withSerializableClasses(BigDecimal.class)
            .withCompatibility(false)
            .build()) {
      parquetRecord = parquet.read();
    }

    assertThat(avroRecord.getDefaultClass().getClass())
        .as("Avro default string class should be String")
        .isEqualTo(Utf8.class);
    assertThat(parquetRecord.getDefaultClass().getClass())
        .as("Parquet default string class should be String")
        .isEqualTo(Utf8.class);

    assertThat(avroRecord.getStringClass().getClass())
        .as("Avro avro.java.string=String class should be String")
        .isEqualTo(String.class);
    assertThat(parquetRecord.getStringClass().getClass())
        .as("Parquet avro.java.string=String class should be String")
        .isEqualTo(String.class);

    assertThat(avroRecord.getStringableClass().getClass())
        .as("Avro stringable class should be BigDecimal")
        .isEqualTo(BigDecimal.class);
    assertThat(parquetRecord.getStringableClass().getClass())
        .as("Parquet stringable class should be BigDecimal")
        .isEqualTo(BigDecimal.class);
    assertThat(parquetRecord.getStringableClass())
        .as("Should have the correct BigDecimal value")
        .isEqualTo(BIG_DECIMAL);

    assertThat(keyClass(avroRecord.getDefaultMap()))
        .as("Avro map default string class should be String")
        .isEqualTo(Utf8.class);
    assertThat(keyClass(parquetRecord.getDefaultMap()))
        .as("Parquet map default string class should be String")
        .isEqualTo(Utf8.class);

    assertThat(keyClass(avroRecord.getStringMap()))
        .as("Avro map avro.java.string=String class should be String")
        .isEqualTo(String.class);
    assertThat(keyClass(parquetRecord.getStringMap()))
        .as("Parquet map avro.java.string=String class should be String")
        .isEqualTo(String.class);

    assertThat(keyClass(avroRecord.getStringableMap()))
        .as("Avro map stringable class should be BigDecimal")
        .isEqualTo(BigDecimal.class);
    assertThat(keyClass(parquetRecord.getStringableMap()))
        .as("Parquet map stringable class should be BigDecimal")
        .isEqualTo(BigDecimal.class);
  }

  @Test
  public void testReflect() throws IOException {
    Schema reflectSchema = ReflectData.get().getSchema(ReflectRecord.class);

    ReflectRecord avroRecord;
    try (DataFileReader<ReflectRecord> avro =
        new DataFileReader<>(avroFile, new ReflectDatumReader<>(reflectSchema))) {
      avroRecord = avro.next();
    }

    ReflectRecord parquetRecord;
    Configuration conf = new Configuration();
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
    AvroReadSupport.setAvroDataSupplier(conf, ReflectDataSupplier.class);
    AvroReadSupport.setAvroReadSchema(conf, reflectSchema);
    AvroReadSupport.setSerializableClasses(conf, BigDecimal.class);
    try (ParquetReader<ReflectRecord> parquet = AvroParquetReader.<ReflectRecord>builder(parquetFile)
        .withConf(conf)
        .build()) {
      parquetRecord = parquet.read();
    }

    assertThat(avroRecord.default_class.getClass())
        .as("Avro default string class should be String")
        .isEqualTo(String.class);
    assertThat(parquetRecord.default_class.getClass())
        .as("Parquet default string class should be String")
        .isEqualTo(String.class);

    assertThat(avroRecord.string_class.getClass())
        .as("Avro avro.java.string=String class should be String")
        .isEqualTo(String.class);
    assertThat(parquetRecord.string_class.getClass())
        .as("Parquet avro.java.string=String class should be String")
        .isEqualTo(String.class);

    assertThat(avroRecord.stringable_class.getClass())
        .as("Avro stringable class should be BigDecimal")
        .isEqualTo(BigDecimal.class);
    assertThat(parquetRecord.stringable_class.getClass())
        .as("Parquet stringable class should be BigDecimal")
        .isEqualTo(BigDecimal.class);
    assertThat(parquetRecord.stringable_class)
        .as("Should have the correct BigDecimal value")
        .isEqualTo(BIG_DECIMAL);

    assertThat(keyClass(avroRecord.default_map))
        .as("Avro map default string class should be String")
        .isEqualTo(String.class);
    assertThat(keyClass(parquetRecord.default_map))
        .as("Parquet map default string class should be String")
        .isEqualTo(String.class);

    assertThat(keyClass(avroRecord.string_map))
        .as("Avro map avro.java.string=String class should be String")
        .isEqualTo(String.class);
    assertThat(keyClass(parquetRecord.string_map))
        .as("Parquet map avro.java.string=String class should be String")
        .isEqualTo(String.class);

    assertThat(keyClass(avroRecord.stringable_map))
        .as("Avro map stringable class should be BigDecimal")
        .isEqualTo(BigDecimal.class);
    assertThat(keyClass(parquetRecord.stringable_map))
        .as("Parquet map stringable class should be BigDecimal")
        .isEqualTo(BigDecimal.class);
  }

  @Test
  public void testReflectJavaClass() throws IOException {
    Schema reflectSchema = ReflectData.get().getSchema(ReflectRecordJavaClass.class);
    System.err.println("Schema: " + reflectSchema.toString(true));
    ReflectRecordJavaClass avroRecord;
    try (DataFileReader<ReflectRecordJavaClass> avro =
        new DataFileReader<>(avroFile, new ReflectDatumReader<>(reflectSchema))) {
      avroRecord = avro.next();
    }

    ReflectRecordJavaClass parquetRecord;
    Configuration conf = new Configuration();
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
    AvroReadSupport.setAvroDataSupplier(conf, ReflectDataSupplier.class);
    AvroReadSupport.setAvroReadSchema(conf, reflectSchema);
    AvroReadSupport.setRequestedProjection(conf, reflectSchema);
    AvroReadSupport.setSerializableClasses(conf, BigDecimal.class);
    try (ParquetReader<ReflectRecordJavaClass> parquet = AvroParquetReader.<ReflectRecordJavaClass>builder(
            parquetFile)
        .withConf(conf)
        .build()) {
      parquetRecord = parquet.read();
    }

    // Avro uses String even if CharSequence is set
    assertThat(avroRecord.default_class.getClass())
        .as("Avro default string class should be String")
        .isEqualTo(String.class);
    assertThat(parquetRecord.default_class.getClass())
        .as("Parquet default string class should be String")
        .isEqualTo(String.class);

    assertThat(avroRecord.stringable_class.getClass())
        .as("Avro stringable class should be BigDecimal")
        .isEqualTo(BigDecimal.class);
    assertThat(parquetRecord.stringable_class.getClass())
        .as("Parquet stringable class should be BigDecimal")
        .isEqualTo(BigDecimal.class);
    assertThat(parquetRecord.stringable_class)
        .as("Should have the correct BigDecimal value")
        .isEqualTo(BIG_DECIMAL);
  }

  @Test
  public void testSpecificValidationFail() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
    AvroReadSupport.setAvroDataSupplier(conf, SpecificDataSupplier.class);
    AvroReadSupport.setAvroReadSchema(conf, org.apache.parquet.avro.StringBehaviorTest.getClassSchema());
    try (ParquetReader<org.apache.parquet.avro.StringBehaviorTest> parquet =
        AvroParquetReader.<org.apache.parquet.avro.StringBehaviorTest>builder(parquetFile)
            .withConf(conf)
            .build()) {
      assertThatThrownBy(parquet::read)
          .isInstanceOf(SecurityException.class)
          .hasMessage(
              "Forbidden java.math.BigDecimal! This class is not trusted to be included in Avro schema "
                  + "using java-class or java-key-class. Please set the Parquet/Hadoop configuration "
                  + "parquet.avro.serializable.classes with the classes you trust.");
    }
  }

  @Test
  public void testReflectValidationFail() throws IOException {
    Schema reflectSchema = ReflectData.get().getSchema(ReflectRecord.class);

    Configuration conf = new Configuration();
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
    AvroReadSupport.setAvroDataSupplier(conf, ReflectDataSupplier.class);
    AvroReadSupport.setAvroReadSchema(conf, reflectSchema);
    try (ParquetReader<ReflectRecord> parquet = AvroParquetReader.<ReflectRecord>builder(parquetFile)
        .withConf(conf)
        .build()) {
      assertThatThrownBy(parquet::read)
          .isInstanceOf(SecurityException.class)
          .hasMessage(
              "Forbidden java.math.BigDecimal! This class is not trusted to be included in Avro schema "
                  + "using java-class or java-key-class. Please set the Parquet/Hadoop configuration "
                  + "parquet.avro.serializable.classes with the classes you trust.");
    }
  }

  @Test
  public void testReflectJavaClassValidationFail() throws IOException {
    Schema reflectSchema = ReflectData.get().getSchema(ReflectRecordJavaClass.class);

    Configuration conf = new Configuration();
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
    AvroReadSupport.setAvroDataSupplier(conf, ReflectDataSupplier.class);
    AvroReadSupport.setAvroReadSchema(conf, reflectSchema);
    AvroReadSupport.setRequestedProjection(conf, reflectSchema);
    try (ParquetReader<ReflectRecordJavaClass> parquet = AvroParquetReader.<ReflectRecordJavaClass>builder(
            parquetFile)
        .withConf(conf)
        .build()) {
      assertThatThrownBy(parquet::read)
          .isInstanceOf(SecurityException.class)
          .hasMessage(
              "Forbidden java.math.BigDecimal! This class is not trusted to be included in Avro schema "
                  + "using java-class or java-key-class. Please set the Parquet/Hadoop configuration "
                  + "parquet.avro.serializable.classes with the classes you trust.");
    }
  }

  public static class ReflectRecord {
    private String default_class;
    private String string_class;

    @Stringable
    private BigDecimal stringable_class;

    private Map<String, Integer> default_map;
    private Map<String, Integer> string_map;
    private Map<BigDecimal, Integer> stringable_map;
  }

  public static class ReflectRecordJavaClass {
    // test avro.java.string behavior
    @AvroSchema("{\"type\": \"string\", \"avro.java.string\": \"CharSequence\"}")
    private String default_class;
    // test using java-class property instead of Stringable
    @AvroSchema("{\"type\": \"string\", \"java-class\": \"java.math.BigDecimal\"}")
    private BigDecimal stringable_class;
  }

  public static Class<?> keyClass(Object obj) {
    assertThat(obj).as("Should be a map").isInstanceOf(Map.class);
    Map<?, ?> map = (Map<?, ?>) obj;
    return Iterables.getFirst(map.keySet(), null).getClass();
  }
}
