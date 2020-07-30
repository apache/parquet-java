/**
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
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
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
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

public class TestStringBehavior {
  public static Schema SCHEMA = null;
  public static BigDecimal BIG_DECIMAL = new BigDecimal("3.14");

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public Path parquetFile;
  public File avroFile;

  @BeforeClass
  public static void readSchemaFile() throws IOException {
    TestStringBehavior.SCHEMA = new Schema.Parser().parse(
        Resources.getResource("stringBehavior.avsc").openStream());
  }

  @Before
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

    File file = temp.newFile("parquet");
    file.delete();
    file.deleteOnExit();

    parquetFile = new Path(file.getPath());
    try(ParquetWriter<GenericRecord> parquet = AvroParquetWriter
        .<GenericRecord>builder(parquetFile)
        .withDataModel(GenericData.get())
        .withSchema(SCHEMA)
        .build()) {
      parquet.write(record);
    }

    avroFile = temp.newFile("avro");
    avroFile.delete();
    avroFile.deleteOnExit();
    try(DataFileWriter<GenericRecord> avro = new DataFileWriter<GenericRecord>(
      new GenericDatumWriter<>(SCHEMA)).create(SCHEMA, avroFile)) {
      avro.append(record);
    }
  }

  @Test
  public void testGeneric() throws IOException {
    GenericRecord avroRecord;
    try(DataFileReader<GenericRecord> avro = new DataFileReader<>(
      avroFile, new GenericDatumReader<>(SCHEMA))) {
      avroRecord = avro.next();
    }

    GenericRecord parquetRecord;
    Configuration conf = new Configuration();
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
    AvroReadSupport.setAvroDataSupplier(conf, GenericDataSupplier.class);
    AvroReadSupport.setAvroReadSchema(conf, SCHEMA);
    try(ParquetReader<GenericRecord> parquet = AvroParquetReader
        .<GenericRecord>builder(parquetFile)
        .withConf(conf)
        .build()) {
      parquetRecord = parquet.read();
    }

    Assert.assertEquals("Avro default string class should be Utf8",
        Utf8.class, avroRecord.get("default_class").getClass());
    Assert.assertEquals("Parquet default string class should be Utf8",
        Utf8.class, parquetRecord.get("default_class").getClass());

    Assert.assertEquals("Avro avro.java.string=String class should be String",
        String.class, avroRecord.get("string_class").getClass());
    Assert.assertEquals("Parquet avro.java.string=String class should be String",
        String.class, parquetRecord.get("string_class").getClass());

    Assert.assertEquals("Avro stringable class should be Utf8",
        Utf8.class, avroRecord.get("stringable_class").getClass());
    Assert.assertEquals("Parquet stringable class should be Utf8",
        Utf8.class, parquetRecord.get("stringable_class").getClass());

    Assert.assertEquals("Avro map default string class should be Utf8",
        Utf8.class, keyClass(avroRecord.get("default_map")));
    Assert.assertEquals("Parquet map default string class should be Utf8",
        Utf8.class, keyClass(parquetRecord.get("default_map")));

    Assert.assertEquals("Avro map avro.java.string=String class should be String",
        String.class, keyClass(avroRecord.get("string_map")));
    Assert.assertEquals("Parquet map avro.java.string=String class should be String",
        String.class, keyClass(parquetRecord.get("string_map")));

    Assert.assertEquals("Avro map stringable class should be Utf8",
        Utf8.class, keyClass(avroRecord.get("stringable_map")));
    Assert.assertEquals("Parquet map stringable class should be Utf8",
        Utf8.class, keyClass(parquetRecord.get("stringable_map")));
  }


  @Test
  public void testSpecific() throws IOException {
    org.apache.parquet.avro.StringBehaviorTest avroRecord;
    try(DataFileReader<org.apache.parquet.avro.StringBehaviorTest> avro =
          new DataFileReader<>(avroFile, new SpecificDatumReader<>(
            org.apache.parquet.avro.StringBehaviorTest.getClassSchema()))) {
      avroRecord = avro.next();
    }

    org.apache.parquet.avro.StringBehaviorTest parquetRecord;
    Configuration conf = new Configuration();
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
    AvroReadSupport.setAvroDataSupplier(conf, SpecificDataSupplier.class);
    AvroReadSupport.setAvroReadSchema(conf,
        org.apache.parquet.avro.StringBehaviorTest.getClassSchema());
    try(ParquetReader<org.apache.parquet.avro.StringBehaviorTest> parquet =
        AvroParquetReader
            .<org.apache.parquet.avro.StringBehaviorTest>builder(parquetFile)
            .withConf(conf)
            .build()) {
      parquetRecord = parquet.read();
    }

    Assert.assertEquals("Avro default string class should be String",
        Utf8.class, avroRecord.getDefaultClass().getClass());
    Assert.assertEquals("Parquet default string class should be String",
        Utf8.class, parquetRecord.getDefaultClass().getClass());

    Assert.assertEquals("Avro avro.java.string=String class should be String",
        String.class, avroRecord.getStringClass().getClass());
    Assert.assertEquals("Parquet avro.java.string=String class should be String",
        String.class, parquetRecord.getStringClass().getClass());

    Assert.assertEquals("Avro stringable class should be BigDecimal",
        BigDecimal.class, avroRecord.getStringableClass().getClass());
    Assert.assertEquals("Parquet stringable class should be BigDecimal",
        BigDecimal.class, parquetRecord.getStringableClass().getClass());
    Assert.assertEquals("Should have the correct BigDecimal value",
        BIG_DECIMAL, parquetRecord.getStringableClass());

    Assert.assertEquals("Avro map default string class should be String",
        Utf8.class, keyClass(avroRecord.getDefaultMap()));
    Assert.assertEquals("Parquet map default string class should be String",
        Utf8.class, keyClass(parquetRecord.getDefaultMap()));

    Assert.assertEquals("Avro map avro.java.string=String class should be String",
        String.class, keyClass(avroRecord.getStringMap()));
    Assert.assertEquals("Parquet map avro.java.string=String class should be String",
        String.class, keyClass(parquetRecord.getStringMap()));

    Assert.assertEquals("Avro map stringable class should be BigDecimal",
        BigDecimal.class, keyClass(avroRecord.getStringableMap()));
    Assert.assertEquals("Parquet map stringable class should be BigDecimal",
        BigDecimal.class, keyClass(parquetRecord.getStringableMap()));
  }

  @Test
  public void testReflect() throws IOException {
    Schema reflectSchema = ReflectData.get()
        .getSchema(ReflectRecord.class);

    ReflectRecord avroRecord;
    try(DataFileReader<ReflectRecord> avro = new DataFileReader<>(
      avroFile, new ReflectDatumReader<>(reflectSchema))) {
      avroRecord = avro.next();
    }

    ReflectRecord parquetRecord;
    Configuration conf = new Configuration();
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
    AvroReadSupport.setAvroDataSupplier(conf, ReflectDataSupplier.class);
    AvroReadSupport.setAvroReadSchema(conf, reflectSchema);
    try(ParquetReader<ReflectRecord> parquet = AvroParquetReader
        .<ReflectRecord>builder(parquetFile)
        .withConf(conf)
        .build()) {
      parquetRecord = parquet.read();
    }

    Assert.assertEquals("Avro default string class should be String",
        String.class, avroRecord.default_class.getClass());
    Assert.assertEquals("Parquet default string class should be String",
        String.class, parquetRecord.default_class.getClass());

    Assert.assertEquals("Avro avro.java.string=String class should be String",
        String.class, avroRecord.string_class.getClass());
    Assert.assertEquals("Parquet avro.java.string=String class should be String",
        String.class, parquetRecord.string_class.getClass());

    Assert.assertEquals("Avro stringable class should be BigDecimal",
        BigDecimal.class, avroRecord.stringable_class.getClass());
    Assert.assertEquals("Parquet stringable class should be BigDecimal",
        BigDecimal.class, parquetRecord.stringable_class.getClass());
    Assert.assertEquals("Should have the correct BigDecimal value",
        BIG_DECIMAL, parquetRecord.stringable_class);

    Assert.assertEquals("Avro map default string class should be String",
        String.class, keyClass(avroRecord.default_map));
    Assert.assertEquals("Parquet map default string class should be String",
        String.class, keyClass(parquetRecord.default_map));

    Assert.assertEquals("Avro map avro.java.string=String class should be String",
        String.class, keyClass(avroRecord.string_map));
    Assert.assertEquals("Parquet map avro.java.string=String class should be String",
        String.class, keyClass(parquetRecord.string_map));

    Assert.assertEquals("Avro map stringable class should be BigDecimal",
        BigDecimal.class, keyClass(avroRecord.stringable_map));
    Assert.assertEquals("Parquet map stringable class should be BigDecimal",
        BigDecimal.class, keyClass(parquetRecord.stringable_map));
  }

  @Test
  public void testReflectJavaClass() throws IOException {
    Schema reflectSchema = ReflectData.get()
        .getSchema(ReflectRecordJavaClass.class);
    System.err.println("Schema: " + reflectSchema.toString(true));
    ReflectRecordJavaClass avroRecord;
    try(DataFileReader<ReflectRecordJavaClass> avro =
          new DataFileReader<>(avroFile,
            new ReflectDatumReader<>(reflectSchema))) {
      avroRecord = avro.next();
    }

    ReflectRecordJavaClass parquetRecord;
    Configuration conf = new Configuration();
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
    AvroReadSupport.setAvroDataSupplier(conf, ReflectDataSupplier.class);
    AvroReadSupport.setAvroReadSchema(conf, reflectSchema);
    AvroReadSupport.setRequestedProjection(conf, reflectSchema);
    try(ParquetReader<ReflectRecordJavaClass> parquet = AvroParquetReader
        .<ReflectRecordJavaClass>builder(parquetFile)
        .withConf(conf)
        .build()) {
      parquetRecord = parquet.read();
    }

    // Avro uses String even if CharSequence is set
    Assert.assertEquals("Avro default string class should be String",
        String.class, avroRecord.default_class.getClass());
    Assert.assertEquals("Parquet default string class should be String",
        String.class, parquetRecord.default_class.getClass());

    Assert.assertEquals("Avro stringable class should be BigDecimal",
        BigDecimal.class, avroRecord.stringable_class.getClass());
    Assert.assertEquals("Parquet stringable class should be BigDecimal",
        BigDecimal.class, parquetRecord.stringable_class.getClass());
    Assert.assertEquals("Should have the correct BigDecimal value",
        BIG_DECIMAL, parquetRecord.stringable_class);
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
    Assert.assertTrue("Should be a map", obj instanceof Map);
    Map<?, ?> map = (Map<?, ?>) obj;
    return Iterables.getFirst(map.keySet(), null).getClass();
  }
}
