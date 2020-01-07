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
package org.apache.parquet.hadoop;

import static java.util.Arrays.asList;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.apache.parquet.column.Encoding.DELTA_BYTE_ARRAY;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.TestUtils.enforceEmptyDir;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import net.openhft.hashing.LongHashFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.junit.rules.TemporaryFolder;

public class TestParquetWriter {

  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();
    Path root = new Path("target/tests/TestParquetWriter/");
    enforceEmptyDir(conf, root);
    MessageType schema = parseMessageType(
        "message test { "
        + "required binary binary_field; "
        + "required int32 int32_field; "
        + "required int64 int64_field; "
        + "required boolean boolean_field; "
        + "required float float_field; "
        + "required double double_field; "
        + "required fixed_len_byte_array(3) flba_field; "
        + "required int96 int96_field; "
        + "} ");
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    Map<String, Encoding> expected = new HashMap<String, Encoding>();
    expected.put("10-" + PARQUET_1_0, PLAIN_DICTIONARY);
    expected.put("1000-" + PARQUET_1_0, PLAIN);
    expected.put("10-" + PARQUET_2_0, RLE_DICTIONARY);
    expected.put("1000-" + PARQUET_2_0, DELTA_BYTE_ARRAY);
    for (int modulo : asList(10, 1000)) {
      for (WriterVersion version : WriterVersion.values()) {
        Path file = new Path(root, version.name() + "_" + modulo);
        ParquetWriter<Group> writer = new ParquetWriter<Group>(
            file,
            new GroupWriteSupport(),
            UNCOMPRESSED, 1024, 1024, 512, true, false, version, conf);
        for (int i = 0; i < 1000; i++) {
          writer.write(
              f.newGroup()
              .append("binary_field", "test" + (i % modulo))
              .append("int32_field", 32)
              .append("int64_field", 64l)
              .append("boolean_field", true)
              .append("float_field", 1.0f)
              .append("double_field", 2.0d)
              .append("flba_field", "foo")
              .append("int96_field", Binary.fromConstantByteArray(new byte[12])));
        }
        writer.close();
        ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();
        for (int i = 0; i < 1000; i++) {
          Group group = reader.read();
          assertEquals("test" + (i % modulo), group.getBinary("binary_field", 0).toStringUsingUTF8());
          assertEquals(32, group.getInteger("int32_field", 0));
          assertEquals(64l, group.getLong("int64_field", 0));
          assertEquals(true, group.getBoolean("boolean_field", 0));
          assertEquals(1.0f, group.getFloat("float_field", 0), 0.001);
          assertEquals(2.0d, group.getDouble("double_field", 0), 0.001);
          assertEquals("foo", group.getBinary("flba_field", 0).toStringUsingUTF8());
          assertEquals(Binary.fromConstantByteArray(new byte[12]),
              group.getInt96("int96_field",0));
        }
        reader.close();
        ParquetMetadata footer = readFooter(conf, file, NO_FILTER);
        for (BlockMetaData blockMetaData : footer.getBlocks()) {
          for (ColumnChunkMetaData column : blockMetaData.getColumns()) {
            if (column.getPath().toDotString().equals("binary_field")) {
              String key = modulo + "-" + version;
              Encoding expectedEncoding = expected.get(key);
              assertTrue(
                  key + ":" + column.getEncodings() + " should contain " + expectedEncoding,
                  column.getEncodings().contains(expectedEncoding));
            }
          }
        }
        assertEquals("Object model property should be example",
            "example", footer.getFileMetaData().getKeyValueMetaData()
                .get(ParquetWriter.OBJECT_MODEL_NAME_PROP));
      }
    }
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testBadWriteSchema() throws IOException {
    final File file = temp.newFile("test.parquet");
    file.delete();

    TestUtils.assertThrows("Should reject a schema with an empty group",
        InvalidSchemaException.class, new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            ExampleParquetWriter.builder(new Path(file.toString()))
                .withType(Types.buildMessage()
                    .addField(new GroupType(REQUIRED, "invalid_group"))
                    .named("invalid_message"))
                .build();
            return null;
          }
        });

    Assert.assertFalse("Should not create a file when schema is rejected",
        file.exists());
  }

  // Testing the issue of PARQUET-1531 where writing null nested rows leads to empty pages if the page row count limit
  // is reached.
  @Test
  public void testNullValuesWithPageRowLimit() throws IOException {
    MessageType schema = Types.buildMessage().optionalList().optionalElement(BINARY).as(stringType()).named("str_list")
        .named("msg");
    final int recordCount = 100;
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    GroupFactory factory = new SimpleGroupFactory(schema);
    Group listNull = factory.newGroup();

    File file = temp.newFile();
    file.delete();
    Path path = new Path(file.getAbsolutePath());
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withPageRowCountLimit(10)
        .withConf(conf)
        .build()) {
      for (int i = 0; i < recordCount; ++i) {
        writer.write(listNull);
      }
    }

    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).build()) {
      int readRecordCount = 0;
      for (Group group = reader.read(); group != null; group = reader.read()) {
        assertEquals(listNull.toString(), group.toString());
        ++readRecordCount;
      }
      assertEquals("Number of written records should be equal to the read one", recordCount, readRecordCount);
    }
  }

  @Test
  public void testParquetFileWithBloomFilter() throws IOException {
    MessageType schema = Types.buildMessage().
      required(BINARY).as(stringType()).named("name").named("msg");

    String[] testNames = {"hello", "parquet", "bloom", "filter"};

    final int recordCount = testNames.length;
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    GroupFactory factory = new SimpleGroupFactory(schema);
    File file = temp.newFile();
    file.delete();
    Path path = new Path(file.getAbsolutePath());
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
      .withPageRowCountLimit(10)
      .withConf(conf)
      .withDictionaryEncoding(false)
      .withBloomFilterColumnNames("name")
      .build()) {
      for (String testName : testNames) {
        writer.write(factory.newGroup().append("name", testName));
      }
    }

    ParquetMetadata footer = readFooter(conf, path, NO_FILTER);
    ParquetFileReader reader = new ParquetFileReader(
      conf, footer.getFileMetaData(), path, footer.getBlocks(), schema.getColumns());

    BloomFilter bloomFilter = reader.getBloomFilterDataReader(footer.getBlocks().get(0))
      .readBloomFilter(footer.getBlocks().get(0).getColumns().get(0));

    for (String name: testNames) {
      assertTrue(bloomFilter.findHash(
        LongHashFunction.xx(0).hashBytes(Binary.fromString(name).toByteBuffer())));
    }
  }
}
