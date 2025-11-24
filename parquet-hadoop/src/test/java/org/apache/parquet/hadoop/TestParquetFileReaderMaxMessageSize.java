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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestParquetFileReaderMaxMessageSize {

  public static Path TEST_FILE;
  public MessageType schema;

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void testSetup() throws IOException {

    File testParquetFile = temp.newFile();
    testParquetFile.delete();

    TEST_FILE = new Path(testParquetFile.toURI());
    // Create a file with many columns
    StringBuilder schemaBuilder = new StringBuilder("message test_schema {");
    for (int i = 0; i < 2000; i++) {
      schemaBuilder.append("required int64 col_").append(i).append(";");
    }
    schemaBuilder.append("}");

    schema = MessageTypeParser.parseMessageType(schemaBuilder.toString());

    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(HadoopOutputFile.fromPath(TEST_FILE, conf))
        .withConf(conf)
        .withType(schema)
        .build()) {

      SimpleGroupFactory factory = new SimpleGroupFactory(schema);
      Group group = factory.newGroup();
      for (int col = 0; col < 2000; col++) {
        group.append("col_" + col, 1L);
      }
      writer.write(group);
    }
  }

  /**
   * Test reading a file with many columns using custom max message size
   */
  @Test
  public void testReadFileWithManyColumns() throws IOException {
    Configuration readConf = new Configuration();
    readConf.setInt("parquet.thrift.string.size.limit", 200 * 1024 * 1024);

    ParquetReadOptions options = HadoopReadOptions.builder(readConf).build();

    try (ParquetFileReader reader =
        ParquetFileReader.open(HadoopInputFile.fromPath(TEST_FILE, readConf), options)) {

      ParquetMetadata metadata = reader.getFooter();
      assertNotNull(metadata);
      assertEquals(schema, metadata.getFileMetaData().getSchema());
      assertTrue(metadata.getBlocks().size() > 0);
    }
  }

  /**
   * Test that default configuration works for normal files
   */
  @Test
  public void testReadNormalFileWithDefaultConfig() throws IOException {
    // Read with default configuration (no custom max message size)
    Configuration readConf = new Configuration();
    ParquetReadOptions options = HadoopReadOptions.builder(readConf).build();

    try (ParquetFileReader reader =
        ParquetFileReader.open(HadoopInputFile.fromPath(TEST_FILE, readConf), options)) {

      ParquetMetadata metadata = reader.getFooter();
      assertNotNull(metadata);
      assertEquals(1, metadata.getBlocks().get(0).getRowCount());
    }
  }

  /**
   * Test that insufficient max message size produces error
   */
  @Test
  public void testInsufficientMaxMessageSizeError() throws IOException {
    // Try to read with very small max message size
    Configuration readConf = new Configuration();
    readConf.setInt("parquet.thrift.string.size.limit", 1); // Only 1 byte

    ParquetReadOptions options = HadoopReadOptions.builder(readConf).build();

    try (ParquetFileReader reader =
        ParquetFileReader.open(HadoopInputFile.fromPath(TEST_FILE, readConf), options)) {
      fail("Should have thrown Message size exceeds limit due to MaxMessageSize");
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(
          "Error should mention TTransportException",
          e.getMessage().contains("Message size exceeds limit")
              || e.getCause().getMessage().contains("Message size exceeds limit")
              || e.getMessage().contains("MaxMessageSize reached")
              || e.getCause().getMessage().contains("MaxMessageSize reached"));
    }
  }
}
