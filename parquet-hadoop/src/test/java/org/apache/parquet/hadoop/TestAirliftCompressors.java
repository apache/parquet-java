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

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestAirliftCompressors {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testParquetReadWriteAirliftCompressorOption() throws Exception {
    Configuration conf = new Configuration();
    File file = temp.newFile();
    file.delete();
    Path path = new Path(file.getAbsolutePath());
    MessageType schema = Types.buildMessage().
      required(BINARY).as(stringType()).named("name").named("msg");
    GroupWriteSupport.setSchema(schema, conf);

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
      .withPageRowCountLimit(10)
      .withConf(conf)
      .withDictionaryEncoding(false)
      .withAirliftCompressorsEnabled(true)
      .withCompressionCodec(CompressionCodecName.GZIP)
      .build()) {
      assertTrue(
        "AirliftCompressor instance should have been used",
        writer.getCodecFactory().getCompressor(CompressionCodecName.GZIP) instanceof AirliftCompressor);
    }

    file = temp.newFile();
    file.delete();
    path = new Path(file.getAbsolutePath());
    // verify that the default read option is to not use airlift compressors
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
      .withPageRowCountLimit(10)
      .withConf(conf)
      .withDictionaryEncoding(false)
      .withCompressionCodec(CompressionCodecName.GZIP)
      .build()) {
      assertFalse(
        "Non-airlift compressor instance should have been used",
        writer.getCodecFactory().getCompressor(CompressionCodecName.GZIP) instanceof AirliftCompressor);
    }

    try (ParquetReader<Group> reader =
      ParquetReader.builder(new GroupReadSupport(), path).withConf(conf).useAirliftCompressors(true).build()) {
      assertTrue("Use airlift compressors should have been true", reader.readOptions().useAirliftCompressors());
      assertTrue(
        "AirliftCompressor instance should have been used",
        reader.readOptions()
          .getCodecFactory()
          .getDecompressor(CompressionCodecName.GZIP) instanceof AirliftDecompressor);
    }

    // verify that the default read option is to not use airlift compressors
    try (ParquetReader<Group> reader =
      ParquetReader.builder(new GroupReadSupport(), path).withConf(conf).build()) {
      assertFalse("Use airlift compressors should have been false", reader.readOptions().useAirliftCompressors());
      assertFalse(
        "Non-airlift decompressor instance should have been used",
        reader.readOptions()
          .getCodecFactory()
          .getDecompressor(CompressionCodecName.GZIP) instanceof AirliftDecompressor);
    }
  }

  @Test
  public void testWriteReadCompatibilityForAirliftAndRegularCompressors() throws Exception {
    boolean[][] writeReadModes = new boolean[][] {
      // Write Airlift , Read non-airlift
      {true, true},
      {true, false},
      {false, true}
    };
    CompressionCodecName[] codecs = {CompressionCodecName.GZIP, CompressionCodecName.LZ4, CompressionCodecName.LZO};
    for (CompressionCodecName codec : codecs) {
      for (boolean[] writeReadMode : writeReadModes) {
        if ((writeReadMode[1] == false || writeReadMode[0] == false) &&
          (codec.equals(CompressionCodecName.LZ4) || codec.equals(CompressionCodecName.LZO))) {
          // TODO: Add tests for non-airlift LZ4 and LZ0.
          // For some reason reads and writes are running into codec class not found exceptions
          continue;
        }
        testWriteReadCompatibilityForAirliftAndRegularCompressors(codec, writeReadMode[0], writeReadMode[1]);
      }
    }
  }

  private void testWriteReadCompatibilityForAirliftAndRegularCompressors(
    CompressionCodecName codecName,
    boolean writeAirlift,
    boolean readAirlift) throws Exception {
    MessageType schema = Types.buildMessage().
      required(BINARY).as(stringType()).named("name").named("msg");
    String[] testNames = {"airlift", "parquet"};
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
      .withAirliftCompressorsEnabled(writeAirlift)
      .withCompressionCodec(codecName)
      .build()) {
      for (String testName : testNames) {
        writer.write(factory.newGroup().append("name", testName));
      }
    }

    try (ParquetReader<Group> reader =
      ParquetReader.builder(new GroupReadSupport(), path).withConf(conf).useAirliftCompressors(readAirlift).build()) {
      Group group = reader.read();
      assertEquals("airlift", group.getBinary("name", 0).toStringUsingUTF8());
      group = reader.read();
      assertEquals("parquet", group.getBinary("name", 0).toStringUsingUTF8());
      assertNull("reader should have returned null since only two values were written to the file", reader.read());
    }
  }
}
