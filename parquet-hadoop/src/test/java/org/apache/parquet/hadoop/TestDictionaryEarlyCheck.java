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

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Integration test verifying that the dictionary check threshold affects encoding in written Parquet files.
 */
public class TestDictionaryEarlyCheck {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private TrackingByteBufferAllocator allocator;

  @Before
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
  }

  @After
  public void closeAllocator() {
    allocator.close();
  }

  @Test
  public void testLargeThresholdPreservesDictionaryInFile() throws IOException {
    Configuration conf = new Configuration();
    MessageType schema = MessageTypeParser.parseMessageType("message test { required int32 val; }");
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);

    Path file = new Path(temp.getRoot().toString(), "large_threshold.parquet");

    ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(file.toString()))
        .withAllocator(allocator)
        .withCompressionCodec(UNCOMPRESSED)
        .withDictionaryPageSize(1024 * 1024)
        .enableDictionaryEncoding()
        .withDictionaryCheckThresholdRawSizeBytes(Long.MAX_VALUE)
        .withWriterVersion(org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0)
        .withConf(conf)
        .build();

    // Write 1000 distinct values — would normally trigger fallback with threshold=0
    for (int i = 0; i < 1000; i++) {
      writer.write(factory.newGroup().append("val", i));
    }
    writer.close();

    ParquetMetadata footer = readFooter(conf, file, NO_FILTER);
    for (BlockMetaData block : footer.getBlocks()) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        assertTrue(
            "Dictionary encoding should be preserved with large threshold, got: " + column.getEncodings(),
            column.getEncodings().contains(Encoding.PLAIN_DICTIONARY));
      }
    }
  }

  @Test
  public void testZeroThresholdFallsBackInFile() throws IOException {
    Configuration conf = new Configuration();
    MessageType schema = MessageTypeParser.parseMessageType("message test { required int32 val; }");
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);

    Path file = new Path(temp.getRoot().toString(), "zero_threshold.parquet");

    ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(file.toString()))
        .withAllocator(allocator)
        .withCompressionCodec(UNCOMPRESSED)
        .withDictionaryPageSize(1024 * 1024)
        .enableDictionaryEncoding()
        .withDictionaryCheckThresholdRawSizeBytes(0)
        .withWriterVersion(org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0)
        .withConf(conf)
        .build();

    for (int i = 0; i < 1000; i++) {
      writer.write(factory.newGroup().append("val", i));
    }
    writer.close();

    ParquetMetadata footer = readFooter(conf, file, NO_FILTER);
    for (BlockMetaData block : footer.getBlocks()) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        assertFalse(
            "Should fall back from dictionary with threshold=0, got: " + column.getEncodings(),
            column.getEncodings().contains(Encoding.PLAIN_DICTIONARY));
      }
    }
  }
}
