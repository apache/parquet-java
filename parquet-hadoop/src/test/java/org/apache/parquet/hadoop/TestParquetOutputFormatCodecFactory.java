/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.hadoop;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.ZSTD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopCodecs;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that the codec factory used to compress the pages written through {@link ParquetOutputFormat} can be
 * customized via {@link ParquetOutputFormat#getCodecFactory(Configuration, ParquetProperties)} (e.g. to use the
 * direct memory based factory created by {@link HadoopCodecs#newDirectFactory}).
 */
public class TestParquetOutputFormatCodecFactory {

  private static final MessageType SCHEMA =
      MessageTypeParser.parseMessageType("message test { required binary str; }");
  private static final int RECORD_COUNT = 100;
  private static final int PAGE_SIZE = 64 * 1024;

  @TempDir
  private java.nio.file.Path tempDir;

  /**
   * A factory delegating to a real one while tracking how many compressors have been requested and whether the
   * factory has been released.
   */
  static class TrackingCodecFactory implements CompressionCodecFactory {
    private final CompressionCodecFactory delegate;
    final AtomicInteger compressorRequests = new AtomicInteger();
    final AtomicBoolean released = new AtomicBoolean(false);

    TrackingCodecFactory(CompressionCodecFactory delegate) {
      this.delegate = delegate;
    }

    @Override
    public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
      compressorRequests.incrementAndGet();
      return delegate.getCompressor(codecName);
    }

    @Override
    public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
      return delegate.getDecompressor(codecName);
    }

    @Override
    public void release() {
      released.set(true);
      delegate.release();
    }
  }

  /**
   * A {@link ParquetOutputFormat} returning the given custom codec factory from
   * {@link #getCodecFactory(Configuration, ParquetProperties)}.
   */
  static class CustomCodecFactoryOutputFormat extends ParquetOutputFormat<Group> {
    private final CompressionCodecFactory codecFactory;

    CustomCodecFactoryOutputFormat(CompressionCodecFactory codecFactory) {
      super(new GroupWriteSupport());
      this.codecFactory = codecFactory;
    }

    @Override
    protected CompressionCodecFactory getCodecFactory(Configuration conf, ParquetProperties props) {
      return codecFactory;
    }
  }

  /**
   * A {@link CustomCodecFactoryOutputFormat} that overrides {@link #releaseCodecFactory} to a no-op, so the same
   * codec factory instance can be shared across every {@link #getRecordWriter} call and survives until the test
   * releases it explicitly.
   */
  static class SharedCodecFactoryOutputFormat extends CustomCodecFactoryOutputFormat {

    SharedCodecFactoryOutputFormat(CompressionCodecFactory codecFactory) {
      super(codecFactory);
    }

    @Override
    protected void releaseCodecFactory(
        Configuration conf, ParquetProperties props, CompressionCodecFactory codecFactory) {
      // shared across writers; released explicitly by the caller once no writer uses it anymore
    }
  }

  /**
   * A codec factory whose {@link #getCompressor} always fails, to exercise the record writer's
   * construction-failure cleanup path.
   */
  static class FailingCodecFactory implements CompressionCodecFactory {
    final AtomicBoolean released = new AtomicBoolean(false);

    @Override
    public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
      throw new RuntimeException("injected codec initialization failure");
    }

    @Override
    public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void release() {
      released.set(true);
    }
  }

  /**
   * A {@link ParquetOutputFormat} using the default codec factory while counting how many times
   * {@link #getCodecFactory} was invoked.
   */
  static class CountingCodecFactoryOutputFormat extends ParquetOutputFormat<Group> {
    final AtomicInteger codecFactoryAcquisitions = new AtomicInteger();

    CountingCodecFactoryOutputFormat() {
      super(new GroupWriteSupport());
    }

    @Override
    protected CompressionCodecFactory getCodecFactory(Configuration conf, ParquetProperties props) {
      codecFactoryAcquisitions.incrementAndGet();
      return super.getCodecFactory(conf, props);
    }
  }

  @Test
  public void testCustomCodecFactoryIsUsedAndReleased() throws Exception {
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(SCHEMA, conf);
    Path file = new Path(new Path(tempDir.toUri()), "custom-codec-factory.parquet");

    TrackingCodecFactory tracking = new TrackingCodecFactory(
        HadoopCodecs.newDirectFactory(conf, new DirectByteBufferAllocator(), PAGE_SIZE));
    ParquetOutputFormat<Group> outputFormat = new CustomCodecFactoryOutputFormat(tracking);

    writeRecords(outputFormat, conf, file, ZSTD);

    // the custom factory must have been used to compress the pages ...
    assertThat(tracking.compressorRequests.get()).isGreaterThan(0);
    // ... and must have been released when the record writer was closed
    assertThat(tracking.released.get()).isTrue();
    // the pages must actually be compressed with the requested codec
    assertThat(ParquetFileReader.readFooter(conf, file, NO_FILTER).getBlocks())
        .isNotEmpty()
        .allSatisfy(block -> assertThat(block.getColumns())
            .isNotEmpty()
            .allSatisfy(column -> assertThat(column.getCodec()).isEqualTo(ZSTD)));
    // and the written data must be intact
    assertThat(readRecords(file, conf)).isEqualTo(expectedValues());
  }

  @Test
  public void testCodecFactoryReleasedWhenRecordWriterConstructionFails() {
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(SCHEMA, conf);
    Path file = new Path(new Path(tempDir.toUri()), "failing-codec-factory.parquet");

    FailingCodecFactory failing = new FailingCodecFactory();
    ParquetOutputFormat<Group> outputFormat = new CustomCodecFactoryOutputFormat(failing);

    // the failure from the injected codec factory must propagate ...
    assertThatThrownBy(() -> outputFormat.getRecordWriter(conf, file, ZSTD, ParquetFileWriter.Mode.CREATE))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("injected codec initialization failure");
    // ... and the factory must still be released, even though the record writer was never fully constructed
    assertThat(failing.released.get()).isTrue();
  }

  @Test
  public void testCodecFactoryNotAcquiredWhenFileWriterConstructionFails() throws Exception {
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(SCHEMA, conf);
    Path file = new Path(new Path(tempDir.toUri()), "existing-file-collision.parquet");

    CountingCodecFactoryOutputFormat outputFormat = new CountingCodecFactoryOutputFormat();

    // the first write succeeds and creates the file, acquiring exactly one codec factory
    writeRecords(outputFormat, conf, file, ZSTD);
    assertThat(outputFormat.codecFactoryAcquisitions.get()).isEqualTo(1);

    // a second Mode.CREATE call against the same path fails while constructing the ParquetFileWriter, before a
    // codec factory is ever acquired for it, so there is nothing left dangling to release
    assertThatThrownBy(() -> outputFormat.getRecordWriter(conf, file, ZSTD, ParquetFileWriter.Mode.CREATE))
        .isInstanceOf(IOException.class);
    assertThat(outputFormat.codecFactoryAcquisitions.get()).isEqualTo(1);
  }

  @Test
  public void testSharedCodecFactorySurvivesWriterClose() throws Exception {
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(SCHEMA, conf);
    Path file1 = new Path(new Path(tempDir.toUri()), "shared-codec-factory-1.parquet");
    Path file2 = new Path(new Path(tempDir.toUri()), "shared-codec-factory-2.parquet");

    TrackingCodecFactory shared = new TrackingCodecFactory(
        HadoopCodecs.newDirectFactory(conf, new DirectByteBufferAllocator(), PAGE_SIZE));
    ParquetOutputFormat<Group> outputFormat = new SharedCodecFactoryOutputFormat(shared);

    writeRecords(outputFormat, conf, file1, ZSTD);
    // the shared factory must not be released just because one writer using it has closed ...
    assertThat(shared.released.get()).isFalse();

    // ... so a second writer sharing the same factory instance can still use it afterwards
    writeRecords(outputFormat, conf, file2, ZSTD);
    assertThat(readRecords(file2, conf)).isEqualTo(expectedValues());
    assertThat(shared.released.get()).isFalse();

    shared.release();
    assertThat(shared.released.get()).isTrue();
  }

  private void writeRecords(
      ParquetOutputFormat<Group> outputFormat, Configuration conf, Path file, CompressionCodecName codec)
      throws IOException, InterruptedException {
    SimpleGroupFactory groupFactory = new SimpleGroupFactory(SCHEMA);
    RecordWriter<Void, Group> writer =
        outputFormat.getRecordWriter(conf, file, codec, ParquetFileWriter.Mode.CREATE);
    try {
      for (int i = 0; i < RECORD_COUNT; i++) {
        writer.write(null, groupFactory.newGroup().append("str", "value-" + i));
      }
    } finally {
      writer.close(null);
    }
  }

  private List<String> readRecords(Path file, Configuration conf) throws IOException {
    List<String> values = new ArrayList<>();
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
        .withConf(conf)
        .build()) {
      Group group;
      while ((group = reader.read()) != null) {
        values.add(group.getBinary("str", 0).toStringUsingUTF8());
      }
    }
    return values;
  }

  private List<String> expectedValues() {
    List<String> values = new ArrayList<>();
    for (int i = 0; i < RECORD_COUNT; i++) {
      values.add("value-" + i);
    }
    return values;
  }
}
