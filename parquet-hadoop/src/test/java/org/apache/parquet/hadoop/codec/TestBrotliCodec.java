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

package org.apache.parquet.hadoop.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.aayushatharva.brotli4j.encoder.BrotliOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

public class TestBrotliCodec {

  @Test
  public void testRoundTrip() throws Exception {
    byte[] input = new byte[64 * 1024];
    new Random(42).nextBytes(input);

    BrotliCodec codec = newCodec(5);
    try (CompressionInputStream in = codec.createInputStream(compress(codec, input))) {
      assertThat(in.readAllBytes()).isEqualTo(input);
    }
  }

  @Test
  public void testLargeStreamCrossingEncoderWindow() throws Exception {
    // brotli4j's streaming decoder fails once the decompressed output crosses the 4 MiB encoder
    // window (hyperxpro/Brotli4j#267). Parquet pages can be larger than that, so keep a regression
    // test for the one-shot decoder path.
    byte[] input = largeCompressibleInput();
    BrotliCodec codec = newCodec(5);

    try (CompressionInputStream in = codec.createInputStream(compress(codec, input))) {
      assertThat(in.readAllBytes()).isEqualTo(input);
    }
  }

  @Test
  public void testLargeStreamThroughCodecFactory() throws Exception {
    byte[] input = largeCompressibleInput();
    CodecFactory factory = new CodecFactory(new Configuration(false), 64 * 1024);
    try {
      BytesInput compressed =
          factory.getCompressor(CompressionCodecName.BROTLI, 5).compress(BytesInput.from(input));
      BytesInput decompressed =
          factory.getDecompressor(CompressionCodecName.BROTLI).decompress(compressed, input.length);
      assertThat(decompressed.toByteArray()).isEqualTo(input);
    } finally {
      factory.release();
    }
  }

  @Test
  public void testCloseClosesTarget() throws Exception {
    TrackingOutputStream target = new TrackingOutputStream();
    BrotliCodec codec = newCodec(5);
    try (CompressionOutputStream out = codec.createOutputStream(target)) {
      out.write(new byte[] {1, 2, 3});
    }
    assertThat(target.closed).isTrue();
  }

  @Test
  public void testFlushThenFinishRoundTrips() throws Exception {
    byte[] input = new byte[1024 * 1024];
    new Random(11).nextBytes(input);
    BrotliCodec codec = newCodec(5);
    TrackingOutputStream target = new TrackingOutputStream();

    try (CompressionOutputStream out = codec.createOutputStream(target)) {
      out.write(input, 0, input.length / 2);
      out.flush();
      assertThat(target.flushes).isGreaterThan(0);
      assertThat(target.size()).isGreaterThan(0);

      out.write(input, input.length / 2, input.length / 2);
      out.finish();
    }

    try (CompressionInputStream in = codec.createInputStream(new ByteArrayInputStream(target.toByteArray()))) {
      assertThat(in.readAllBytes()).isEqualTo(input);
    }
  }

  @Test
  public void testBoundedDecompressionRejectsBomb() throws Exception {
    byte[] zeros = new byte[16 * 1024 * 1024];
    byte[] compressed = compressBytes(newCodec(5), zeros);

    assertThatThrownBy(() -> newCodec(5).createInputStream(new ByteArrayInputStream(compressed), 1024))
        .isInstanceOf(IOException.class)
        .hasMessage("Failed to decompress Brotli stream")
        .hasStackTraceContaining("exceeds maximum size");

    CodecFactory factory = new CodecFactory(new Configuration(false), 64 * 1024);
    try {
      assertThatThrownBy(() -> factory.getDecompressor(CompressionCodecName.BROTLI)
              .decompress(BytesInput.from(compressed), 1024))
          .isInstanceOf(IOException.class)
          .hasMessage("Failed to decompress Brotli stream")
          .hasStackTraceContaining("exceeds maximum size");
    } finally {
      factory.release();
    }
  }

  @Test
  public void testInvalidQualityOnlyRejectsCompression() throws Exception {
    byte[] input = new byte[1024];
    new Random(13).nextBytes(input);
    byte[] compressed = compressBytes(newCodec(5), input);

    BrotliCodec codec = newCodec(99);
    try (CompressionInputStream in = codec.createInputStream(new ByteArrayInputStream(compressed), input.length)) {
      assertThat(in.readAllBytes()).isEqualTo(input);
    }

    assertThatThrownBy(() -> codec.createOutputStream(new ByteArrayOutputStream()))
        .isInstanceOf(BadConfigurationException.class)
        .hasMessage("Invalid Brotli compression quality: 99. Expected 0..11.");
  }

  @Test
  public void testEmptyAndZeroSizeStreams() throws Exception {
    byte[] compressed = compressBytes(newCodec(5), new byte[0]);
    try (CompressionInputStream in = newCodec(5).createInputStream(new ByteArrayInputStream(compressed), 0)) {
      assertThat(in.readAllBytes()).isEmpty();
    }
  }

  @Test
  public void testZeroSizeRejectsOneByteOutput() throws Exception {
    byte[] compressed = compressBytes(newCodec(5), new byte[] {1});

    assertThatThrownBy(() -> newCodec(5).createInputStream(new ByteArrayInputStream(compressed), 0))
        .isInstanceOf(IOException.class)
        .hasMessage("Failed to decompress Brotli stream")
        .hasRootCauseMessage("Brotli decompressed size 1 exceeds maximum size 0");

    CodecFactory factory = new CodecFactory(new Configuration(false), 64 * 1024);
    try {
      assertThatThrownBy(() -> factory.getDecompressor(CompressionCodecName.BROTLI)
              .decompress(BytesInput.from(compressed), 0))
          .isInstanceOf(IOException.class)
          .hasMessage("Failed to decompress Brotli stream")
          .hasRootCauseMessage("Brotli decompressed size 1 exceeds maximum size 0");
    } finally {
      factory.release();
    }
  }

  @Test
  public void testNegativeSizeIsRejectedForEmptyAndNonEmptyInput() throws Exception {
    byte[][] inputs = {new byte[0], compressBytes(newCodec(5), new byte[] {1})};
    for (byte[] input : inputs) {
      assertThatThrownBy(() -> newCodec(5).createInputStream(new ByteArrayInputStream(input), -1))
          .isInstanceOf(IOException.class)
          .hasMessage("Invalid Brotli maximum output size: -1. Expected a non-negative size.");
    }
  }

  @Test
  public void testCloseAfterFlushFailureClosesEncoderAndTarget() throws Exception {
    FailingOutputStream target = new FailingOutputStream();
    CompressionOutputStream out = newCodec(5).createOutputStream(target);
    BrotliOutputStream encoder = encoderOf(out);
    out.write(new byte[] {1, 2, 3});
    target.failOnFlush = true;

    assertThatThrownBy(out::flush).isInstanceOf(IOException.class).hasMessage("flush failed");
    assertThatThrownBy(out::close).isInstanceOf(IOException.class).hasMessageContaining("failed");
    assertThatThrownBy(encoder::flush).isInstanceOf(IOException.class).hasMessage("write after close");
    out.close();
    assertThat(target.closes).isEqualTo(1);
  }

  @Test
  public void testCloseAfterWriteFailureClosesEncoderAndTarget() throws Exception {
    FailingOutputStream target = new FailingOutputStream();
    CompressionOutputStream out = newCodec(1).createOutputStream(target);
    BrotliOutputStream encoder = encoderOf(out);
    byte[] input = new byte[1024 * 1024];
    new Random(17).nextBytes(input);
    target.failOnWrite = true;

    assertThatThrownBy(() -> out.write(input))
        .isInstanceOf(IOException.class)
        .hasMessage("write failed");
    assertThatThrownBy(out::close).isInstanceOf(IOException.class).hasMessageContaining("failed");
    assertThatThrownBy(encoder::flush).isInstanceOf(IOException.class).hasMessage("write after close");
    out.close();
    assertThat(target.closes).isEqualTo(1);
  }

  @Test
  public void testClosePreservesFailureWhenCleanupAlsoFails() throws Exception {
    FailingOutputStream target = new FailingOutputStream();
    CompressionOutputStream out = newCodec(5).createOutputStream(target);
    out.write(new byte[] {1, 2, 3});
    target.failOnFlush = true;
    target.failOnClose = true;

    assertThatThrownBy(out::flush).isInstanceOf(IOException.class).hasMessage("flush failed");
    assertThatThrownBy(out::close)
        .isInstanceOf(IOException.class)
        .hasMessage("Previous Brotli stream operation failed")
        .hasCause(target.flushFailure)
        .hasSuppressedException(target.closeFailure);
    out.close();
    assertThat(target.closes).isEqualTo(1);
  }

  @Test
  public void testTryWithResourcesPreservesWriteFailure() throws Exception {
    FailingOutputStream target = new FailingOutputStream();
    byte[] input = new byte[1024 * 1024];
    new Random(19).nextBytes(input);
    target.failOnWrite = true;

    assertThatThrownBy(() -> {
          try (CompressionOutputStream out = newCodec(1).createOutputStream(target)) {
            out.write(input);
          }
        })
        .isInstanceOf(IOException.class)
        .hasMessage("write failed")
        .isSameAs(target.writeFailure);
    assertThat(target.writeFailure.getSuppressed()).hasSize(1);
    assertThat(target.writeFailure.getSuppressed()[0])
        .isInstanceOf(IOException.class)
        .hasMessage("Previous Brotli stream operation failed")
        .hasCause(target.writeFailure);
    assertThat(target.closes).isEqualTo(1);
  }

  @Test
  public void testCloseAfterFinishFailureIsIdempotent() throws Exception {
    FailingOutputStream target = new FailingOutputStream();
    BrotliCodec codec = newCodec(5);
    CompressionOutputStream out = codec.createOutputStream(target);
    out.write(new byte[100 * 1024]);
    target.failOnFlush = true;

    assertThatThrownBy(out::finish).isInstanceOf(IOException.class).hasMessage("flush failed");
    assertThatThrownBy(out::close).isInstanceOf(IOException.class).hasMessageContaining("failed");
    out.close();
    assertThat(target.closes).isEqualTo(1);
  }

  @Test
  public void testFinishFailureIsReportedOnRetry() throws Exception {
    FailingOutputStream target = new FailingOutputStream();
    BrotliCodec codec = newCodec(5);
    CompressionOutputStream out = codec.createOutputStream(target);
    out.write(new byte[100 * 1024]);
    target.failOnFlush = true;

    assertThatThrownBy(out::finish).isInstanceOf(IOException.class).hasMessage("flush failed");
    assertThatThrownBy(out::finish)
        .isInstanceOf(IOException.class)
        .hasMessage("Previous Brotli stream operation failed")
        .hasCause(target.flushFailure);
    assertThatThrownBy(out::close).isInstanceOf(IOException.class).hasMessageContaining("failed");
  }

  @Test
  public void testPositionAndSeek() throws Exception {
    byte[] input = new byte[100];
    new Random(7).nextBytes(input);
    BrotliCodec codec = newCodec(5);

    try (CompressionInputStream in = codec.createInputStream(compress(codec, input))) {
      assertThat(in.read()).isEqualTo(input[0] & 0xFF);
      assertThat(in.getPos()).isEqualTo(1);

      in.seek(0);
      assertThat(in.getPos()).isZero();
      assertThat(in.readAllBytes()).isEqualTo(input);
      assertThatThrownBy(in::resetState)
          .isInstanceOf(IOException.class)
          .hasMessage("BrotliCompressionInputStream does not support resetState");
    }
  }

  private static BrotliCodec newCodec(int quality) {
    Configuration conf = new Configuration(false);
    conf.setInt(BrotliCodec.BROTLI_QUALITY, quality);
    BrotliCodec codec = new BrotliCodec();
    codec.setConf(conf);
    return codec;
  }

  private static BrotliOutputStream encoderOf(CompressionOutputStream out) throws ReflectiveOperationException {
    // Verify the native encoder's lifecycle without exposing it through the production codec API.
    Field field = BrotliCompressionOutputStream.class.getDeclaredField("encoder");
    field.setAccessible(true);
    return (BrotliOutputStream) field.get(out);
  }

  private static ByteArrayInputStream compress(BrotliCodec codec, byte[] input) throws IOException {
    return new ByteArrayInputStream(compressBytes(codec, input));
  }

  private static byte[] compressBytes(BrotliCodec codec, byte[] input) throws IOException {
    ByteArrayOutputStream compressed = new ByteArrayOutputStream();
    try (CompressionOutputStream out = codec.createOutputStream(compressed)) {
      out.write(input);
      out.finish();
    }
    return compressed.toByteArray();
  }

  private static byte[] largeCompressibleInput() {
    StringBuilder builder = new StringBuilder();
    String line = "at com.example.SomeClass.someMethod(SomeClass.java:123)\n";
    while (builder.length() < 5 * 1024 * 1024) {
      builder.append(line);
    }
    return builder.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static final class TrackingOutputStream extends ByteArrayOutputStream {
    private boolean closed;
    private int flushes;

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }

    @Override
    public void flush() throws IOException {
      flushes++;
      super.flush();
    }
  }

  private static final class FailingOutputStream extends OutputStream {
    private final IOException writeFailure = new IOException("write failed");
    private final IOException flushFailure = new IOException("flush failed");
    private final IOException closeFailure = new IOException("close failed");
    private boolean failOnWrite;
    private boolean failOnFlush;
    private boolean failOnClose;
    private int closes;

    @Override
    public void write(int b) throws IOException {
      if (failOnWrite) {
        throw writeFailure;
      }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      if (failOnWrite) {
        throw writeFailure;
      }
    }

    @Override
    public void flush() throws IOException {
      if (failOnFlush) {
        throw flushFailure;
      }
    }

    @Override
    public void close() throws IOException {
      closes++;
      if (failOnClose) {
        throw closeFailure;
      }
    }
  }
}
