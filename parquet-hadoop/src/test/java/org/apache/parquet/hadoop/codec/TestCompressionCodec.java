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
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.apache.parquet.bytes.ByteBufferReleaser;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestCompressionCodec {

  private final int pageSize = 64 * 1024;

  @Test
  public void testLz4RawBlock() throws IOException {
    testBlock(CompressionCodecName.LZ4_RAW);
  }

  @Test
  public void testSnappyBlock() throws IOException {
    testBlock(CompressionCodecName.SNAPPY);
  }

  private void testBlock(CompressionCodecName codecName) throws IOException {
    // Reuse the codec objects between test cases
    CompressionCodec codec = getCodec(codecName, 4 * 1024);
    Compressor compressor = codec.createCompressor();
    Decompressor decompressor = codec.createDecompressor();

    testBlockCompression(compressor, decompressor, "");
    testBlockCompression(compressor, decompressor, "FooBar");
    testBlockCompression(compressor, decompressor, "FooBar1FooBar2");
    testBlockCompression(compressor, decompressor, "FooBar");
    testBlockCompression(compressor, decompressor, "ablahblahblahabcdef");
    testBlockCompression(compressor, decompressor, "");
    testBlockCompression(compressor, decompressor, "FooBar");
  }

  // Test compression in the block fashion
  private void testBlockCompression(Compressor compressor, Decompressor decompressor, String data)
      throws IOException {
    compressor.reset();
    decompressor.reset();

    int uncompressedSize = data.length();
    byte[] uncompressedData = data.getBytes();

    assert (compressor.needsInput());
    compressor.setInput(uncompressedData, 0, uncompressedSize);
    assert (compressor.needsInput());
    compressor.finish();
    assert (!compressor.needsInput());
    assert (!compressor.finished() || uncompressedSize == 0);
    byte[] compressedData = new byte[1000];

    int compressedSize = compressor.compress(compressedData, 0, 1000);
    assert (compressor.finished());

    assert (!decompressor.finished());
    assert (decompressor.needsInput());
    decompressor.setInput(compressedData, 0, compressedSize);
    assert (!decompressor.finished());
    byte[] decompressedData = new byte[uncompressedSize];
    int decompressedSize = decompressor.decompress(decompressedData, 0, uncompressedSize);
    assert (decompressor.finished());

    assertThat(decompressedSize).isEqualTo(uncompressedSize);
    assertThat(decompressedData).isEqualTo(uncompressedData);
  }

  @Test
  public void testLz4RawCodec() throws IOException {
    testCodec(CompressionCodecName.LZ4_RAW);
  }

  @Test
  public void testSnappyCodec() throws IOException {
    testCodec(CompressionCodecName.SNAPPY);
  }

  // Test compression in the streaming fashion
  private void testCodec(CompressionCodecName codecName) throws IOException {
    int[] bufferSizes = {128, 1024, 4 * 1024, 16 * 1024, 128 * 1024, 1024 * 1024};
    int[] dataSizes = {0, 1, 10, 128, 1024, 2048, 1024 * 1024};

    for (int i = 0; i < bufferSizes.length; i++) {
      CompressionCodec codec = getCodec(codecName, bufferSizes[i]);
      for (int j = 0; j < dataSizes.length; j++) {
        // do not repeat
        testCodec(codec, dataSizes[j], dataSizes[j]);
        // repeat by every 128 bytes
        testCodec(codec, dataSizes[j], 128);
      }
    }
  }

  private void testCodec(CompressionCodec codec, int dataSize, int repeatSize) throws IOException {
    byte[] data = new byte[dataSize];
    if (repeatSize >= dataSize) {
      (new Random()).nextBytes(data);
    } else {
      byte[] repeat = new byte[repeatSize];
      (new Random()).nextBytes(repeat);
      for (int offset = 0; offset < dataSize; offset += repeatSize) {
        System.arraycopy(repeat, 0, data, offset, Math.min(repeatSize, dataSize - offset));
      }
    }
    BytesInput compressedData = compress(codec, BytesInput.from(data));
    byte[] decompressedData = decompress(codec, compressedData, data.length);
    assertThat(decompressedData).isEqualTo(data);
  }

  private BytesInput compress(CompressionCodec codec, BytesInput bytes) throws IOException {
    ByteArrayOutputStream compressedOutBuffer = new ByteArrayOutputStream((int) bytes.size());
    CompressionOutputStream cos = codec.createOutputStream(compressedOutBuffer);
    bytes.writeAllTo(cos);
    cos.close();
    return BytesInput.from(compressedOutBuffer);
  }

  private byte[] decompress(CompressionCodec codec, BytesInput bytes, int uncompressedSize) throws IOException {
    InputStream is = codec.createInputStream(bytes.toInputStream());
    byte[] decompressed = BytesInput.from(is, uncompressedSize).toByteArray();
    is.close();
    return decompressed;
  }

  private CompressionCodec getCodec(CompressionCodecName codecName, int bufferSize) {
    switch (codecName) {
      case LZ4_RAW: {
        Configuration conf = new Configuration();
        conf.setInt(Lz4RawCodec.BUFFER_SIZE_CONFIG, bufferSize);
        Lz4RawCodec codec = new Lz4RawCodec();
        codec.setConf(conf);
        return codec;
      }
      case SNAPPY: {
        Configuration conf = new Configuration();
        conf.setInt("io.file.buffer.size", bufferSize);
        SnappyCodec codec = new SnappyCodec();
        codec.setConf(conf);
        return codec;
      }
      default:
        // Not implemented yet
        return null;
    }
  }

  /**
   * Regression test for #3478: LZ4_RAW heap decompression fails when the decompressed page
   * exceeds the ~8KB chunk size used by stream materialization in BytesInput.copy().
   */
  @Test
  public void testLz4RawHeapDecompressorCanCopyLargePage() throws IOException {
    final int size = 16 * 1024;
    final byte[] raw = new byte[size];
    new Random(42).nextBytes(raw);

    try (TrackingByteBufferAllocator allocator = TrackingByteBufferAllocator.wrap(new DirectByteBufferAllocator());
        ByteBufferReleaser releaser = new ByteBufferReleaser(allocator)) {
      CodecFactory heapCodecFactory = new CodecFactory(new Configuration(), pageSize);
      BytesInputCompressor compressor = heapCodecFactory.getCompressor(CompressionCodecName.LZ4_RAW);
      BytesInputDecompressor decompressor = heapCodecFactory.getDecompressor(CompressionCodecName.LZ4_RAW);

      BytesInput compressed = compressor.compress(BytesInput.from(raw));
      BytesInput decompressed = decompressor.decompress(compressed, size);

      BytesInput copied = decompressed.copy(releaser);
      assertThat(copied.toByteArray()).isEqualTo(raw);

      compressor.release();
      decompressor.release();
      heapCodecFactory.release();
    }
  }

  /**
   * CodecFactory.HeapBytesDecompressor routes SNAPPY/LZ4_RAW through the one-shot
   * DirectDecompressor path for large pages (multi-MB, the size range that used to force many
   * incremental setInput() growth cycles); this confirms that path still round-trips correctly
   * end to end through the public CompressionCodecFactory API.
   */
  @Test
  public void testSnappyHeapDecompressorCanDecompressLargePage() throws IOException {
    testHeapDecompressorCanDecompressLargePage(CompressionCodecName.SNAPPY);
  }

  @Test
  public void testLz4RawHeapDecompressorCanDecompressLargePage() throws IOException {
    testHeapDecompressorCanDecompressLargePage(CompressionCodecName.LZ4_RAW);
  }

  private void testHeapDecompressorCanDecompressLargePage(CompressionCodecName codecName) throws IOException {
    final int size = 8 * 1024 * 1024;
    final byte[] raw = new byte[size];
    new Random(42).nextBytes(raw);

    try (TrackingByteBufferAllocator allocator = TrackingByteBufferAllocator.wrap(new DirectByteBufferAllocator());
        ByteBufferReleaser releaser = new ByteBufferReleaser(allocator)) {
      CodecFactory heapCodecFactory = new CodecFactory(new Configuration(), pageSize);
      BytesInputCompressor compressor = heapCodecFactory.getCompressor(codecName);
      BytesInputDecompressor decompressor = heapCodecFactory.getDecompressor(codecName);

      BytesInput compressed = compressor.compress(BytesInput.from(raw));
      BytesInput decompressed = decompressor.decompress(compressed, size);

      BytesInput copied = decompressed.copy(releaser);
      assertThat(copied.toByteArray()).isEqualTo(raw);

      compressor.release();
      decompressor.release();
      heapCodecFactory.release();
    }
  }

  /**
   * If a page's embedded/actual decompressed size disagrees with the uncompressed_page_size
   * declared in the page header, decompression must fail loudly (as the old streaming path did,
   * via StreamBytesInput.readFully's EOFException) rather than silently handing back a
   * shorter-than-declared BytesInput.
   */
  @Test
  public void testSnappyHeapDecompressorThrowsOnSizeMismatch() throws IOException {
    testHeapDecompressorThrowsOnSizeMismatch(CompressionCodecName.SNAPPY);
  }

  @Test
  public void testLz4RawHeapDecompressorThrowsOnSizeMismatch() throws IOException {
    testHeapDecompressorThrowsOnSizeMismatch(CompressionCodecName.LZ4_RAW);
  }

  private void testHeapDecompressorThrowsOnSizeMismatch(CompressionCodecName codecName) throws IOException {
    final int size = 1024;
    final byte[] raw = new byte[size];
    new Random(42).nextBytes(raw);

    CodecFactory heapCodecFactory = new CodecFactory(new Configuration(), pageSize);
    BytesInputCompressor compressor = heapCodecFactory.getCompressor(codecName);
    BytesInputDecompressor decompressor = heapCodecFactory.getDecompressor(codecName);

    BytesInput compressed = compressor.compress(BytesInput.from(raw));

    assertThatThrownBy(() -> decompressor.decompress(compressed, size + 100))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Unexpected decompressed size");

    compressor.release();
    decompressor.release();
    heapCodecFactory.release();
  }

  /**
   * decompress(ByteBuffer, int, ByteBuffer, int) is a public entry point on
   * CompressionCodecFactory.BytesInputDecompressor that no in-repo caller exercises for
   * SNAPPY/LZ4_RAW today (ColumnChunkPageReadStore only reaches it when both an off-heap
   * allocator and off-heap decrypt buffering are configured together). It shares the same
   * DirectDecompressor one-shot plumbing as the BytesInput overload, but has its own
   * position/limit save-and-restore bookkeeping that was previously untested. This exercises it
   * directly with non-zero starting positions and generous surrounding limits, so a broken
   * restore (e.g. a dropped `output.limit(origOutputLimit)`) would be caught here.
   */
  @Test
  public void testSnappyHeapDecompressorByteBufferOverloadRoundTrip() throws IOException {
    testHeapDecompressorByteBufferOverloadRoundTrip(CompressionCodecName.SNAPPY);
  }

  @Test
  public void testLz4RawHeapDecompressorByteBufferOverloadRoundTrip() throws IOException {
    testHeapDecompressorByteBufferOverloadRoundTrip(CompressionCodecName.LZ4_RAW);
  }

  private void testHeapDecompressorByteBufferOverloadRoundTrip(CompressionCodecName codecName) throws IOException {
    final int size = 4096;
    final byte[] raw = new byte[size];
    new Random(42).nextBytes(raw);
    final int padding = 17;

    CodecFactory heapCodecFactory = new CodecFactory(new Configuration(), pageSize);
    BytesInputCompressor compressor = heapCodecFactory.getCompressor(codecName);
    BytesInputDecompressor decompressor = heapCodecFactory.getDecompressor(codecName);

    byte[] compressedArr = compressor.compress(BytesInput.from(raw)).toByteArray();

    // Pad both buffers on either side of the region actually used, and leave their limits at
    // full capacity (not trimmed to the compressed/decompressed size) so the test also proves
    // the original limit -- not just the original position -- is restored after the call.
    ByteBuffer input = ByteBuffer.allocateDirect(padding + compressedArr.length + padding);
    input.position(padding);
    input.put(compressedArr);
    input.position(padding);
    int originalInputLimit = input.limit();

    ByteBuffer output = ByteBuffer.allocateDirect(padding + size + padding);
    output.position(padding);
    int originalOutputLimit = output.limit();

    decompressor.decompress(input, compressedArr.length, output, size);

    assertThat(input.position()).isEqualTo(padding + compressedArr.length);
    assertThat(input.limit()).isEqualTo(originalInputLimit);
    assertThat(output.position()).isEqualTo(padding + size);
    assertThat(output.limit()).isEqualTo(originalOutputLimit);

    byte[] result = new byte[size];
    output.position(padding);
    output.get(result);
    assertThat(result).isEqualTo(raw);

    compressor.release();
    decompressor.release();
    heapCodecFactory.release();
  }

  /**
   * The DirectDecompressor one-shot path requires direct buffers (xerial Snappy throws
   * SnappyError.NOT_A_DIRECT_BUFFER for anything else), but this overload's own public contract
   * (CompressionCodecFactory.BytesInputDecompressor.decompress(ByteBuffer,int,ByteBuffer,int))
   * makes no such requirement and never did -- any caller holding ordinary heap ByteBuffers (e.g.
   * a downstream CodecFactory subclass like Iceberg's) must keep working here.
   */
  @Test
  public void testSnappyHeapDecompressorByteBufferOverloadWorksWithHeapBuffers() throws IOException {
    testHeapDecompressorByteBufferOverloadWorksWithHeapBuffers(CompressionCodecName.SNAPPY);
  }

  @Test
  public void testLz4RawHeapDecompressorByteBufferOverloadWorksWithHeapBuffers() throws IOException {
    testHeapDecompressorByteBufferOverloadWorksWithHeapBuffers(CompressionCodecName.LZ4_RAW);
  }

  private void testHeapDecompressorByteBufferOverloadWorksWithHeapBuffers(CompressionCodecName codecName)
      throws IOException {
    final int size = 4096;
    final byte[] raw = new byte[size];
    new Random(42).nextBytes(raw);

    CodecFactory heapCodecFactory = new CodecFactory(new Configuration(), pageSize);
    BytesInputCompressor compressor = heapCodecFactory.getCompressor(codecName);
    BytesInputDecompressor decompressor = heapCodecFactory.getDecompressor(codecName);

    byte[] compressedArr = compressor.compress(BytesInput.from(raw)).toByteArray();

    ByteBuffer input = ByteBuffer.allocate(compressedArr.length);
    input.put(compressedArr).flip();
    ByteBuffer output = ByteBuffer.allocate(size);

    decompressor.decompress(input, compressedArr.length, output, size);

    byte[] result = new byte[size];
    output.flip();
    output.get(result);
    assertThat(result).isEqualTo(raw);

    compressor.release();
    decompressor.release();
    heapCodecFactory.release();
  }

  /**
   * The direct buffers HeapBytesDecompressor allocates for the DirectDecompressor one-shot path
   * are reused across calls instead of freshly allocated each time (allocating fresh direct
   * buffers per call scales badly under concurrent decompression). Round-tripping several pages
   * of varying sizes -- including a shrink back down after a large page forces the reused buffer
   * to grow -- on the same decompressor instance guards against stale data or incorrect
   * position/limit bookkeeping leaking between reuses.
   */
  @Test
  public void testSnappyHeapDecompressorReusesBuffersAcrossCalls() throws IOException {
    testHeapDecompressorReusesBuffersAcrossCalls(CompressionCodecName.SNAPPY);
  }

  @Test
  public void testLz4RawHeapDecompressorReusesBuffersAcrossCalls() throws IOException {
    testHeapDecompressorReusesBuffersAcrossCalls(CompressionCodecName.LZ4_RAW);
  }

  private void testHeapDecompressorReusesBuffersAcrossCalls(CompressionCodecName codecName) throws IOException {
    CodecFactory heapCodecFactory = new CodecFactory(new Configuration(), pageSize);
    BytesInputCompressor compressor = heapCodecFactory.getCompressor(codecName);
    BytesInputDecompressor decompressor = heapCodecFactory.getDecompressor(codecName);

    int[] sizes = {1024, 1024 * 1024, 256};
    Random random = new Random(42);
    for (int size : sizes) {
      byte[] raw = new byte[size];
      random.nextBytes(raw);
      BytesInput compressed = compressor.compress(BytesInput.from(raw));
      byte[] result = decompressor.decompress(compressed, size).toByteArray();
      assertThat(result).as("size=" + size).isEqualTo(raw);
    }

    compressor.release();
    decompressor.release();
    heapCodecFactory.release();
  }

  /**
   * If reading the compressed bytes themselves fails partway through (e.g. a truncated/corrupt
   * stream, as ParquetFileReader.readCompressedDictionary can hit on a damaged file), the reused
   * input buffer must still be returned to a valid state -- not left permanently marked as
   * "in use" -- so a subsequent decompress call on the same (pooled, reused) decompressor
   * instance keeps working rather than failing with an unrelated IllegalStateException forever.
   */
  @Test
  public void testSnappyHeapDecompressorRecoversAfterFailedRead() throws IOException {
    testHeapDecompressorRecoversAfterFailedRead(CompressionCodecName.SNAPPY);
  }

  @Test
  public void testLz4RawHeapDecompressorRecoversAfterFailedRead() throws IOException {
    testHeapDecompressorRecoversAfterFailedRead(CompressionCodecName.LZ4_RAW);
  }

  private void testHeapDecompressorRecoversAfterFailedRead(CompressionCodecName codecName) throws IOException {
    final int size = 4096;
    final byte[] raw = new byte[size];
    new Random(42).nextBytes(raw);

    CodecFactory heapCodecFactory = new CodecFactory(new Configuration(), pageSize);
    BytesInputCompressor compressor = heapCodecFactory.getCompressor(codecName);
    BytesInputDecompressor decompressor = heapCodecFactory.getDecompressor(codecName);

    byte[] compressedArr = compressor.compress(BytesInput.from(raw)).toByteArray();

    // A stream that throws partway through being read, simulating a truncated/corrupt file.
    InputStream failingStream = new InputStream() {
      int position = 0;

      @Override
      public int read() throws IOException {
        if (position >= compressedArr.length / 2) {
          throw new IOException("simulated truncated read");
        }
        return compressedArr[position++] & 0xff;
      }
    };

    // StreamBytesInput.writeInto wraps the underlying IOException in a RuntimeException; the
    // exact type isn't the point here, recovery afterward (below) is.
    assertThatThrownBy(() -> decompressor.decompress(BytesInput.from(failingStream, compressedArr.length), size));

    // The decompressor (and its reused buffers) must still be usable for the next page.
    BytesInput compressed = compressor.compress(BytesInput.from(raw));
    byte[] result = decompressor.decompress(compressed, size).toByteArray();
    assertThat(result).isEqualTo(raw);

    compressor.release();
    decompressor.release();
    heapCodecFactory.release();
  }

  @Test
  public void testSnappyHeapDecompressorByteBufferOverloadThrowsOnSizeMismatch() throws IOException {
    testHeapDecompressorByteBufferOverloadThrowsOnSizeMismatch(CompressionCodecName.SNAPPY);
  }

  @Test
  public void testLz4RawHeapDecompressorByteBufferOverloadThrowsOnSizeMismatch() throws IOException {
    testHeapDecompressorByteBufferOverloadThrowsOnSizeMismatch(CompressionCodecName.LZ4_RAW);
  }

  private void testHeapDecompressorByteBufferOverloadThrowsOnSizeMismatch(CompressionCodecName codecName)
      throws IOException {
    final int size = 1024;
    final byte[] raw = new byte[size];
    new Random(42).nextBytes(raw);

    CodecFactory heapCodecFactory = new CodecFactory(new Configuration(), pageSize);
    BytesInputCompressor compressor = heapCodecFactory.getCompressor(codecName);
    BytesInputDecompressor decompressor = heapCodecFactory.getDecompressor(codecName);

    byte[] compressedArr = compressor.compress(BytesInput.from(raw)).toByteArray();
    ByteBuffer input = ByteBuffer.allocateDirect(compressedArr.length);
    input.put(compressedArr).flip();
    ByteBuffer output = ByteBuffer.allocateDirect(size + 100);

    assertThatThrownBy(() -> decompressor.decompress(input, compressedArr.length, output, size + 100))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Unexpected decompressed size");

    compressor.release();
    decompressor.release();
    heapCodecFactory.release();
  }

  /**
   * SnappyDecompressor.decompress(ByteBuffer, ByteBuffer) must not rely solely on the xerial
   * Snappy library's own internal bounds checking (CVE-2026-90559 relevant: snappy-java <=
   * 1.1.10.8 would write past the end of an undersized output buffer rather than rejecting it).
   * Parquet should reject an output buffer that is too small for the block's own declared
   * uncompressed length itself, independent of which snappy-java version happens to be resolved
   * on the classpath at runtime.
   */
  @Test
  public void testSnappyDecompressRejectsUndersizedOutputBuffer() throws IOException {
    byte[] raw = new byte[4096];
    new Random(42).nextBytes(raw);

    SnappyCompressor compressor = new SnappyCompressor();
    compressor.setInput(raw, 0, raw.length);
    compressor.finish();
    ByteArrayOutputStream compressedOut = new ByteArrayOutputStream();
    byte[] chunk = new byte[64 * 1024];
    while (!compressor.finished()) {
      int n = compressor.compress(chunk, 0, chunk.length);
      compressedOut.write(chunk, 0, n);
    }
    byte[] compressedArr = compressedOut.toByteArray();
    ByteBuffer compressedBuf = ByteBuffer.allocateDirect(compressedArr.length);
    compressedBuf.put(compressedArr).flip();

    // Too small for the block's own declared uncompressed length (raw.length).
    ByteBuffer undersizedOutput = ByteBuffer.allocateDirect(raw.length - 100);

    assertThatThrownBy(() -> new SnappyDecompressor().decompress(compressedBuf, undersizedOutput))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("declares");
  }

  /**
   * DirectDecompressor.decompress(ByteBuffer, ByteBuffer) lets a caller decompress straight
   * between two correctly-sized buffers using the already-known compressed/uncompressed sizes (as
   * recorded in a Parquet page header), without funneling the input through setInput()'s
   * incrementally-growing scratch buffer first.
   */
  @Test
  public void testSnappyOneShotDirectDecompress() throws IOException {
    testOneShotDirectDecompress(new SnappyCompressor(), new SnappyDecompressor());
  }

  @Test
  public void testLz4RawOneShotDirectDecompress() throws IOException {
    testOneShotDirectDecompress(new Lz4RawCompressor(), new Lz4RawDecompressor());
  }

  private <T extends NonBlockedDecompressor & DirectDecompressor> void testOneShotDirectDecompress(
      Compressor compressor, T decompressor) throws IOException {
    // Large enough to have required many setInput() growth cycles under the old streaming path.
    byte[] raw = new byte[5 * 1024 * 1024];
    new Random(42).nextBytes(raw);

    compressor.setInput(raw, 0, raw.length);
    compressor.finish();
    ByteArrayOutputStream compressedOut = new ByteArrayOutputStream();
    byte[] chunk = new byte[64 * 1024];
    while (!compressor.finished()) {
      int n = compressor.compress(chunk, 0, chunk.length);
      compressedOut.write(chunk, 0, n);
    }

    byte[] compressedArr = compressedOut.toByteArray();
    ByteBuffer compressedBuf = ByteBuffer.allocateDirect(compressedArr.length);
    compressedBuf.put(compressedArr).flip();
    ByteBuffer uncompressedBuf = ByteBuffer.allocateDirect(raw.length);

    int startPos = uncompressedBuf.position();
    decompressor.decompress(compressedBuf, uncompressedBuf);
    // Some implementations flip the output buffer, some don't: rely on the limit if the
    // position did not change (mirrors DirectCodecFactory.FullDirectDecompressor).
    int decompressedSize = uncompressedBuf.position() - startPos;
    if (decompressedSize == 0) {
      decompressedSize = uncompressedBuf.limit();
    }

    assertThat(decompressedSize).isEqualTo(raw.length);
    byte[] result = new byte[decompressedSize];
    uncompressedBuf.rewind();
    uncompressedBuf.get(result);
    assertThat(result).isEqualTo(raw);
  }

  @Test
  public void TestDecompressorInvalidState() throws IOException {
    // Create a mock Decompressor that returns 0 when decompress is called.
    Decompressor mockDecompressor = Mockito.mock(Decompressor.class);
    when(mockDecompressor.decompress(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
        .thenReturn(0);

    // Create a NonBlockedDecompressorStream with the mock Decompressor.
    NonBlockedDecompressorStream decompressorStream =
        new NonBlockedDecompressorStream(new ByteArrayInputStream(new byte[0]), mockDecompressor, 1024);

    assertThatThrownBy(() -> decompressorStream.read(new byte[1024], 0, 1024))
        .isInstanceOf(IOException.class)
        .hasMessage("Corrupt file: Zero bytes read during decompression.");
  }
}
