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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.AesMode;
import org.apache.parquet.crypto.ModuleCipherFactory;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.PositionOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Round-trip tests for {@link SelfReferenceStorage}, the storage-inheritance engine for FILE
 * self-references (parquet-format PR #603). Each test writes a stored representation and resolves it
 * back, asserting the resolved bytes equal the original and that the recorded {@code offset}/
 * {@code size} cover exactly the stored bytes.
 */
public class TestSelfReferenceStorage {

  private static final int PAGE_SIZE = 64 * 1024;
  // A 32-byte AES key.
  private static final byte[] COLUMN_KEY = "0123456789012345".getBytes();
  private static final byte[] FILE_AAD = "unique-file-aad!".getBytes();

  /** A simple in-memory {@link PositionOutputStream} for capturing written bytes. */
  private static final class InMemoryPositionOutputStream extends PositionOutputStream {
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    @Override
    public long getPos() {
      return baos.size();
    }

    @Override
    public void write(int b) {
      baos.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      baos.write(b, off, len);
    }

    byte[] toByteArray() {
      return baos.toByteArray();
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = CompressionCodecName.class,
      names = {"UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD", "LZ4_RAW"})
  public void testRoundTripUnencrypted(CompressionCodecName codec) throws IOException {
    CodecFactory codecFactory = new CodecFactory(new Configuration(), PAGE_SIZE);
    byte[] resolved = highlyCompressiblePayload(4096);

    InMemoryPositionOutputStream out = new InMemoryPositionOutputStream();
    // Simulate a leading byte already in the file, so offset is non-zero.
    out.write(new byte[] {(byte) 0xAB}, 0, 1);

    SelfReferenceStorage.StoredRange range = SelfReferenceStorage.write(
        BytesInput.from(resolved), codecFactory.getCompressor(codec), null, null, 0, 0, out);

    byte[] fileBytes = out.toByteArray();
    assertThat(range.getOffset()).isEqualTo(1L);
    assertThat(range.getSize()).isEqualTo(fileBytes.length - 1L);
    if (codec == CompressionCodecName.UNCOMPRESSED) {
      // Uncompressed: the stored bytes are exactly the resolved bytes.
      assertThat(range.getSize()).isEqualTo((long) resolved.length);
    }

    byte[] stored =
        Arrays.copyOfRange(fileBytes, (int) range.getOffset(), (int) (range.getOffset() + range.getSize()));
    BytesInput resolvedBack =
        SelfReferenceStorage.resolve(BytesInput.from(stored), codec, codecFactory, null, null, 0, 0, 0L);

    assertThat(resolvedBack.toByteArray()).isEqualTo(resolved);
    codecFactory.release();
  }

  /**
   * The decompressed size of a self-reference is not stored, so the reader grows its output buffer
   * until the payload fits. This exercises payload sizes spanning several doublings, including sizes
   * that are exact powers of two, where a full output buffer is ambiguous between "complete" and
   * "truncated".
   */
  @ParameterizedTest
  @EnumSource(
      value = CompressionCodecName.class,
      names = {"SNAPPY", "GZIP", "ZSTD", "LZ4_RAW"})
  public void testRoundTripAcrossBufferGrowth(CompressionCodecName codec) throws IOException {
    CodecFactory codecFactory = new CodecFactory(new Configuration(), PAGE_SIZE);
    int[] sizes = {1, 8192, 8193, 16384, 100_000, 1 << 20};

    for (int size : sizes) {
      byte[] resolved = highlyCompressiblePayload(size);
      InMemoryPositionOutputStream out = new InMemoryPositionOutputStream();
      SelfReferenceStorage.StoredRange range = SelfReferenceStorage.write(
          BytesInput.from(resolved), codecFactory.getCompressor(codec), null, null, 0, 0, out);

      byte[] stored = out.toByteArray();
      assertThat(range.getSize()).isEqualTo((long) stored.length);
      BytesInput resolvedBack =
          SelfReferenceStorage.resolve(BytesInput.from(stored), codec, codecFactory, null, null, 0, 0, 0L);
      assertThat(resolvedBack.toByteArray())
          .as("payload of %s bytes", size)
          .isEqualTo(resolved);
    }
    codecFactory.release();
  }

  /**
   * Incompressible data expands slightly under most codecs, so the initial guess of twice the
   * compressed size is generous; this simply confirms such payloads round-trip too.
   */
  @ParameterizedTest
  @EnumSource(
      value = CompressionCodecName.class,
      names = {"SNAPPY", "GZIP", "ZSTD", "LZ4_RAW"})
  public void testRoundTripIncompressiblePayload(CompressionCodecName codec) throws IOException {
    CodecFactory codecFactory = new CodecFactory(new Configuration(), PAGE_SIZE);
    byte[] resolved = new byte[64 * 1024];
    new java.util.Random(42).nextBytes(resolved);

    InMemoryPositionOutputStream out = new InMemoryPositionOutputStream();
    SelfReferenceStorage.StoredRange range = SelfReferenceStorage.write(
        BytesInput.from(resolved), codecFactory.getCompressor(codec), null, null, 0, 0, out);

    byte[] stored = out.toByteArray();
    assertThat(range.getSize()).isEqualTo((long) stored.length);
    BytesInput resolvedBack =
        SelfReferenceStorage.resolve(BytesInput.from(stored), codec, codecFactory, null, null, 0, 0, 0L);
    assertThat(resolvedBack.toByteArray()).isEqualTo(resolved);
    codecFactory.release();
  }

  @ParameterizedTest
  @EnumSource(value = AesMode.class)
  public void testRoundTripEncrypted(AesMode mode) throws IOException {
    CodecFactory codecFactory = new CodecFactory(new Configuration(), PAGE_SIZE);
    byte[] resolved = highlyCompressiblePayload(4096);
    CompressionCodecName codec = CompressionCodecName.SNAPPY;

    BlockCipher.Encryptor encryptor = ModuleCipherFactory.getEncryptor(mode, COLUMN_KEY);

    InMemoryPositionOutputStream out = new InMemoryPositionOutputStream();
    SelfReferenceStorage.StoredRange range = SelfReferenceStorage.write(
        BytesInput.from(resolved), codecFactory.getCompressor(codec), encryptor, FILE_AAD, 1, 2, out);

    byte[] fileBytes = out.toByteArray();
    assertThat(range.getOffset()).isEqualTo(0L);
    assertThat(range.getSize()).isEqualTo((long) fileBytes.length);
    // The stored module carries the 4-byte length prefix and 12-byte nonce (and a 16-byte GCM tag
    // for GCM), so it is larger than the raw compressed payload.
    int expectedOverhead = AesCipher.NONCE_LENGTH + 4 + (mode == AesMode.GCM ? AesCipher.GCM_TAG_LENGTH : 0);
    assertThat(range.getSize()).isGreaterThan((long) expectedOverhead);

    BlockCipher.Decryptor decryptor = ModuleCipherFactory.getDecryptor(mode, COLUMN_KEY);
    byte[] stored =
        Arrays.copyOfRange(fileBytes, (int) range.getOffset(), (int) (range.getOffset() + range.getSize()));
    BytesInput resolvedBack = SelfReferenceStorage.resolve(
        BytesInput.from(stored), codec, codecFactory, decryptor, FILE_AAD, 1, 2, range.getOffset());

    assertThat(resolvedBack.toByteArray()).isEqualTo(resolved);
    codecFactory.release();
  }

  /**
   * The AAD binds a stored representation to its offset, so resolving the same bytes as if they lived
   * at a different offset must fail rather than silently return data. For GCM the tag check catches
   * it; CTR has no tag, so it yields garbage instead -- either way the bytes must not come back
   * intact.
   */
  @Test
  public void testResolveWithWrongOffsetDoesNotReturnPayload() throws IOException {
    CodecFactory codecFactory = new CodecFactory(new Configuration(), PAGE_SIZE);
    byte[] resolved = highlyCompressiblePayload(4096);
    CompressionCodecName codec = CompressionCodecName.SNAPPY;

    BlockCipher.Encryptor encryptor = ModuleCipherFactory.getEncryptor(AesMode.GCM, COLUMN_KEY);
    InMemoryPositionOutputStream out = new InMemoryPositionOutputStream();
    SelfReferenceStorage.StoredRange range = SelfReferenceStorage.write(
        BytesInput.from(resolved), codecFactory.getCompressor(codec), encryptor, FILE_AAD, 1, 2, out);

    byte[] stored = out.toByteArray();
    BlockCipher.Decryptor decryptor = ModuleCipherFactory.getDecryptor(AesMode.GCM, COLUMN_KEY);
    assertThatThrownBy(() -> SelfReferenceStorage.resolve(
            BytesInput.from(stored), codec, codecFactory, decryptor, FILE_AAD, 1, 2, range.getOffset() + 1))
        .isInstanceOf(ParquetCryptoRuntimeException.class);
    codecFactory.release();
  }

  /**
   * Two self-references with identical payloads in the same column chunk sit at different offsets, so
   * their AADs differ and their ciphertexts must not be interchangeable.
   */
  @Test
  public void testIdenticalPayloadsAtDifferentOffsetsAreNotInterchangeable() throws IOException {
    CodecFactory codecFactory = new CodecFactory(new Configuration(), PAGE_SIZE);
    byte[] resolved = highlyCompressiblePayload(1024);
    CompressionCodecName codec = CompressionCodecName.SNAPPY;

    BlockCipher.Encryptor encryptor = ModuleCipherFactory.getEncryptor(AesMode.GCM, COLUMN_KEY);
    InMemoryPositionOutputStream out = new InMemoryPositionOutputStream();
    SelfReferenceStorage.StoredRange first = SelfReferenceStorage.write(
        BytesInput.from(resolved), codecFactory.getCompressor(codec), encryptor, FILE_AAD, 1, 2, out);
    SelfReferenceStorage.StoredRange second = SelfReferenceStorage.write(
        BytesInput.from(resolved), codecFactory.getCompressor(codec), encryptor, FILE_AAD, 1, 2, out);

    assertThat(second.getOffset()).isGreaterThan(first.getOffset());

    byte[] fileBytes = out.toByteArray();
    byte[] firstStored =
        Arrays.copyOfRange(fileBytes, (int) first.getOffset(), (int) (first.getOffset() + first.getSize()));

    // The first block's bytes cannot be resolved at the second block's offset.
    BlockCipher.Decryptor decryptor = ModuleCipherFactory.getDecryptor(AesMode.GCM, COLUMN_KEY);
    assertThatThrownBy(() -> SelfReferenceStorage.resolve(
            BytesInput.from(firstStored),
            codec,
            codecFactory,
            decryptor,
            FILE_AAD,
            1,
            2,
            second.getOffset()))
        .isInstanceOf(ParquetCryptoRuntimeException.class);
    codecFactory.release();
  }

  @Test
  public void testEmptyPayloadRoundTrip() throws IOException {
    CodecFactory codecFactory = new CodecFactory(new Configuration(), PAGE_SIZE);
    byte[] resolved = new byte[0];

    InMemoryPositionOutputStream out = new InMemoryPositionOutputStream();
    SelfReferenceStorage.StoredRange range = SelfReferenceStorage.write(
        BytesInput.from(resolved),
        codecFactory.getCompressor(CompressionCodecName.UNCOMPRESSED),
        null,
        null,
        0,
        0,
        out);

    assertThat(range.getSize()).isEqualTo(0L);
    BytesInput resolvedBack = SelfReferenceStorage.resolve(
        BytesInput.from(new byte[0]), CompressionCodecName.UNCOMPRESSED, codecFactory, null, null, 0, 0, 0L);
    assertThat(resolvedBack.toByteArray()).isEqualTo(resolved);
    codecFactory.release();
  }

  /** An empty payload round-trips through a real codec too, not only UNCOMPRESSED. */
  @ParameterizedTest
  @EnumSource(
      value = CompressionCodecName.class,
      names = {"SNAPPY", "GZIP", "ZSTD", "LZ4_RAW"})
  public void testEmptyPayloadRoundTripCompressed(CompressionCodecName codec) throws IOException {
    CodecFactory codecFactory = new CodecFactory(new Configuration(), PAGE_SIZE);
    InMemoryPositionOutputStream out = new InMemoryPositionOutputStream();
    SelfReferenceStorage.StoredRange range = SelfReferenceStorage.write(
        BytesInput.from(new byte[0]), codecFactory.getCompressor(codec), null, null, 0, 0, out);

    byte[] stored = out.toByteArray();
    assertThat(range.getSize()).isEqualTo((long) stored.length);
    BytesInput resolvedBack =
        SelfReferenceStorage.resolve(BytesInput.from(stored), codec, codecFactory, null, null, 0, 0, 0L);
    assertThat(resolvedBack.toByteArray()).isEmpty();
    codecFactory.release();
  }

  private static byte[] highlyCompressiblePayload(int length) {
    byte[] payload = new byte[length];
    for (int i = 0; i < length; i++) {
      payload[i] = (byte) (i % 16);
    }
    return payload;
  }
}
