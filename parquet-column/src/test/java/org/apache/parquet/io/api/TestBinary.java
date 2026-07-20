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
package org.apache.parquet.io.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.TestBinary.BinaryFactory.BinaryAndOriginal;
import org.junit.jupiter.api.Test;

public class TestBinary {

  private static final String testString = "test-123";
  private static final String UTF8 = "UTF-8";

  static interface BinaryFactory {
    static class BinaryAndOriginal {
      public Binary binary;
      public byte[] original;

      public BinaryAndOriginal(Binary binary, byte[] original) {
        this.binary = binary;
        this.original = original;
      }
    }

    BinaryAndOriginal get(byte[] bytes, boolean reused) throws Exception;
  }

  private static void mutate(byte[] bytes) {
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) (bytes[i] + 1);
    }
  }

  private static final BinaryFactory BYTE_ARRAY_BACKED_BF = new BinaryFactory() {
    @Override
    public BinaryAndOriginal get(byte[] bytes, boolean reused) throws Exception {
      byte[] orig = Arrays.copyOf(bytes, bytes.length);
      if (reused) {
        return new BinaryAndOriginal(Binary.fromReusedByteArray(orig), orig);
      } else {
        return new BinaryAndOriginal(Binary.fromConstantByteArray(orig), orig);
      }
    }
  };

  private static final BinaryFactory BYTE_ARRAY_SLICE_BACKED_BF = new BinaryFactory() {
    @Override
    public BinaryAndOriginal get(byte[] bytes, boolean reused) throws Exception {
      byte[] orig = padded(bytes);
      Binary b;
      if (reused) {
        b = Binary.fromReusedByteArray(orig, 5, bytes.length);
      } else {
        b = Binary.fromConstantByteArray(orig, 5, bytes.length);
      }
      assertThat(b.getBytes()).isEqualTo(bytes);
      return new BinaryAndOriginal(b, orig);
    }
  };

  private static final BinaryFactory BUFFER_BF = new BinaryFactory() {
    @Override
    public BinaryAndOriginal get(byte[] bytes, boolean reused) throws Exception {
      byte[] orig = padded(bytes);
      ByteBuffer buff = ByteBuffer.wrap(orig, 5, bytes.length);
      Binary b;

      if (reused) {
        b = Binary.fromReusedByteBuffer(buff);
      } else {
        b = Binary.fromConstantByteBuffer(buff);
      }

      buff.mark();
      assertThat(b.getBytes()).isEqualTo(bytes);
      buff.reset();
      return new BinaryAndOriginal(b, orig);
    }
  };

  private static final BinaryFactory DIRECT_BUFFER_BF = new BinaryFactory() {
    @Override
    public BinaryAndOriginal get(byte[] bytes, boolean reused) throws Exception {
      ByteBuffer direct = ByteBuffer.allocateDirect(bytes.length);
      direct.put(bytes);
      direct.flip();
      Binary b;

      if (reused) {
        b = Binary.fromReusedByteBuffer(direct);
      } else {
        b = Binary.fromConstantByteBuffer(direct);
      }

      assertThat(b.getBytes()).isEqualTo(bytes);
      // Return the backing byte[] so tests can mutate it, though for direct buffers
      // there is no accessible backing array. We return a copy of the original bytes.
      return new BinaryAndOriginal(b, bytes);
    }
  };

  private static final BinaryFactory STRING_BF = new BinaryFactory() {
    @Override
    public BinaryAndOriginal get(byte[] bytes, boolean reused) throws Exception {
      Binary b = Binary.fromString(new String(bytes, UTF8));
      return new BinaryAndOriginal(b, b.getBytesUnsafe()); // only way to get underlying bytes for testing
    }
  };

  private static byte[] padded(byte[] bytes) {
    byte[] padded = new byte[bytes.length + 10];

    for (int i = 0; i < 5; i++) {
      padded[i] = (byte) i;
    }

    System.arraycopy(bytes, 0, padded, 5, bytes.length);

    for (int i = 0; i < 5; i++) {
      padded[i + 5 + bytes.length] = (byte) i;
    }

    return padded;
  }

  @Test
  public void testByteArrayBackedBinary() throws Exception {
    testBinary(BYTE_ARRAY_BACKED_BF, true);
    testBinary(BYTE_ARRAY_BACKED_BF, false);
  }

  @Test
  public void testByteArraySliceBackedBinary() throws Exception {
    testBinary(BYTE_ARRAY_SLICE_BACKED_BF, true);
    testBinary(BYTE_ARRAY_SLICE_BACKED_BF, false);
  }

  @Test
  public void testByteBufferBackedBinary() throws Exception {
    testBinary(BUFFER_BF, true);
    testBinary(BUFFER_BF, false);
  }

  @Test
  public void testDirectByteBufferBackedBinary() throws Exception {
    // Direct buffers have different copy() semantics (always materializes to heap),
    // so we test them separately instead of using the generic testBinary flow.
    testSlice(DIRECT_BUFFER_BF, true);
    testSlice(DIRECT_BUFFER_BF, false);
    testDirectConstantCopy(DIRECT_BUFFER_BF);
    testDirectReusedCopy(DIRECT_BUFFER_BF);
    testSerializable(DIRECT_BUFFER_BF, true);
    testSerializable(DIRECT_BUFFER_BF, false);
  }

  @Test
  public void testDirectByteBufferCopyAlwaysMaterializesToHeap() throws Exception {
    // For constant (non-reused) direct ByteBuffers, copy() must return a new Binary
    // rather than 'this', because the direct memory can be freed independently.
    byte[] data = testString.getBytes(UTF8);
    ByteBuffer direct = ByteBuffer.allocateDirect(data.length);
    direct.put(data);
    direct.flip();

    Binary binary = Binary.fromConstantByteBuffer(direct);
    Binary copy = binary.copy();

    // The copy must NOT be the same object, even though the binary is constant
    assertThat(copy)
        .as("copy() of a direct ByteBuffer-backed constant Binary must not return 'this'")
        .isNotSameAs(binary);
    assertThat(copy.getBytes()).isEqualTo(data);
    assertThat(copy.getBytesUnsafe()).isEqualTo(data);
  }

  @Test
  public void testDirectByteBufferCopyIsIndependentOfOriginalBuffer() throws Exception {
    // Verify the copied Binary is independent of the original direct ByteBuffer.
    // Simulates the scenario where direct memory is overwritten after copy.
    byte[] data = testString.getBytes(UTF8);
    ByteBuffer direct = ByteBuffer.allocateDirect(data.length);
    direct.put(data);
    direct.flip();

    Binary binary = Binary.fromReusedByteBuffer(direct);
    Binary copy = binary.copy();

    // Overwrite the direct buffer content to simulate memory reuse / free
    direct.clear();
    for (int i = 0; i < data.length; i++) {
      direct.put((byte) 0);
    }

    // The copy should still hold the original data
    assertThat(copy.getBytes()).isEqualTo(data);
    assertThat(copy.getBytesUnsafe()).isEqualTo(data);
  }

  @Test
  public void testHeapByteBufferConstantCopyReturnsSame() throws Exception {
    // For heap-backed constant ByteBuffers, copy() should return 'this' (existing behavior)
    byte[] data = testString.getBytes(UTF8);
    ByteBuffer heap = ByteBuffer.wrap(data);

    Binary binary = Binary.fromConstantByteBuffer(heap);
    Binary copy = binary.copy();

    assertThat(copy)
        .as("copy() of a heap ByteBuffer-backed constant Binary should return 'this'")
        .isSameAs(binary);
  }

  @Test
  public void testEqualityMethods() throws Exception {
    Binary bin1 = Binary.fromConstantByteArray("alice".getBytes(), 1, 3);
    Binary bin2 = Binary.fromConstantByteBuffer(ByteBuffer.wrap("alice".getBytes(), 1, 3));
    assertThat(bin2).isEqualTo(bin1);
  }

  @Test
  public void testWriteAllTo() throws Exception {
    byte[] orig = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
    testWriteAllToHelper(Binary.fromConstantByteBuffer(ByteBuffer.wrap(orig)), orig);
    ByteBuffer buf = ByteBuffer.allocateDirect(orig.length);
    buf.put(orig);
    buf.flip();
    testWriteAllToHelper(Binary.fromConstantByteBuffer(buf), orig);
  }

  private void testWriteAllToHelper(Binary binary, byte[] orig) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream(orig.length);
    binary.writeTo(out);
    assertThat(out.toByteArray()).isEqualTo(orig);
  }

  @Test
  public void testFromStringBinary() throws Exception {
    testBinary(STRING_BF, false);
  }

  private void testSlice(BinaryFactory bf, boolean reused) throws Exception {
    BinaryAndOriginal bao = bf.get(testString.getBytes(UTF8), reused);

    assertThat(bao.binary.slice(0, testString.length()).getBytesUnsafe()).isEqualTo(testString.getBytes(UTF8));
    assertThat(bao.binary.slice(5, 3).getBytesUnsafe()).isEqualTo("123".getBytes(UTF8));
  }

  private void testConstantCopy(BinaryFactory bf) throws Exception {
    BinaryAndOriginal bao = bf.get(testString.getBytes(UTF8), false);
    assertThat(bao.binary.isBackingBytesReused()).isFalse();

    assertThat(bao.binary.getBytes()).isEqualTo(testString.getBytes(UTF8));
    assertThat(bao.binary.getBytesUnsafe()).isEqualTo(testString.getBytes(UTF8));
    assertThat(bao.binary.copy().getBytesUnsafe()).isEqualTo(testString.getBytes(UTF8));
    assertThat(bao.binary.copy().getBytes()).isEqualTo(testString.getBytes(UTF8));

    bao = bf.get(testString.getBytes(UTF8), false);
    assertThat(bao.binary.isBackingBytesReused()).isFalse();

    Binary copy = bao.binary.copy();

    assertThat(bao.binary).isSameAs(copy);
  }

  private void testReusedCopy(BinaryFactory bf) throws Exception {
    BinaryAndOriginal bao = bf.get(testString.getBytes(UTF8), true);
    assertThat(bao.binary.isBackingBytesReused()).isTrue();

    assertThat(bao.binary.getBytes()).isEqualTo(testString.getBytes(UTF8));
    assertThat(bao.binary.getBytesUnsafe()).isEqualTo(testString.getBytes(UTF8));
    assertThat(bao.binary.copy().getBytesUnsafe()).isEqualTo(testString.getBytes(UTF8));
    assertThat(bao.binary.copy().getBytes()).isEqualTo(testString.getBytes(UTF8));

    bao = bf.get(testString.getBytes(UTF8), true);
    assertThat(bao.binary.isBackingBytesReused()).isTrue();

    Binary copy = bao.binary.copy();
    mutate(bao.original);

    assertThat(copy.getBytes()).isEqualTo(testString.getBytes(UTF8));
    assertThat(copy.getBytesUnsafe()).isEqualTo(testString.getBytes(UTF8));
    assertThat(copy.copy().getBytesUnsafe()).isEqualTo(testString.getBytes(UTF8));
    assertThat(copy.copy().getBytes()).isEqualTo(testString.getBytes(UTF8));
  }

  /**
   * Tests copy() on a constant (non-reused) direct ByteBuffer-backed Binary.
   * Unlike heap-backed binaries, copy() must return a new object because the direct
   * memory can be freed independently.
   */
  private void testDirectConstantCopy(BinaryFactory bf) throws Exception {
    BinaryAndOriginal bao = bf.get(testString.getBytes(UTF8), false);
    assertThat(bao.binary.isBackingBytesReused()).isFalse();

    assertThat(bao.binary.getBytes()).isEqualTo(testString.getBytes(UTF8));
    assertThat(bao.binary.getBytesUnsafe()).isEqualTo(testString.getBytes(UTF8));
    assertThat(bao.binary.copy().getBytesUnsafe()).isEqualTo(testString.getBytes(UTF8));
    assertThat(bao.binary.copy().getBytes()).isEqualTo(testString.getBytes(UTF8));

    bao = bf.get(testString.getBytes(UTF8), false);
    assertThat(bao.binary.isBackingBytesReused()).isFalse();

    Binary copy = bao.binary.copy();

    // Direct ByteBuffer-backed constant Binary.copy() must NOT return 'this'
    assertThat(bao.binary).isNotSameAs(copy);
    // But the data must be equal
    assertThat(copy).isEqualTo(bao.binary);
  }

  /**
   * Tests copy() on a reused direct ByteBuffer-backed Binary.
   * The copy must be fully independent and survive mutation of the original buffer.
   */
  private void testDirectReusedCopy(BinaryFactory bf) throws Exception {
    BinaryAndOriginal bao = bf.get(testString.getBytes(UTF8), true);
    assertThat(bao.binary.isBackingBytesReused()).isTrue();

    assertThat(bao.binary.getBytes()).isEqualTo(testString.getBytes(UTF8));
    assertThat(bao.binary.getBytesUnsafe()).isEqualTo(testString.getBytes(UTF8));
    assertThat(bao.binary.copy().getBytesUnsafe()).isEqualTo(testString.getBytes(UTF8));
    assertThat(bao.binary.copy().getBytes()).isEqualTo(testString.getBytes(UTF8));

    bao = bf.get(testString.getBytes(UTF8), true);
    assertThat(bao.binary.isBackingBytesReused()).isTrue();

    Binary copy = bao.binary.copy();
    assertThat(bao.binary).isNotSameAs(copy);

    assertThat(copy.getBytes()).isEqualTo(testString.getBytes(UTF8));
    assertThat(copy.getBytesUnsafe()).isEqualTo(testString.getBytes(UTF8));
    assertThat(copy.copy().getBytesUnsafe()).isEqualTo(testString.getBytes(UTF8));
    assertThat(copy.copy().getBytes()).isEqualTo(testString.getBytes(UTF8));
  }

  private void testSerializable(BinaryFactory bf, boolean reused) throws Exception {
    BinaryAndOriginal bao = bf.get("polygon".getBytes(UTF8), reused);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(baos);
    out.writeObject(bao.binary);
    out.close();
    baos.close();

    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
    Object object = in.readObject();
    assertThat(object).isInstanceOf(Binary.class);
    assertThat(object).isEqualTo(bao.binary);
  }

  private void testBinary(BinaryFactory bf, boolean reused) throws Exception {
    testSlice(bf, reused);

    if (reused) {
      testReusedCopy(bf);
    } else {
      testConstantCopy(bf);
    }

    testSerializable(bf, reused);
  }

  @Test
  public void testCompare() {
    Binary b1 = Binary.fromCharSequence("aaaaaaaa");
    Binary b2 = Binary.fromString("aaaaaaab");
    Binary b3 = Binary.fromReusedByteArray("aaaaaaaaaaa".getBytes(), 1, 8);
    Binary b4 = Binary.fromConstantByteBuffer(ByteBuffer.wrap("aaaaaaac".getBytes()));

    assertThat(b1).isLessThan(b2);
    assertThat(b2).isGreaterThan(b1);
    assertThat(b3).isLessThan(b4);
    assertThat(b4).isGreaterThan(b3);
    assertThat(b1).isLessThan(b4);
    assertThat(b4).isGreaterThan(b1);
    assertThat(b2).isLessThan(b4);
    assertThat(b4).isGreaterThan(b2);

    assertThat(b1).isEqualByComparingTo(b3);
    assertThat(b3).isEqualByComparingTo(b1);
  }

  @Test
  public void testGet2BytesLittleEndian() {
    // ByteBufferBackedBinary: get2BytesLittleEndian
    Binary b1 = Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[] {0x01, 0x02}));
    assertThat(b1.get2BytesLittleEndian()).isEqualTo((short) 0x0201);

    // ByteArrayBackedBinary: get2BytesLittleEndian
    Binary b2 = Binary.fromConstantByteArray(new byte[] {0x01, 0x02});
    assertThat(b2.get2BytesLittleEndian()).isEqualTo((short) 0x0201);

    // ByteArraySliceBackedBinary: get2BytesLittleEndian
    Binary b3 = Binary.fromConstantByteArray(new byte[] {0x00, 0x01, 0x02, 0x03}, 1, 2);
    assertThat(b3.get2BytesLittleEndian()).isEqualTo((short) 0x0201);
  }

  @Test
  public void testGet2BytesLittleEndianWrongLength() {
    // ByteBufferBackedBinary: get2BytesLittleEndian
    Binary b1 = Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[] {0x01, 0x02, 0x03}));
    assertThatThrownBy(() -> b1.get2BytesLittleEndian())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("length must be 2");

    // ByteArrayBackedBinary: get2BytesLittleEndian
    Binary b2 = Binary.fromConstantByteArray(new byte[] {0x01, 0x02, 0x03});
    assertThatThrownBy(() -> b2.get2BytesLittleEndian())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("length must be 2");

    // ByteArraySliceBackedBinary: get2BytesLittleEndian
    Binary b3 = Binary.fromConstantByteArray(new byte[] {0x00, 0x01, 0x02, 0x03}, 1, 3);
    assertThatThrownBy(() -> b3.get2BytesLittleEndian())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("length must be 2");
  }

  @Test
  public void testFromCharSequenceEncodesValidUtf8() {
    // Cover ASCII, multi-byte BMP, a supplementary code point (valid surrogate pair) and empty.
    assertFromCharSequenceEncodesUtf8("test-123-é中"); // ASCII + U+00E9 (2-byte) + U+4E2D (3-byte)
    assertFromCharSequenceEncodesUtf8("😀"); // U+1F600, valid surrogate pair (4-byte)
    assertFromCharSequenceEncodesUtf8(""); // empty
  }

  private static void assertFromCharSequenceEncodesUtf8(String value) {
    // fromCharSequence routes any CharSequence (here a StringBuilder) through FromCharSequenceBinary.
    // For valid input the strict encoder must match String#getBytes(UTF_8), so this is a genuine
    // cross-check, not a circular assertion.
    Binary binary = Binary.fromCharSequence(new StringBuilder(value));
    assertThat(binary.getBytes()).isEqualTo(value.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testFromCharSequenceRejectsMalformedUtf16() {
    // An unpaired high surrogate is invalid UTF-16. FromCharSequenceBinary must fail fast
    // rather than silently substituting a replacement byte (as String#getBytes(UTF_8) would).
    CharSequence value = new StringBuilder().append('a').append('\uD800').append('b');
    assertThatThrownBy(() -> Binary.fromCharSequence(value))
        .isInstanceOf(ParquetEncodingException.class)
        .hasMessage("Failed to encode CharSequence as UTF-8.");
  }
}
