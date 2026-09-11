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

import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.schema.PrimitiveComparator;

public abstract class Binary implements Comparable<Binary>, Serializable {

  protected boolean isBackingBytesReused;

  // this isn't really something others should extend
  private Binary() {}

  public static final Binary EMPTY = fromConstantByteArray(new byte[0]);

  public abstract String toStringUsingUTF8();

  public abstract int length();

  public abstract void writeTo(OutputStream out) throws IOException;

  public abstract void writeTo(DataOutput out) throws IOException;

  public abstract byte[] getBytes();

  /**
   * Variant of getBytes() that avoids copying backing data structure by returning
   * backing byte[] of the Binary. Do not modify backing byte[] unless you know what
   * you are doing.
   *
   * @return backing byte[] of correct size, with an offset of 0, if possible, else returns result of getBytes()
   */
  public abstract byte[] getBytesUnsafe();

  public abstract Binary slice(int start, int length);

  abstract boolean equals(byte[] bytes, int offset, int length);

  abstract boolean equals(ByteBuffer bytes, int offset, int length);

  abstract boolean equals(Binary other);

  /**
   * @deprecated will be removed in 2.0.0. The comparison logic depends on the related logical type therefore this one
   * might not be correct. The {@link java.util.Comparator} implementation for the related type available at
   * {@link org.apache.parquet.schema.PrimitiveType#comparator} should be used instead.
   */
  @Override
  @Deprecated
  public abstract int compareTo(Binary other);

  abstract int lexicographicCompare(Binary other);

  abstract int lexicographicCompare(byte[] other, int otherOffset, int otherLength);

  abstract int lexicographicCompare(ByteBuffer other, int otherOffset, int otherLength);

  public abstract ByteBuffer toByteBuffer();

  public short get2BytesLittleEndian() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj instanceof Binary) {
      return equals((Binary) obj);
    }
    return false;
  }

  @Override
  public String toString() {
    return "Binary{" + length()
        + (isBackingBytesReused ? " reused" : " constant")
        + " bytes, "
        + Arrays.toString(getBytesUnsafe())
        + "}";
  }

  public Binary copy() {
    if (isBackingBytesReused) {
      return Binary.fromConstantByteArray(getBytes());
    } else {
      return this;
    }
  }

  /**
   * Signals if backing bytes are owned, and can be modified, by producer of the Binary
   *
   * @return if backing bytes are held on by producer of the Binary
   */
  public boolean isBackingBytesReused() {
    return isBackingBytesReused;
  }

  private static class ByteArraySliceBackedBinary extends Binary {
    private final byte[] value;
    private final int offset;
    private final int length;

    public ByteArraySliceBackedBinary(byte[] value, int offset, int length, boolean isBackingBytesReused) {
      this.value = value;
      this.offset = offset;
      this.length = length;
      this.isBackingBytesReused = isBackingBytesReused;
    }

    @Override
    public String toStringUsingUTF8() {
      // Charset#decode uses a thread-local decoder cache and is faster than
      // new String(...) which instantiates a new Decoder per invocation
      return StandardCharsets.UTF_8
          .decode(ByteBuffer.wrap(value, offset, length))
          .toString();
    }

    @Override
    public int length() {
      return length;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
      out.write(value, offset, length);
    }

    @Override
    public byte[] getBytes() {
      return Arrays.copyOfRange(value, offset, offset + length);
    }

    @Override
    public byte[] getBytesUnsafe() {
      // Backing array is larger than the slice used for this Binary.
      return getBytes();
    }

    @Override
    public Binary slice(int start, int length) {
      if (isBackingBytesReused) {
        return Binary.fromReusedByteArray(value, offset + start, length);
      } else {
        return Binary.fromConstantByteArray(value, offset + start, length);
      }
    }

    @Override
    public int hashCode() {
      return Binary.hashCode(value, offset, length);
    }

    @Override
    boolean equals(Binary other) {
      return other.equals(value, offset, length);
    }

    @Override
    boolean equals(byte[] other, int otherOffset, int otherLength) {
      return Binary.equals(value, offset, length, other, otherOffset, otherLength);
    }

    @Override
    boolean equals(ByteBuffer bytes, int otherOffset, int otherLength) {
      return Binary.equals(value, offset, length, bytes, otherOffset, otherLength);
    }

    @Override
    public int compareTo(Binary other) {
      return PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR.compare(this, other);
    }

    @Override
    int lexicographicCompare(Binary other) {
      // NOTE: We have to flip the sign, since we swap operands sides
      return -other.lexicographicCompare(value, offset, length);
    }

    @Override
    int lexicographicCompare(byte[] other, int otherOffset, int otherLength) {
      return Binary.lexicographicCompare(value, offset, length, other, otherOffset, otherLength);
    }

    @Override
    int lexicographicCompare(ByteBuffer other, int otherOffset, int otherLength) {
      return Binary.lexicographicCompare(value, offset, length, other, otherOffset, otherLength);
    }

    @Override
    public ByteBuffer toByteBuffer() {
      return ByteBuffer.wrap(value, offset, length);
    }

    @Override
    public short get2BytesLittleEndian() {
      if (length != 2) {
        throw new IllegalArgumentException("length must be 2");
      }

      return (short) (((value[offset + 1] & 0xff) << 8) | (value[offset] & 0xff));
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
      out.write(value, offset, length);
    }
  }

  private static class FromStringBinary extends ByteBufferBackedBinary {
    public FromStringBinary(String value) {
      // reused is false, because we do not hold on to the buffer after
      // conversion, and nobody else has a handle to it
      super(encodeUTF8(value), false);
    }

    @Override
    public String toString() {
      return "Binary{\"" + toStringUsingUTF8() + "\"}";
    }

    private static ByteBuffer encodeUTF8(String value) {
      return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }
  }

  private static class FromCharSequenceBinary extends ByteBufferBackedBinary {
    public FromCharSequenceBinary(CharSequence value) {
      // reused is false, because we do not hold on to the buffer after
      // conversion, and nobody else has a handle to it
      super(encodeUTF8(value), false);
    }

    @Override
    public String toString() {
      return "Binary{\"" + toStringUsingUTF8() + "\"}";
    }

    private static ByteBuffer encodeUTF8(CharSequence value) {
      try {
        // Use a fresh encoder per call rather than a static ThreadLocal initialized with a lambda
        // (UTF_8::newEncoder): that lambda's class is loaded by the application ClassLoader and can
        // keep it from being unloaded in long-lived pooled threads, leaking Metaspace (GH-3398).
        // The encoder also preserves strict CodingErrorAction.REPORT, so malformed UTF-16 fails
        // fast instead of being silently replaced (as String#getBytes(UTF_8) would).
        return StandardCharsets.UTF_8.newEncoder().encode(CharBuffer.wrap(value));
      } catch (CharacterCodingException e) {
        throw new ParquetEncodingException("Failed to encode CharSequence as UTF-8.", e);
      }
    }
  }

  public static Binary fromReusedByteArray(final byte[] value, final int offset, final int length) {
    return new ByteArraySliceBackedBinary(value, offset, length, true);
  }

  public static Binary fromConstantByteArray(final byte[] value, final int offset, final int length) {
    return new ByteArraySliceBackedBinary(value, offset, length, false);
  }

  @Deprecated
  /**
   * @deprecated Use @link{fromReusedByteArray} or @link{fromConstantByteArray} instead
   */
  public static Binary fromByteArray(final byte[] value, final int offset, final int length) {
    return fromReusedByteArray(value, offset, length); // Assume producer intends to reuse byte[]
  }

  private static class ByteArrayBackedBinary extends Binary {
    private final byte[] value;

    public ByteArrayBackedBinary(byte[] value, boolean isBackingBytesReused) {
      this.value = value;
      this.isBackingBytesReused = isBackingBytesReused;
    }

    @Override
    public String toStringUsingUTF8() {
      return StandardCharsets.UTF_8.decode(ByteBuffer.wrap(value)).toString();
    }

    @Override
    public int length() {
      return value.length;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
      out.write(value);
    }

    @Override
    public byte[] getBytes() {
      return Arrays.copyOfRange(value, 0, value.length);
    }

    @Override
    public byte[] getBytesUnsafe() {
      return value;
    }

    @Override
    public Binary slice(int start, int length) {
      if (isBackingBytesReused) {
        return Binary.fromReusedByteArray(value, start, length);
      } else {
        return Binary.fromConstantByteArray(value, start, length);
      }
    }

    @Override
    public int hashCode() {
      return Binary.hashCode(value, 0, value.length);
    }

    @Override
    boolean equals(Binary other) {
      return other.equals(value, 0, value.length);
    }

    @Override
    boolean equals(byte[] other, int otherOffset, int otherLength) {
      return Binary.equals(value, 0, value.length, other, otherOffset, otherLength);
    }

    @Override
    boolean equals(ByteBuffer bytes, int otherOffset, int otherLength) {
      return Binary.equals(value, 0, value.length, bytes, otherOffset, otherLength);
    }

    @Override
    public int compareTo(Binary other) {
      return PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR.compare(this, other);
    }

    @Override
    int lexicographicCompare(Binary other) {
      // NOTE: We have to flip the sign, since we swap operands sides
      return -other.lexicographicCompare(value, 0, value.length);
    }

    @Override
    int lexicographicCompare(byte[] other, int otherOffset, int otherLength) {
      return Binary.lexicographicCompare(this.value, 0, value.length, other, otherOffset, otherLength);
    }

    @Override
    int lexicographicCompare(ByteBuffer other, int otherOffset, int otherLength) {
      return Binary.lexicographicCompare(this.value, 0, value.length, other, otherOffset, otherLength);
    }

    @Override
    public ByteBuffer toByteBuffer() {
      return ByteBuffer.wrap(value);
    }

    @Override
    public short get2BytesLittleEndian() {
      if (value.length != 2) {
        throw new IllegalArgumentException("length must be 2");
      }

      return (short) (((value[1] & 0xff) << 8) | (value[0] & 0xff));
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
      out.write(value);
    }
  }

  public static Binary fromReusedByteArray(final byte[] value) {
    return new ByteArrayBackedBinary(value, true);
  }

  public static Binary fromConstantByteArray(final byte[] value) {
    return new ByteArrayBackedBinary(value, false);
  }

  @Deprecated
  /**
   * @deprecated Use @link{fromReusedByteArray} or @link{fromConstantByteArray} instead
   */
  public static Binary fromByteArray(final byte[] value) {
    return fromReusedByteArray(value); // Assume producer intends to reuse byte[]
  }

  private static class ByteBufferBackedBinary extends Binary {
    private ByteBuffer value;
    private transient byte[] cachedBytes;
    private int offset;
    private int length;

    public ByteBufferBackedBinary(ByteBuffer value, boolean isBackingBytesReused) {
      this.value = value;
      this.offset = value.position();
      this.length = value.remaining();
      this.isBackingBytesReused = isBackingBytesReused;
    }

    public ByteBufferBackedBinary(ByteBuffer value, int offset, int length, boolean isBackingBytesReused) {
      this.value = value;
      this.offset = offset;
      this.length = length;
      this.isBackingBytesReused = isBackingBytesReused;
    }

    @Override
    public String toStringUsingUTF8() {
      String ret;
      if (value.hasArray()) {
        ret = new String(value.array(), value.arrayOffset() + offset, length, StandardCharsets.UTF_8);
      } else {
        // Duplicate before adjusting position/limit so we never mutate the shared
        // buffer's own position: readBytes() may have already advanced it past
        // this value's range (e.g. lazily-consumed values in a repeated field),
        // and limit(offset + length) would otherwise silently clamp it backwards.
        ByteBuffer duplicate = value.duplicate();
        duplicate.position(offset);
        duplicate.limit(offset + length);
        ret = StandardCharsets.UTF_8.decode(duplicate).toString();
      }

      return ret;
    }

    @Override
    public int length() {
      return length;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
      if (value.hasArray()) {
        out.write(value.array(), value.arrayOffset() + offset, length);
      } else {
        out.write(getBytesUnsafe(), 0, length);
      }
    }

    @Override
    public byte[] getBytes() {
      byte[] bytes = new byte[length];

      if (value.hasArray()) {
        System.arraycopy(value.array(), value.arrayOffset() + offset, bytes, 0, length);
      } else {
        // Duplicate before adjusting position/limit so we never mutate the shared
        // buffer's own position: readBytes() may have already advanced it past
        // this value's range (e.g. lazily-consumed values in a repeated field),
        // and limit(offset + length) would otherwise silently clamp it backwards.
        ByteBuffer duplicate = value.duplicate();
        duplicate.position(offset);
        duplicate.limit(offset + length);
        duplicate.get(bytes);
      }
      if (!isBackingBytesReused) { // backing buffer might change
        cachedBytes = bytes;
      }
      return bytes;
    }

    @Override
    public byte[] getBytesUnsafe() {
      return cachedBytes != null ? cachedBytes : getBytes();
    }

    @Override
    public Binary slice(int start, int length) {
      return Binary.fromConstantByteArray(getBytesUnsafe(), start, length);
    }

    @Override
    public Binary copy() {
      if (value.isDirect()) {
        // Direct ByteBuffers may be backed by memory that can be freed independently, so always materialize to
        // a heap-backed copy to avoid use-after-free.
        return Binary.fromConstantByteArray(getBytes());
      }
      return super.copy();
    }

    @Override
    public int hashCode() {
      if (value.hasArray()) {
        return Binary.hashCode(value.array(), value.arrayOffset() + offset, length);
      } else {
        return Binary.hashCode(value, offset, length);
      }
    }

    @Override
    boolean equals(Binary other) {
      if (value.hasArray()) {
        return other.equals(value.array(), value.arrayOffset() + offset, length);
      } else {
        return other.equals(value, offset, length);
      }
    }

    @Override
    boolean equals(byte[] other, int otherOffset, int otherLength) {
      if (value.hasArray()) {
        return Binary.equals(
            value.array(), value.arrayOffset() + offset, length, other, otherOffset, otherLength);
      } else {
        return Binary.equals(other, otherOffset, otherLength, value, offset, length);
      }
    }

    @Override
    boolean equals(ByteBuffer otherBytes, int otherOffset, int otherLength) {
      return Binary.equals(value, 0, length, otherBytes, otherOffset, otherLength);
    }

    @Override
    public int compareTo(Binary other) {
      return PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR.compare(this, other);
    }

    @Override
    int lexicographicCompare(Binary other) {
      if (value.hasArray()) {
        // NOTE: We have to flip the sign, since we swap operands sides
        return -other.lexicographicCompare(value.array(), value.arrayOffset() + offset, length);
      } else {
        // NOTE: We have to flip the sign, since we swap operands sides
        return -other.lexicographicCompare(value, offset, length);
      }
    }

    @Override
    int lexicographicCompare(byte[] other, int otherOffset, int otherLength) {
      if (value.hasArray()) {
        return Binary.lexicographicCompare(
            value.array(), value.arrayOffset() + offset, length, other, otherOffset, otherLength);
      } else {
        // NOTE: We have to flip the sign, since we swap operands sides
        return -Binary.lexicographicCompare(other, otherOffset, otherLength, value, offset, length);
      }
    }

    @Override
    int lexicographicCompare(ByteBuffer other, int otherOffset, int otherLength) {
      return Binary.lexicographicCompare(value, offset, length, other, otherOffset, otherLength);
    }

    @Override
    public ByteBuffer toByteBuffer() {
      ByteBuffer ret = value.duplicate();
      ret.position(offset);
      ret.limit(offset + length);
      return ret;
    }

    @Override
    public short get2BytesLittleEndian() {
      if (length != 2) {
        throw new IllegalArgumentException("length must be 2");
      }

      return (short) (((value.get(offset + 1) & 0xff) << 8) | (value.get(offset) & 0xff));
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
      // TODO: should not have to materialize those bytes
      out.write(getBytesUnsafe());
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
      byte[] bytes = getBytesUnsafe();
      out.writeInt(bytes.length);
      out.write(bytes);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
      int length = in.readInt();
      byte[] bytes = new byte[length];
      in.readFully(bytes, 0, length);
      this.value = ByteBuffer.wrap(bytes);
      this.offset = 0;
      this.length = length;
    }

    private void readObjectNoData() throws ObjectStreamException {
      this.value = ByteBuffer.wrap(new byte[0]);
    }
  }

  public static Binary fromReusedByteBuffer(final ByteBuffer value, int offset, int length) {
    return new ByteBufferBackedBinary(value, offset, length, true);
  }

  public static Binary fromConstantByteBuffer(final ByteBuffer value, int offset, int length) {
    return new ByteBufferBackedBinary(value, offset, length, false);
  }

  public static Binary fromReusedByteBuffer(final ByteBuffer value) {
    return new ByteBufferBackedBinary(value, true);
  }

  public static Binary fromConstantByteBuffer(final ByteBuffer value) {
    return new ByteBufferBackedBinary(value, false);
  }

  @Deprecated
  /**
   * @deprecated Use @link{fromReusedByteBuffer} or @link{fromConstantByteBuffer} instead
   */
  public static Binary fromByteBuffer(final ByteBuffer value) {
    return fromReusedByteBuffer(value); // Assume producer intends to reuse byte[]
  }

  public static Binary fromString(String value) {
    return new FromStringBinary(value);
  }

  public static Binary fromCharSequence(CharSequence value) {
    return new FromCharSequenceBinary(value);
  }

  public static int lexicographicCompare(Binary one, Binary other) {
    return one.lexicographicCompare(other);
  }

  // ---------------------------------------------------------------------------
  // Byte comparison / hashing primitives.
  //
  // equals / lexicographicCompare / mismatch delegate to Arrays.* / ByteBuffer.mismatch
  // range overloads, which route through ArraysSupport.vectorizedMismatch --
  // an @IntrinsicCandidate helper HotSpot substitutes with a SIMD byte-scan
  // (SSE / AVX2 / NEON on modern hardware). Compared to the previous hand-rolled
  // scalar byte loops, measured throughput on this project's BinaryComparisonBenchmark
  // (JDK 17) is roughly 3-4x for 64- and 512-byte values and ~1.1-1.7x for 8-byte
  // values, where the intrinsic barely wins. These primitives sit on statistics
  // min/max maintenance, dictionary hash-map probing, predicate evaluation, and
  // bloom-filter build, so the win is broad.
  //
  // hashCode is a different story. The 31*h+b polynomial has a serial dependency
  // across iterations, so SIMD needs a lane-split algebraic trick. HotSpot only
  // gained that intrinsic in JDK 21 (via ArraysSupport.vectorizedHashCode, which
  // is @IntrinsicCandidate). On JDK 17 -- this project's target -- Arrays.hashCode
  // is a plain scalar loop and delivers no measured speedup over the hand-rolled
  // version it replaces. The call is kept for forward compatibility: JDK 21+
  // runtimes pick up the vectorized intrinsic silently, whereas a bespoke loop
  // would stay stuck at scalar forever.
  //
  // Semantics are preserved bit-for-bit:
  //   - hashCode(byte[]) uses the same 31*h + b polynomial as before
  //     (that's what the JDK's own Arrays.hashCode computes).
  //   - equals is bytewise identity.
  //   - lexicographicCompare is unsigned bytewise, with shorter-runs-first
  //     tie-break on prefix match, matching Arrays.compareUnsigned.
  // ---------------------------------------------------------------------------

  private static final int hashCode(byte[] array, int offset, int length) {
    // Route full-array hashCode through Arrays.hashCode. On JDK 17 that is a
    // plain scalar loop; on JDK 21+ it delegates to the vectorized intrinsic
    // ArraysSupport.vectorizedHashCode. The slice path below reproduces the
    // same 31*h+b polynomial exactly.
    if (offset == 0 && length == array.length) {
      return Arrays.hashCode(array);
    }
    int result = 1;
    int end = offset + length;
    for (int i = offset; i < end; i++) {
      result = 31 * result + array[i];
    }
    return result;
  }

  private static final int hashCode(ByteBuffer buf, int offset, int length) {
    // If the buffer is heap-backed, fall through to the byte[] path so JDK 21+
    // can pick up the vectorized hashCode intrinsic.
    if (buf.hasArray()) {
      return hashCode(buf.array(), buf.arrayOffset() + offset, length);
    }
    int result = 1;
    int end = offset + length;
    for (int i = offset; i < end; i++) {
      result = 31 * result + buf.get(i);
    }
    return result;
  }

  private static final boolean equals(
      ByteBuffer buf1, int offset1, int length1, ByteBuffer buf2, int offset2, int length2) {
    if (buf1 == null && buf2 == null) return true;
    if (buf1 == null || buf2 == null) return false;
    if (length1 != length2) return false;
    // Fast-path both heap-backed: use vectorized Arrays.equals on the underlying arrays.
    if (buf1.hasArray() && buf2.hasArray()) {
      final int o1 = buf1.arrayOffset() + offset1;
      final int o2 = buf2.arrayOffset() + offset2;
      return Arrays.equals(buf1.array(), o1, o1 + length1, buf2.array(), o2, o2 + length2);
    }
    // ByteBuffer.mismatch is intrinsified on JDK 11+ and delegates to
    // ArraysSupport.vectorizedMismatch; use it via sliced views so it applies
    // to non-array-backed (direct / read-only) buffers as well.
    ByteBuffer s1 = slice(buf1, offset1, length1);
    ByteBuffer s2 = slice(buf2, offset2, length2);
    return s1.mismatch(s2) < 0;
  }

  private static final boolean equals(
      byte[] array1, int offset1, int length1, ByteBuffer buf, int offset2, int length2) {
    if (array1 == null && buf == null) return true;
    if (array1 == null || buf == null) return false;
    if (length1 != length2) return false;
    if (buf.hasArray()) {
      final int o2 = buf.arrayOffset() + offset2;
      return Arrays.equals(array1, offset1, offset1 + length1, buf.array(), o2, o2 + length2);
    }
    for (int i = 0; i < length1; i++) {
      if (array1[i + offset1] != buf.get(i + offset2)) {
        return false;
      }
    }
    return true;
  }

  /**
   * @see Arrays#equals(byte[], int, int, byte[], int, int)
   */
  private static final boolean equals(
      byte[] array1, int offset1, int length1, byte[] array2, int offset2, int length2) {
    if (array1 == null && array2 == null) return true;
    if (array1 == null || array2 == null) return false;
    // Arrays.equals(byte[], int, int, byte[], int, int) delegates to
    // ArraysSupport.mismatch, which routes through the @IntrinsicCandidate
    // vectorizedMismatch helper HotSpot substitutes with a SIMD byte-scan.
    return Arrays.equals(array1, offset1, offset1 + length1, array2, offset2, offset2 + length2);
  }

  private static final int lexicographicCompare(
      byte[] array1, int offset1, int length1, byte[] array2, int offset2, int length2) {
    if (array1 == null && array2 == null) return 0;
    if (array1 == null || array2 == null) return array1 != null ? 1 : -1;
    // Arrays.compareUnsigned routes through the same ArraysSupport.vectorizedMismatch
    // intrinsic: a SIMD mismatch scan, then an unsigned compare on the mismatching
    // pair, with shorter-first tie-break on a full prefix match -- semantically
    // identical to the previous hand-rolled loop.
    return Arrays.compareUnsigned(array1, offset1, offset1 + length1, array2, offset2, offset2 + length2);
  }

  private static final int lexicographicCompare(
      byte[] array, int offset1, int length1, ByteBuffer buffer, int offset2, int length2) {
    if (array == null && buffer == null) return 0;
    if (array == null || buffer == null) return array != null ? 1 : -1;
    if (buffer.hasArray()) {
      final int o2 = buffer.arrayOffset() + offset2;
      return Arrays.compareUnsigned(array, offset1, offset1 + length1, buffer.array(), o2, o2 + length2);
    }
    // Compare via ByteBuffer.mismatch to find the first differing position.
    ByteBuffer left = ByteBuffer.wrap(array, offset1, length1).slice();
    ByteBuffer right = slice(buffer, offset2, length2);
    int mm = left.mismatch(right);
    if (mm < 0) return length1 - length2;
    if (mm >= length1) return -1;
    if (mm >= length2) return 1;
    return (array[offset1 + mm] & 0xFF) - (buffer.get(offset2 + mm) & 0xFF);
  }

  private static final int lexicographicCompare(
      ByteBuffer buffer1, int offset1, int length1, ByteBuffer buffer2, int offset2, int length2) {
    if (buffer1 == null && buffer2 == null) return 0;
    if (buffer1 == null || buffer2 == null) return buffer1 != null ? 1 : -1;
    if (buffer1.hasArray() && buffer2.hasArray()) {
      final int o1 = buffer1.arrayOffset() + offset1;
      final int o2 = buffer2.arrayOffset() + offset2;
      return Arrays.compareUnsigned(buffer1.array(), o1, o1 + length1, buffer2.array(), o2, o2 + length2);
    }
    ByteBuffer left = slice(buffer1, offset1, length1);
    ByteBuffer right = slice(buffer2, offset2, length2);
    int mm = left.mismatch(right);
    if (mm < 0) return length1 - length2;
    if (mm >= length1) return -1;
    if (mm >= length2) return 1;
    return (buffer1.get(offset1 + mm) & 0xFF) - (buffer2.get(offset2 + mm) & 0xFF);
  }

  /** Return a sliced view of {@code buf} covering {@code [offset, offset+length)} without mutating {@code buf}. */
  private static ByteBuffer slice(ByteBuffer buf, int offset, int length) {
    ByteBuffer dup = buf.duplicate();
    dup.position(offset).limit(offset + length);
    return dup.slice();
  }
}
