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
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.schema.PrimitiveComparator;

abstract public class Binary implements Comparable<Binary>, Serializable {

  protected boolean isBackingBytesReused;

  // this isn't really something others should extend
  private Binary() { }

  public static final Binary EMPTY = fromConstantByteArray(new byte[0]);

  abstract public String toStringUsingUTF8();

  abstract public int length();

  abstract public void writeTo(OutputStream out) throws IOException;

  abstract public void writeTo(DataOutput out) throws IOException;

  abstract public byte[] getBytes();

  /**
   * Variant of getBytes() that avoids copying backing data structure by returning
   * backing byte[] of the Binary. Do not modify backing byte[] unless you know what
   * you are doing.
   * @return backing byte[] of correct size, with an offset of 0, if possible, else returns result of getBytes()
   */
  abstract public byte[] getBytesUnsafe();

  abstract public Binary slice(int start, int length);

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
  abstract public int compareTo(Binary other);

  abstract int compareToLexicographic(Binary other);

  abstract int compareToLexicographic(byte[] other, int otherOffset, int otherLength);

  abstract int compareToLexicographic(ByteBuffer other, int otherOffset, int otherLength);

  abstract public ByteBuffer toByteBuffer();

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj instanceof Binary) {
      return equals((Binary)obj);
    }
    return false;
  }

  @Override
  public String toString() {
    return "Binary{" +
        length() +
        (isBackingBytesReused ? " reused": " constant") +
        " bytes, " +
        Arrays.toString(getBytesUnsafe())
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
          .decode(ByteBuffer.wrap(value, offset, length)).toString();
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
    int compareToLexicographic(Binary other) {
      // NOTE: We have to flip the sign, since we swap operands sides
      return -other.compareToLexicographic(value, offset, length);
    }

    @Override
    int compareToLexicographic(byte[] other, int otherOffset, int otherLength) {
      return Binary.compareToLexicographic(value, offset, length, other, otherOffset, otherLength);
    }

    @Override
    int compareToLexicographic(ByteBuffer other, int otherOffset, int otherLength) {
      return Binary.compareToLexicographic(value, offset, length, other, otherOffset, otherLength);
    }

    @Override
    public ByteBuffer toByteBuffer() {
      return ByteBuffer.wrap(value, offset, length);
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

    private static final ThreadLocal<CharsetEncoder> ENCODER =
      ThreadLocal.withInitial(StandardCharsets.UTF_8::newEncoder);

    private static ByteBuffer encodeUTF8(CharSequence value) {
      try {
        return ENCODER.get().encode(CharBuffer.wrap(value));
      } catch (CharacterCodingException e) {
        throw new ParquetEncodingException("UTF-8 not supported.", e);
      }
    }
  }

  public static Binary fromReusedByteArray(final byte[] value, final int offset, final int length) {
    return new ByteArraySliceBackedBinary(value, offset, length, true);
  }

  public static Binary fromConstantByteArray(final byte[] value, final int offset,
                                             final int length) {
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
    int compareToLexicographic(Binary other) {
      // NOTE: We have to flip the sign, since we swap operands sides
      return -other.compareToLexicographic(value, 0, value.length);
    }

    @Override
    int compareToLexicographic(byte[] other, int otherOffset, int otherLength) {
      return Binary.compareToLexicographic(this.value, 0, value.length, other, otherOffset, otherLength);
    }

    @Override
    int compareToLexicographic(ByteBuffer other, int otherOffset, int otherLength) {
      return Binary.compareToLexicographic(this.value, 0, value.length, other, otherOffset, otherLength);
    }

    @Override
    public ByteBuffer toByteBuffer() {
      return ByteBuffer.wrap(value);
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
        ret = new String(value.array(), value.arrayOffset() + offset, length,
            StandardCharsets.UTF_8);
      } else {
        int limit = value.limit();
        value.limit(offset+length);
        int position = value.position();
        value.position(offset);
        // no corresponding interface to read a subset of a buffer, would have to slice it
        // which creates another ByteBuffer object or do what is done here to adjust the
        // limit/offset and set them back after
        ret = StandardCharsets.UTF_8.decode(value).toString();
        value.limit(limit);
        value.position(position);
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

      int limit = value.limit();
      value.limit(offset + length);
      int position = value.position();
      value.position(offset);
      value.get(bytes);
      value.limit(limit);
      value.position(position);
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
        return Binary.equals(value.array(), value.arrayOffset() + offset, length, other, otherOffset, otherLength);
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
    int compareToLexicographic(Binary other) {
      if (value.hasArray()) {
        // NOTE: We have to flip the sign, since we swap operands sides
        return -other.compareToLexicographic(value.array(), value.arrayOffset() + offset, length);
      } else {
        // NOTE: We have to flip the sign, since we swap operands sides
        return -other.compareToLexicographic(value, offset, length);
      }
    }

    @Override
    int compareToLexicographic(byte[] other, int otherOffset, int otherLength) {
      if (value.hasArray()) {
        return Binary.compareToLexicographic(value.array(), value.arrayOffset() + offset, length, other, otherOffset, otherLength);
      } else {
        // NOTE: We have to flip the sign, since we swap operands sides
        return -Binary.compareToLexicographic(other, otherOffset, otherLength, value, offset, length);
      }
    }

    @Override
    int compareToLexicographic(ByteBuffer other, int otherOffset, int otherLength) {
      return Binary.compareToLexicographic(value, offset, length, other, otherOffset, otherLength);
    }

    @Override
    public ByteBuffer toByteBuffer() {
      ByteBuffer ret = value.duplicate();
      ret.position(offset);
      ret.limit(offset + length);
      return ret;
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

  public static int compareToLexicographic(Binary one, Binary other) {
    return one.compareToLexicographic(other);
  }

  /**
   * @see {@link Arrays#hashCode(byte[])}
   * @param array
   * @param offset
   * @param length
   * @return
   */
  private static final int hashCode(byte[] array, int offset, int length) {
    int result = 1;
    for (int i = offset; i < offset + length; i++) {
      byte b = array[i];
      result = 31 * result + b;
    }
    return result;
  }

  private static final int hashCode(ByteBuffer buf, int offset, int length) {
    int result = 1;
    for (int i = offset; i < offset + length; i++) {
      byte b = buf.get(i);
      result = 31 * result + b;
    }
    return result;
  }

  private static final boolean equals(ByteBuffer buf1, int offset1, int length1, ByteBuffer buf2, int offset2, int length2) {
    if (buf1 == null && buf2 == null) return true;
    if (buf1 == null || buf2 == null) return false;
    if (length1 != length2) return false;
    for (int i = 0; i < length1; i++) {
      if (buf1.get(i + offset1) != buf2.get(i + offset2)) {
        return false;
      }
    }
    return true;
  }

  private static final boolean equals(byte[] array1, int offset1, int length1, ByteBuffer buf, int offset2, int length2) {
    if (array1 == null && buf == null) return true;
    if (array1 == null || buf == null) return false;
    if (length1 != length2) return false;
    for (int i = 0; i < length1; i++) {
      if (array1[i + offset1] != buf.get(i + offset2)) {
        return false;
      }
    }
    return true;
  }

  /**
   * @see {@link Arrays#equals(byte[], byte[])}
   * @param array1
   * @param offset1
   * @param length1
   * @param array2
   * @param offset2
   * @param length2
   * @return
   */
  private static final boolean equals(byte[] array1, int offset1, int length1, byte[] array2, int offset2, int length2) {
    if (array1 == null && array2 == null) return true;
    if (array1 == null || array2 == null) return false;
    if (length1 != length2) return false;
    if (array1 == array2 && offset1 == offset2) return true;
    for (int i = 0; i < length1; i++) {
      if (array1[i + offset1] != array2[i + offset2]) {
        return false;
      }
    }
    return true;
  }

  private static final int compareToLexicographic(byte[] array1, int offset1, int length1, byte[] array2, int offset2, int length2) {
    if (array1 == null && array2 == null) return 0;
    if (array1 == null || array2 == null) return array1 != null ? 1 : -1;

    int minLen = Math.min(length1, length2);
    for (int i = 0; i < minLen; i++) {
      int res = unsignedCompare(array1[i + offset1], array2[i + offset2]);
      if (res != 0) {
        return res;
      }
    }

    return length1 - length2;
  }

  private static final int compareToLexicographic(byte[] array1, int offset1, int length1, ByteBuffer array2, int offset2, int length2) {
    if (array1 == null && array2 == null) return 0;
    if (array1 == null || array2 == null) return array1 != null ? 1 : -1;

    int minLen = Math.min(length1, length2);
    for (int i = 0; i < minLen; i++) {
      int res = unsignedCompare(array1[i + offset1], array2.get(i + offset2));
      if (res != 0) {
        return res;
      }
    }

    return length1 - length2;
  }

  private static final int compareToLexicographic(ByteBuffer array1, int offset1, int length1, ByteBuffer array2, int offset2, int length2) {
    if (array1 == null && array2 == null) return 0;
    if (array1 == null || array2 == null) return array1 != null ? 1 : -1;

    int minLen = Math.min(length1, length2);
    for (int i = 0; i < minLen; i++) {
      int res = unsignedCompare(array1.get(i + offset1), array2.get(i + offset2));
      if (res != 0) {
        return res;
      }
    }

    return length1 - length2;
  }

  private static int unsignedCompare(byte b1, byte b2) {
    return toUnsigned(b1) - toUnsigned(b2);
  }

  private static final int toUnsigned(byte b) {
    return b & 0xFF;
  }
}
