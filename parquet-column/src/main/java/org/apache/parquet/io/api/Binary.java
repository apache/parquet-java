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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.parquet.io.ParquetEncodingException;

import static org.apache.parquet.bytes.BytesUtils.UTF8;

abstract public class Binary implements Comparable<Binary>, Serializable {

  private boolean isUnmodifiable;

  // this isn't really something others should extend
  private Binary() { }

  public static final Binary EMPTY = fromUnmodifiedByteArray(new byte[0]);

  abstract public String toStringUsingUTF8();

  abstract public int length();

  abstract public void writeTo(OutputStream out) throws IOException;

  abstract public void writeTo(DataOutput out) throws IOException;

  abstract public byte[] getBytes();

  /**
   * Variant of getBytes() that avoids copying backing data structure
   * @return backing byte[] if possible, else returns result of getBytes()
   */
  abstract public byte[] getBytesUnsafe();

  abstract boolean equals(byte[] bytes, int offset, int length);

  abstract boolean equals(Binary other);

  abstract public int compareTo(Binary other);

  abstract int compareTo(byte[] bytes, int offset, int length);

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
    return "Binary{" + length() + " bytes, " + Arrays.toString(getBytesUnsafe()) + "}";
  }

  public Binary copy() {
    if (isUnmodifiable) {
      return this;
    } else {
      return Binary.fromReusedByteArray(getBytes());
    }
  }

  protected void setUnmodifiable(boolean isUnmodifiable) {
    this.isUnmodifiable = isUnmodifiable;
  }

  private static class ByteArraySliceBackedBinary extends Binary {
    private final byte[] value;
    private final int offset;
    private final int length;

    public ByteArraySliceBackedBinary(byte[] value, int offset, int length, boolean isUnmodifiable) {
      this.value = value;
      this.offset = offset;
      this.length = length;
      setUnmodifiable(isUnmodifiable);
    }

    @Override
    public String toStringUsingUTF8() {
      return UTF8.decode(ByteBuffer.wrap(value, offset, length)).toString();
      // TODO: figure out why the following line was much slower
      // rdb: new String(...) is slower because it instantiates a new Decoder,
      //      while Charset#decode uses a thread-local decoder cache
      // return new String(value, offset, length, BytesUtils.UTF8);
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
      return getBytes();
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
    public int compareTo(Binary other) {
      return other.compareTo(value, offset, length);
    }

    @Override
    int compareTo(byte[] other, int otherOffset, int otherLength) {
      return Binary.compareTwoByteArrays(value, offset, length, other, otherOffset, otherLength);
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

  private static class FromStringBinary extends ByteArrayBackedBinary {
    public FromStringBinary(byte[] value) {
      super(value, true);
    }

    @Override
    public String toString() {
      return "Binary{\"" + toStringUsingUTF8() + "\"}";
    }
  }

  private static Binary fromByteArray(final byte[] value, final int offset, final int length,
                                      final boolean isUnmodifiable) {
    return new ByteArraySliceBackedBinary(value, offset, length, isUnmodifiable);
  }

  public static Binary fromReusedByteArray(final byte[] value, final int offset, final int length) {
    return fromByteArray(value, offset, length, false);
  }

  public static Binary fromUnmodifiedByteArray(final byte[] value, final int offset,
                                               final int length) {
    return fromByteArray(value, offset, length, true);
  }

  private static class ByteArrayBackedBinary extends Binary {
    private final byte[] value;

    public ByteArrayBackedBinary(byte[] value, boolean isUnmodifiable) {
      this.value = value;
      setUnmodifiable(isUnmodifiable);
    }

    @Override
    public String toStringUsingUTF8() {
      return UTF8.decode(ByteBuffer.wrap(value)).toString();
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
    public int compareTo(Binary other) {
      return other.compareTo(value, 0, value.length);
    }

    @Override
    int compareTo(byte[] other, int otherOffset, int otherLength) {
      return Binary.compareTwoByteArrays(value, 0, value.length, other, otherOffset, otherLength);
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

  private static Binary fromByteArray(final byte[] value, final boolean isUnmodifiable) {
    return new ByteArrayBackedBinary(value, isUnmodifiable);
  }

  public static Binary fromReusedByteArray(final byte[] value) {
    return fromByteArray(value, false);
  }

  public static Binary fromUnmodifiedByteArray(final byte[] value) {
    return fromByteArray(value, true);
  }

  private static class ByteBufferBackedBinary extends Binary {
    private transient ByteBuffer value;

    public ByteBufferBackedBinary(ByteBuffer value, boolean isUnmodifiable) {
      this.value = value;
      setUnmodifiable(isUnmodifiable);
    }

    @Override
    public String toStringUsingUTF8() {
      return UTF8.decode(value).toString();
    }

    @Override
    public int length() {
      return value.remaining();
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
      // TODO: should not have to materialize those bytes
      out.write(getBytesUnsafe());
    }

    @Override
    public byte[] getBytes() {
      byte[] bytes = new byte[value.remaining()];

      value.mark();
      value.get(bytes).reset();
      return bytes;
    }

    @Override
    public byte[] getBytesUnsafe() {
      return getBytes();
    }

    @Override
    public int hashCode() {
      if (value.hasArray()) {
        return Binary.hashCode(value.array(), value.arrayOffset() + value.position(),
            value.arrayOffset() + value.remaining());
      }
      byte[] bytes = getBytesUnsafe();
      return Binary.hashCode(bytes, 0, bytes.length);
    }

    @Override
    boolean equals(Binary other) {
      if (value.hasArray()) {
        return other.equals(value.array(), value.arrayOffset() + value.position(),
            value.arrayOffset() + value.remaining());
      }
      byte[] bytes = getBytesUnsafe();
      return other.equals(bytes, 0, bytes.length);
    }

    @Override
    boolean equals(byte[] other, int otherOffset, int otherLength) {
      if (value.hasArray()) {
        return Binary.equals(value.array(), value.arrayOffset() + value.position(),
            value.arrayOffset() + value.remaining(), other, otherOffset, otherLength);
      }
      byte[] bytes = getBytesUnsafe();
      return Binary.equals(bytes, 0, bytes.length, other, otherOffset, otherLength);
    }

    @Override
    public int compareTo(Binary other) {
      if (value.hasArray()) {
        return other.compareTo(value.array(), value.arrayOffset() + value.position(),
            value.arrayOffset() + value.remaining());
      }
      byte[] bytes = getBytesUnsafe();
      return other.compareTo(bytes, 0, bytes.length);
    }

    @Override
    int compareTo(byte[] other, int otherOffset, int otherLength) {
      if (value.hasArray()) {
        return Binary.compareTwoByteArrays(value.array(), value.arrayOffset() + value.position(),
            value.arrayOffset() + value.remaining(), other, otherOffset, otherLength);
      }
      byte[] bytes = getBytesUnsafe();
      return Binary.compareTwoByteArrays(bytes, 0, bytes.length, other, otherOffset, otherLength);
    }

    @Override
    public ByteBuffer toByteBuffer() {
      return value;
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
    }

    private void readObjectNoData() throws ObjectStreamException {
      this.value = ByteBuffer.wrap(new byte[0]);
    }

  }

  private static Binary fromByteBuffer(final ByteBuffer value, final boolean isUnmodifiable) {
    return new ByteBufferBackedBinary(value, isUnmodifiable);
  }

  public static Binary fromReusedByteBuffer(final ByteBuffer value) {
    return fromByteBuffer(value, false);
  }

  public static Binary fromUnmodifiedByteBuffer(final ByteBuffer value) {
    return fromByteBuffer(value, true);
  }

  public static Binary fromString(final String value) {
    try {
      return new FromStringBinary(value.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new ParquetEncodingException("UTF-8 not supported.", e);
    }
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

  private static final int compareTwoByteArrays(byte[] array1, int offset1, int length1,
                                                byte[] array2, int offset2, int length2) {
    if (array1 == null && array2 == null) return 0;
    if (array1 == array2 && offset1 == offset2 && length1 == length2) return 0;
    int min_length = (length1 < length2) ? length1 : length2;
    for (int i = 0; i < min_length; i++) {
      if (array1[i + offset1] < array2[i + offset2]) {
        return 1;
      }
      if (array1[i + offset1] > array2[i + offset2]) {
        return -1;
      }
    }
    // check remainder
    if (length1 == length2) { return 0; }
    else if (length1 < length2) { return 1;}
    else { return -1; }
  }
}
