/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.io.api;

import static parquet.bytes.BytesUtils.UTF8;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import parquet.bytes.BytesUtils;
import parquet.io.ParquetEncodingException;

abstract public class Binary implements Comparable<Binary> {

  public static final Binary EMPTY = fromByteArray(new byte[0]);

  public static Binary fromByteArray(
      final byte[] value,
      final int offset,
      final int length) {

    return new Binary() {
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

    };
  }

  public static Binary fromByteArray(final byte[] value) {
    return new Binary() {
      @Override
      public String toStringUsingUTF8() {
        return new String(value, BytesUtils.UTF8);
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
    };
  }

  public static Binary fromByteBuffer(final ByteBuffer value) {
    return new Binary() {
      @Override
      public String toStringUsingUTF8() {
        return new String(getBytes(), BytesUtils.UTF8);
      }

      @Override
      public int length() {
        return value.remaining();
      }

      @Override
      public void writeTo(OutputStream out) throws IOException {
        // TODO: should not have to materialize those bytes
        out.write(getBytes());
      }

      @Override
      public byte[] getBytes() {
        byte[] bytes = new byte[value.remaining()];

        value.mark();
        value.get(bytes).reset();
        return bytes;
      }

      @Override
      public int hashCode() {
        if (value.hasArray()) {
          return Binary.hashCode(value.array(), value.arrayOffset() + value.position(),
              value.arrayOffset() + value.remaining());
        }
        byte[] bytes = getBytes();
        return Binary.hashCode(bytes, 0, bytes.length);
      }

      @Override
      boolean equals(Binary other) {
        if (value.hasArray()) {
          return other.equals(value.array(), value.arrayOffset() + value.position(),
              value.arrayOffset() + value.remaining());
        }
        byte[] bytes = getBytes();
        return other.equals(bytes, 0, bytes.length);
      }

      @Override
      boolean equals(byte[] other, int otherOffset, int otherLength) {
        if (value.hasArray()) {
          return Binary.equals(value.array(), value.arrayOffset() + value.position(),
              value.arrayOffset() + value.remaining(), other, otherOffset, otherLength);
        }
        byte[] bytes = getBytes();
        return Binary.equals(bytes, 0, bytes.length, other, otherOffset, otherLength);
      }

      @Override
      public int compareTo(Binary other) {
        if (value.hasArray()) {
          return other.compareTo(value.array(), value.arrayOffset() + value.position(),
              value.arrayOffset() + value.remaining());
        }
        byte[] bytes = getBytes();
        return other.compareTo(bytes, 0, bytes.length);
      }

      @Override
      int compareTo(byte[] other, int otherOffset, int otherLength) {
        if (value.hasArray()) {
          return Binary.compareTwoByteArrays(value.array(), value.arrayOffset() + value.position(),
              value.arrayOffset() + value.remaining(), other, otherOffset, otherLength);
        }
        byte[] bytes = getBytes();
        return Binary.compareTwoByteArrays(bytes, 0, bytes.length, other, otherOffset, otherLength);
      }

      @Override
      public ByteBuffer toByteBuffer() {
        return value;
      }

      @Override
      public void writeTo(DataOutput out) throws IOException {
        // TODO: should not have to materialize those bytes
        out.write(getBytes());
      }
    };
  }

  public static Binary fromString(final String value) {
    try {
      return fromByteArray(value.getBytes("UTF-8"));
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

  abstract public String toStringUsingUTF8();

  abstract public int length();

  abstract public void writeTo(OutputStream out) throws IOException;

  abstract public void writeTo(DataOutput out) throws IOException;

  abstract public byte[] getBytes();

  abstract boolean equals(byte[] bytes, int offset, int length);

  abstract boolean equals(Binary other);

  abstract public int compareTo(Binary other);

  abstract int compareTo(byte[] bytes, int offset, int length);

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

  abstract public ByteBuffer toByteBuffer();

  public String toString() {
    return "Binary{" + length() + " bytes, " + Arrays.toString(getBytes()) + "}";
  };
}
