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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import parquet.bytes.BytesUtils;

abstract public class Binary {

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
      public ByteBuffer toByteBuffer() {
        return ByteBuffer.wrap(value, offset, length);
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
      public ByteBuffer toByteBuffer() {
        return ByteBuffer.wrap(value);
      }
    };
  }

  public static Binary fromString(final String value) {
    return fromByteArray(value.getBytes(BytesUtils.UTF8));
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
    if (array1 == array2) return true;
    if (array1 == null || array2 == null) return false;
    if (length1 != length2) return false;
    for (int i = 0; i < length1; i++) {
      if (array1[i + offset1] != array2[i + offset2]) {
        return false;
      }
    }
    return true;
  }

  abstract public String toStringUsingUTF8();

  abstract public int length();

  abstract public void writeTo(OutputStream out) throws IOException;

  abstract public byte[] getBytes();

  abstract boolean equals(byte[] bytes, int offset, int length);

  abstract boolean equals(Binary other);

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

}
