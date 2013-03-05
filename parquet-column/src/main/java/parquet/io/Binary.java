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
package parquet.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import parquet.bytes.BytesUtils;

abstract public class Binary {

  public static final Binary EMPTY = new Binary() {
    @Override
    public String toStringUsingUTF8() {
      return "";
    }

    @Override
    public int length() {
      return 0;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
    }

    @Override
    public byte[] getBytes() {
      return new byte[0];
    }

  };

  public static Binary fromByteArray(
      final byte[] value,
      final int offset,
      final int length) {

    return new Binary() {
      @Override
      public String toStringUsingUTF8() {
        return new String(value, offset, length, BytesUtils.UTF8);
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

    };
  }

  public static Binary fromString(final String value) {
    return fromByteArray(value.getBytes(BytesUtils.UTF8));
  }

  abstract public String toStringUsingUTF8();

  abstract public int length();

  abstract public void writeTo(OutputStream out) throws IOException;

  abstract public byte[] getBytes();

}
