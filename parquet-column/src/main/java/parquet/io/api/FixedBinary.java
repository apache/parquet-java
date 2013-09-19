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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Hackish wrapper around Binary.
 */
public class FixedBinary {
  private Binary binary;

  public static FixedBinary fromByteArray(final byte[] value, final int offset,
                                          final int length) {
    return new FixedBinary(value, offset, length);
  }

  public static FixedBinary fromByteArray(final byte[] value) {
    return new FixedBinary(value);
  }

  public static FixedBinary fromByteBuffer(final ByteBuffer value) {
    return new FixedBinary(value);
  }

  public static FixedBinary fromString(final String value) {
    return new FixedBinary(value);
  }

  public FixedBinary(final byte[] value, final int offset, final int length) {
    this(Binary.fromByteArray(value, offset, length));
  }

  public FixedBinary(final byte[] value) {
    this(Binary.fromByteArray(value));
  }

  public FixedBinary(final ByteBuffer value) {
    this(Binary.fromByteBuffer(value));
  }

  public FixedBinary(final String value) {
    this(Binary.fromString(value));
  }

  public FixedBinary(Binary binary) {
    this.binary = binary;
  }

  public Binary getBinary() {
    return binary;
  }

  public String toStringUsingUTF8() {
    return binary.toStringUsingUTF8();
  }

  public int length() {
    return binary.length();
  }

  public void writeTo(OutputStream out) throws IOException {
    binary.writeTo(out);
  }
  
  public byte[] getBytes() {
    return binary.getBytes();
  }

  @Override
  public int hashCode() {
    return binary.hashCode();
  }

  public boolean equals(byte[] bytes, int offset, int length) {
    return binary.equals(bytes, offset, length);
  }

  public boolean equals(FixedBinary other) {
    return binary.equals(other.getBinary());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj instanceof FixedBinary) {
      return equals((FixedBinary) obj);
    }
    return false;
  }

  public ByteBuffer toByteBuffer() {
    return binary.toByteBuffer();
  }

  public String toString() {
    return "FixedBinary{" + binary.length() + " bytes, " + 
        binary.toStringUsingUTF8() + "}";
  }
}
