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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.parquet.io.api.TestBinary.BinaryFactory.BinaryAndOriginal;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

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
      byte [] orig = padded(bytes);
      Binary b;
      if (reused) {
        b = Binary.fromReusedByteArray(orig, 5, bytes.length);
      } else {
        b = Binary.fromConstantByteArray(orig, 5, bytes.length);
      }
      assertArrayEquals(bytes, b.getBytes());
      return new BinaryAndOriginal(b, orig);
    }
  };

  private static final BinaryFactory BUFFER_BF = new BinaryFactory() {
    @Override
    public BinaryAndOriginal get(byte[] bytes, boolean reused) throws Exception {
      byte [] orig = padded(bytes);
      ByteBuffer buff = ByteBuffer.wrap(orig, 5, bytes.length);
      Binary b;

      if (reused) {
        b = Binary.fromReusedByteBuffer(buff);
      } else {
        b = Binary.fromConstantByteBuffer(buff);
      }

      buff.mark();
      assertArrayEquals(bytes, b.getBytes());
      buff.reset();
      return new BinaryAndOriginal(b, orig);
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
  public void testFromStringBinary() throws Exception {
    testBinary(STRING_BF, false);
  }

  private void testSlice(BinaryFactory bf, boolean reused) throws Exception {
    BinaryAndOriginal bao = bf.get(testString.getBytes(UTF8), reused);

    assertArrayEquals(testString.getBytes(UTF8), bao.binary.slice(0, testString.length()).getBytesUnsafe());
    assertArrayEquals("123".getBytes(UTF8), bao.binary.slice(5, 3).getBytesUnsafe());
  }

  private void testConstantCopy(BinaryFactory bf) throws Exception {
    BinaryAndOriginal bao = bf.get(testString.getBytes(UTF8), false);
    assertEquals(false, bao.binary.isBackingBytesReused());

    assertArrayEquals(testString.getBytes(UTF8), bao.binary.getBytes());
    assertArrayEquals(testString.getBytes(UTF8), bao.binary.getBytesUnsafe());
    assertArrayEquals(testString.getBytes(UTF8), bao.binary.copy().getBytesUnsafe());
    assertArrayEquals(testString.getBytes(UTF8), bao.binary.copy().getBytes());

    bao = bf.get(testString.getBytes(UTF8), false);
    assertEquals(false, bao.binary.isBackingBytesReused());

    Binary copy = bao.binary.copy();

    assertSame(copy, bao.binary);
  }

  private void testReusedCopy(BinaryFactory bf) throws Exception {
    BinaryAndOriginal bao = bf.get(testString.getBytes(UTF8), true);
    assertEquals(true, bao.binary.isBackingBytesReused());

    assertArrayEquals(testString.getBytes(UTF8), bao.binary.getBytes());
    assertArrayEquals(testString.getBytes(UTF8), bao.binary.getBytesUnsafe());
    assertArrayEquals(testString.getBytes(UTF8), bao.binary.copy().getBytesUnsafe());
    assertArrayEquals(testString.getBytes(UTF8), bao.binary.copy().getBytes());

    bao = bf.get(testString.getBytes(UTF8), true);
    assertEquals(true, bao.binary.isBackingBytesReused());

    Binary copy = bao.binary.copy();
    mutate(bao.original);

    assertArrayEquals(testString.getBytes(UTF8), copy.getBytes());
    assertArrayEquals(testString.getBytes(UTF8), copy.getBytesUnsafe());
    assertArrayEquals(testString.getBytes(UTF8), copy.copy().getBytesUnsafe());
    assertArrayEquals(testString.getBytes(UTF8), copy.copy().getBytes());
  }

  private void testSerializable(BinaryFactory bf, boolean reused) throws Exception {
    BinaryAndOriginal bao = bf.get("polygon".getBytes(UTF8), reused);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(baos);
    out.writeObject(bao.binary);
    out.close();
    baos.close();

    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(
        baos.toByteArray()));
    Object object = in.readObject();
    assertTrue(object instanceof Binary);
    assertEquals(bao.binary, object);
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
}
