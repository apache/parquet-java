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
package org.apache.parquet.variant;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Consumer;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestVariantArray {
  private static final Logger LOG = LoggerFactory.getLogger(TestVariantArray.class);
  private static final String RANDOM_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

  /** Random number generator for generating random strings */
  private static SecureRandom random = new SecureRandom(new byte[] {1, 2, 3, 4, 5});

  private static final ByteBuffer EMPTY_METADATA = ByteBuffer.wrap(new byte[] {0b1});

  private static final byte[] VALUE_NULL = new byte[] {primitiveHeader(0)};
  private static final byte[] VALUE_BOOL = new byte[] {primitiveHeader(1)};
  private static final byte[] VALUE_INT = new byte[] {primitiveHeader(5), (byte) 0xD2, 0x02, (byte) 0x96, 0x49};
  private static final byte[] VALUE_STRING =
      new byte[] {primitiveHeader(16), 0x07, 0x00, 0x00, 0x00, 'v', 'a', 'r', 'i', 'a', 'n', 't'};
  private static final byte[] VALUE_SHORT_STRING = new byte[] {0b101, 'c'};
  private static final byte[] VALUE_DATE = new byte[] {0b101100, (byte) 0xE3, 0x4E, 0x00, 0x00};

  private void checkType(Variant v, int expectedBasicType, Variant.Type expectedType) {
    Assert.assertEquals(expectedBasicType, v.value.get(v.value.position()) & VariantUtil.BASIC_TYPE_MASK);
    Assert.assertEquals(expectedType, v.getType());
  }

  private String randomString(int len) {
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      sb.append(RANDOM_CHARS.charAt(random.nextInt(RANDOM_CHARS.length())));
    }
    return sb.toString();
  }

  private void testVariant(Variant v, Consumer<Variant> consumer) {
    consumer.accept(v);
    // Create new Variant with different byte offsets
    byte[] newValue = new byte[v.value.capacity() + 50];
    byte[] newMetadata = new byte[v.metadata.capacity() + 50];
    Arrays.fill(newValue, (byte) 0xFF);
    Arrays.fill(newMetadata, (byte) 0xFF);
    v.value.position(0);
    v.value.get(newValue, 25, v.value.capacity());
    v.value.position(0);
    v.metadata.position(0);
    v.metadata.get(newMetadata, 25, v.metadata.capacity());
    v.metadata.position(0);
    Variant v2 = new Variant(
        ByteBuffer.wrap(newValue, 25, v.value.capacity()),
        ByteBuffer.wrap(newMetadata, 25, v.metadata.capacity()));
    consumer.accept(v2);
  }

  private static byte primitiveHeader(int type) {
    return (byte) (type << 2);
  }

  private static int getMinIntegerSize(int value) {
    return (value <= 0xFF) ? 1 : (value <= 0xFFFF) ? 2 : (value <= 0xFFFFFF) ? 3 : 4;
  }

  private static void writeVarlenInt(ByteBuffer buffer, int value, int valueSize) {
    if (valueSize == 1) {
      buffer.put((byte) value);
    } else if (valueSize == 2) {
      buffer.putShort((short) value);
    } else if (valueSize == 3) {
      buffer.put((byte) (value & 0xFF));
      buffer.put((byte) ((value >> 8) & 0xFF));
      buffer.put((byte) ((value >> 16) & 0xFF));
    } else {
      buffer.putInt(value);
    }
  }

  private static byte[] constructString(String value) {
    return ByteBuffer.allocate(value.length() + 5)
        .order(ByteOrder.LITTLE_ENDIAN)
        .put(primitiveHeader(16))
        .putInt(value.length())
        .put(value.getBytes(StandardCharsets.UTF_8))
        .array();
  }

  private static byte[] constructArray(byte[]... elements) {
    int dataSize = 0;
    for (byte[] element : elements) {
      dataSize += element.length;
    }

    boolean isLarge = elements.length > 0xFF;
    int offsetSize = getMinIntegerSize(dataSize);
    int headerSize = 1 + (isLarge ? 4 : 1) + (elements.length + 1) * offsetSize;

    ByteBuffer output = ByteBuffer.allocate(headerSize + dataSize).order(ByteOrder.LITTLE_ENDIAN);

    output.put(VariantUtil.arrayHeader(isLarge, offsetSize));

    if (isLarge) {
      output.putInt(elements.length);
    } else {
      output.put((byte) elements.length);
    }

    int currOffset = 0;
    for (int i = 0; i < elements.length; ++i) {
      writeVarlenInt(output, currOffset, offsetSize);
      currOffset += elements[i].length;
    }
    writeVarlenInt(output, currOffset, offsetSize);

    for (int i = 0; i < elements.length; ++i) {
      output.put(elements[i]);
    }
    output.flip();
    return output.array();
  }

  @Test
  public void testEmptyArray() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {0b0011, 0x00}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(0, v.numArrayElements());
    });
  }

  @Test
  public void testEmptyLargeArray() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {0b10011, 0x00, 0x00, 0x00, 0x00}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(0, v.numArrayElements());
    });
  }

  @Test
  public void testLargeArraySize() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {0b10011, (byte) 0xFF, (byte) 0x01, 0x00, 0x00}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(511, v.numArrayElements());
    });
  }

  @Test
  public void testMixedArray() {
    byte[] nested = constructArray(VALUE_INT, VALUE_NULL, VALUE_SHORT_STRING);
    Variant value = new Variant(
        ByteBuffer.wrap(constructArray(VALUE_DATE, VALUE_BOOL, VALUE_INT, VALUE_STRING, nested)),
        EMPTY_METADATA);

    testVariant(value, v -> {
      checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(5, v.numArrayElements());
      checkType(v.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.DATE);
      Assert.assertEquals(
          LocalDate.parse("2025-04-17"),
          LocalDate.ofEpochDay(v.getElementAtIndex(0).getInt()));
      checkType(v.getElementAtIndex(1), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getElementAtIndex(1).getBoolean());
      checkType(v.getElementAtIndex(2), VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(1234567890, v.getElementAtIndex(2).getInt());
      checkType(v.getElementAtIndex(3), VariantUtil.PRIMITIVE, Variant.Type.STRING);
      Assert.assertEquals("variant", v.getElementAtIndex(3).getString());
      checkType(v.getElementAtIndex(4), VariantUtil.ARRAY, Variant.Type.ARRAY);

      Variant nestedV = v.getElementAtIndex(4);
      Assert.assertEquals(3, nestedV.numArrayElements());
      checkType(nestedV.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(1234567890, nestedV.getElementAtIndex(0).getInt());
      checkType(nestedV.getElementAtIndex(1), VariantUtil.PRIMITIVE, Variant.Type.NULL);
      checkType(nestedV.getElementAtIndex(2), VariantUtil.SHORT_STR, Variant.Type.STRING);
      Assert.assertEquals("c", nestedV.getElementAtIndex(2).getString());
    });
  }

  public void testArrayOffsetSize(String randomString) {
    Variant value = new Variant(
        ByteBuffer.wrap(constructArray(constructString(randomString), VALUE_BOOL, VALUE_INT)), EMPTY_METADATA);

    testVariant(value, v -> {
      checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(3, v.numArrayElements());
      checkType(v.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.STRING);
      Assert.assertEquals(randomString, v.getElementAtIndex(0).getString());
      checkType(v.getElementAtIndex(1), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getElementAtIndex(1).getBoolean());
      checkType(v.getElementAtIndex(2), VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(1234567890, v.getElementAtIndex(2).getInt());
    });
  }

  @Test
  public void testArrayTwoByteOffset() {
    // a string larger than 255 bytes to push the value offset size above 1 byte
    testArrayOffsetSize(randomString(300));
  }

  @Test
  public void testArrayThreeByteOffset() {
    // a string larger than 65535 bytes to push the value offset size above 2 bytes
    testArrayOffsetSize(randomString(70_000));
  }

  @Test
  public void testArrayFourByteOffset() {
    // a string larger than 16777215 bytes to push the value offset size above 3 bytes
    testArrayOffsetSize(randomString(16_800_000));
  }

  @Test
  public void testInvalidArray() {
    try {
      // An object header
      Variant value = new Variant(ByteBuffer.wrap(new byte[] {0b1000010}), EMPTY_METADATA);
      value.numArrayElements();
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertEquals("Cannot read OBJECT value as ARRAY", e.getMessage());
    }
  }
}
