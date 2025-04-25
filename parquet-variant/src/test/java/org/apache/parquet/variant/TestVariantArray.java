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
import java.time.LocalDate;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestVariantArray {
  private static final Logger LOG = LoggerFactory.getLogger(TestVariantArray.class);

  private static final byte[] VALUE_NULL = new byte[] {VariantTestUtil.primitiveHeader(0)};
  private static final byte[] VALUE_BOOL = new byte[] {VariantTestUtil.primitiveHeader(1)};
  private static final byte[] VALUE_INT =
      new byte[] {VariantTestUtil.primitiveHeader(5), (byte) 0xD2, 0x02, (byte) 0x96, 0x49};
  private static final byte[] VALUE_STRING =
      new byte[] {VariantTestUtil.primitiveHeader(16), 0x07, 0x00, 0x00, 0x00, 'v', 'a', 'r', 'i', 'a', 'n', 't'};
  private static final byte[] VALUE_SHORT_STRING = new byte[] {0b101, 'c'};
  private static final byte[] VALUE_DATE = new byte[] {0b101100, (byte) 0xE3, 0x4E, 0x00, 0x00};

  private static byte[] constructArray(byte[]... elements) {
    int dataSize = 0;
    for (byte[] element : elements) {
      dataSize += element.length;
    }

    boolean isLarge = elements.length > 0xFF;
    int offsetSize = VariantTestUtil.getMinIntegerSize(dataSize);
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
      VariantTestUtil.writeVarlenInt(output, currOffset, offsetSize);
      currOffset += elements[i].length;
    }
    VariantTestUtil.writeVarlenInt(output, currOffset, offsetSize);

    for (int i = 0; i < elements.length; ++i) {
      output.put(elements[i]);
    }
    output.flip();
    return output.array();
  }

  @Test
  public void testEmptyArray() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {0b0011, 0x00}), VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(0, v.numArrayElements());
    });
  }

  @Test
  public void testEmptyLargeArray() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {0b10011, 0x00, 0x00, 0x00, 0x00}), VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(0, v.numArrayElements());
    });
  }

  @Test
  public void testEmptyArrayBuilder() {
    VariantBuilder b = new VariantBuilder(true);
    VariantArrayBuilder a = b.startArray();
    b.endArray(a);
    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(0, v.numArrayElements());
    });
  }

  @Test
  public void testLargeArraySize() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {0b10011, (byte) 0xFF, (byte) 0x01, 0x00, 0x00}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(511, v.numArrayElements());
    });
  }

  @Test
  public void testLargeArraySizeBuilder() {
    VariantBuilder b = new VariantBuilder(true);
    VariantArrayBuilder a = b.startArray();
    for (int i = 0; i < 511; i++) {
      b.startArrayElement(a);
      b.appendLong(i);
    }
    b.endArray(a);
    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(511, v.numArrayElements());
      for (int i = 0; i < 511; i++) {
        Assert.assertEquals(i, v.getElementAtIndex(i).getInt());
      }
    });
  }

  @Test
  public void testMixedArray() {
    byte[] nested = constructArray(VALUE_INT, VALUE_NULL, VALUE_SHORT_STRING);
    Variant value = new Variant(
        ByteBuffer.wrap(constructArray(VALUE_DATE, VALUE_BOOL, VALUE_INT, VALUE_STRING, nested)),
        VariantTestUtil.EMPTY_METADATA);

    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(5, v.numArrayElements());
      VariantTestUtil.checkType(v.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.DATE);
      Assert.assertEquals(
          LocalDate.parse("2025-04-17"),
          LocalDate.ofEpochDay(v.getElementAtIndex(0).getInt()));
      VariantTestUtil.checkType(v.getElementAtIndex(1), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getElementAtIndex(1).getBoolean());
      VariantTestUtil.checkType(v.getElementAtIndex(2), VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(1234567890, v.getElementAtIndex(2).getInt());
      VariantTestUtil.checkType(v.getElementAtIndex(3), VariantUtil.PRIMITIVE, Variant.Type.STRING);
      Assert.assertEquals("variant", v.getElementAtIndex(3).getString());
      VariantTestUtil.checkType(v.getElementAtIndex(4), VariantUtil.ARRAY, Variant.Type.ARRAY);

      Variant nestedV = v.getElementAtIndex(4);
      Assert.assertEquals(3, nestedV.numArrayElements());
      VariantTestUtil.checkType(nestedV.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(1234567890, nestedV.getElementAtIndex(0).getInt());
      VariantTestUtil.checkType(nestedV.getElementAtIndex(1), VariantUtil.PRIMITIVE, Variant.Type.NULL);
      VariantTestUtil.checkType(nestedV.getElementAtIndex(2), VariantUtil.SHORT_STR, Variant.Type.STRING);
      Assert.assertEquals("c", nestedV.getElementAtIndex(2).getString());
    });
  }

  @Test
  public void testMixedArrayBuilder() {
    VariantBuilder b = new VariantBuilder(true);
    VariantArrayBuilder arrBuilder = b.startArray();
    b.startArrayElement(arrBuilder);
    b.appendBoolean(true);
    b.startArrayElement(arrBuilder);
    b.appendLong(1234567890);
    b.startArrayElement(arrBuilder);
    {
      // build a nested array
      VariantArrayBuilder nestedBuilder = b.startArray();
      b.startArrayElement(nestedBuilder);
      {
        // build a nested empty array
        VariantArrayBuilder emptyBuilder = b.startArray();
        b.endArray(emptyBuilder);
      }
      b.startArrayElement(nestedBuilder);
      b.appendString("variant");
      b.endArray(nestedBuilder);
    }
    b.endArray(arrBuilder);

    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(3, v.numArrayElements());
      Assert.assertTrue(v.getElementAtIndex(0).getBoolean());
      Assert.assertEquals(1234567890, v.getElementAtIndex(1).getLong());
      VariantTestUtil.checkType(v.getElementAtIndex(2), VariantUtil.ARRAY, Variant.Type.ARRAY);

      Variant nested = v.getElementAtIndex(2);
      Assert.assertEquals(2, nested.numArrayElements());
      VariantTestUtil.checkType(nested.getElementAtIndex(0), VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(0, nested.getElementAtIndex(0).numArrayElements());
      Assert.assertEquals("variant", nested.getElementAtIndex(1).getString());
    });
  }

  private void testArrayOffsetSize(String randomString) {
    Variant value = new Variant(
        ByteBuffer.wrap(constructArray(VariantTestUtil.constructString(randomString), VALUE_BOOL, VALUE_INT)),
        VariantTestUtil.EMPTY_METADATA);

    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(3, v.numArrayElements());
      VariantTestUtil.checkType(v.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.STRING);
      Assert.assertEquals(randomString, v.getElementAtIndex(0).getString());
      VariantTestUtil.checkType(v.getElementAtIndex(1), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getElementAtIndex(1).getBoolean());
      VariantTestUtil.checkType(v.getElementAtIndex(2), VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(1234567890, v.getElementAtIndex(2).getInt());
    });
  }

  @Test
  public void testArrayTwoByteOffset() {
    // a string larger than 255 bytes to push the value offset size above 1 byte
    testArrayOffsetSize(VariantTestUtil.randomString(300));
  }

  @Test
  public void testArrayThreeByteOffset() {
    // a string larger than 65535 bytes to push the value offset size above 2 bytes
    testArrayOffsetSize(VariantTestUtil.randomString(70_000));
  }

  @Test
  public void testArrayFourByteOffset() {
    // a string larger than 16777215 bytes to push the value offset size above 3 bytes
    testArrayOffsetSize(VariantTestUtil.randomString(16_800_000));
  }

  private void testArrayOffsetSizeBuilder(String randomString) {
    VariantBuilder b = new VariantBuilder(true);
    VariantArrayBuilder arrBuilder = b.startArray();
    b.startArrayElement(arrBuilder);
    b.appendString(randomString);
    b.startArrayElement(arrBuilder);
    b.appendBoolean(true);
    b.startArrayElement(arrBuilder);
    b.appendLong(1234567890);
    b.endArray(arrBuilder);

    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(3, v.numArrayElements());
      VariantTestUtil.checkType(v.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.STRING);
      Assert.assertEquals(randomString, v.getElementAtIndex(0).getString());
      VariantTestUtil.checkType(v.getElementAtIndex(1), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getElementAtIndex(1).getBoolean());
      VariantTestUtil.checkType(v.getElementAtIndex(2), VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(1234567890, v.getElementAtIndex(2).getInt());
    });
  }

  @Test
  public void testArrayTwoByteOffsetBuilder() {
    // a string larger than 255 bytes to push the value offset size above 1 byte
    testArrayOffsetSizeBuilder(VariantTestUtil.randomString(300));
  }

  @Test
  public void testArrayThreeByteOffsetBuilder() {
    // a string larger than 65535 bytes to push the value offset size above 2 bytes
    testArrayOffsetSizeBuilder(VariantTestUtil.randomString(70_000));
  }

  @Test
  public void testArrayFourByteOffsetBuilder() {
    // a string larger than 16777215 bytes to push the value offset size above 3 bytes
    testArrayOffsetSizeBuilder(VariantTestUtil.randomString(16_800_000));
  }

  @Test
  public void testInvalidArray() {
    try {
      // An object header
      Variant value = new Variant(ByteBuffer.wrap(new byte[] {0b1000010}), VariantTestUtil.EMPTY_METADATA);
      value.numArrayElements();
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertEquals("Cannot read OBJECT value as ARRAY", e.getMessage());
    }
  }
}
