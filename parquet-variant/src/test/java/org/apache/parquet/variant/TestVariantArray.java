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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;
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
      assertThat(v.numArrayElements()).isEqualTo(0);
    });
  }

  @Test
  public void testEmptyLargeArray() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {0b10011, 0x00, 0x00, 0x00, 0x00}), VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      assertThat(v.numArrayElements()).isEqualTo(0);
    });
  }

  @Test
  public void testLargeArraySize() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {0b10011, (byte) 0xFF, (byte) 0x01, 0x00, 0x00}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      assertThat(v.numArrayElements()).isEqualTo(511);
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
      assertThat(v.numArrayElements()).isEqualTo(5);
      VariantTestUtil.checkType(v.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.DATE);
      assertThat(LocalDate.ofEpochDay(v.getElementAtIndex(0).getInt())).isEqualTo(LocalDate.parse("2025-04-17"));
      VariantTestUtil.checkType(v.getElementAtIndex(1), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      assertThat(v.getElementAtIndex(1).getBoolean()).isTrue();
      VariantTestUtil.checkType(v.getElementAtIndex(2), VariantUtil.PRIMITIVE, Variant.Type.INT);
      assertThat(v.getElementAtIndex(2).getInt()).isEqualTo(1234567890);
      VariantTestUtil.checkType(v.getElementAtIndex(3), VariantUtil.PRIMITIVE, Variant.Type.STRING);
      assertThat(v.getElementAtIndex(3).getString()).isEqualTo("variant");
      VariantTestUtil.checkType(v.getElementAtIndex(4), VariantUtil.ARRAY, Variant.Type.ARRAY);

      Variant nestedV = v.getElementAtIndex(4);
      assertThat(nestedV.numArrayElements()).isEqualTo(3);
      VariantTestUtil.checkType(nestedV.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.INT);
      assertThat(nestedV.getElementAtIndex(0).getInt()).isEqualTo(1234567890);
      VariantTestUtil.checkType(nestedV.getElementAtIndex(1), VariantUtil.PRIMITIVE, Variant.Type.NULL);
      VariantTestUtil.checkType(nestedV.getElementAtIndex(2), VariantUtil.SHORT_STR, Variant.Type.STRING);
      assertThat(nestedV.getElementAtIndex(2).getString()).isEqualTo("c");
    });
  }

  private void testArrayOffsetSize(String randomString) {
    Variant value = new Variant(
        ByteBuffer.wrap(constructArray(VariantTestUtil.constructString(randomString), VALUE_BOOL, VALUE_INT)),
        VariantTestUtil.EMPTY_METADATA);

    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      assertThat(v.numArrayElements()).isEqualTo(3);
      VariantTestUtil.checkType(v.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.STRING);
      assertThat(v.getElementAtIndex(0).getString()).isEqualTo(randomString);
      VariantTestUtil.checkType(v.getElementAtIndex(1), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      assertThat(v.getElementAtIndex(1).getBoolean()).isTrue();
      VariantTestUtil.checkType(v.getElementAtIndex(2), VariantUtil.PRIMITIVE, Variant.Type.INT);
      assertThat(v.getElementAtIndex(2).getInt()).isEqualTo(1234567890);
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

  @Test
  public void testInvalidArray() {
    // An object header
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {0b1000010}), VariantTestUtil.EMPTY_METADATA);
    assertThatThrownBy(value::numArrayElements)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot read OBJECT value as ARRAY");
  }
}
