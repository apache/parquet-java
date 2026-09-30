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
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestVariantArrayBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(TestVariantArrayBuilder.class);

  @Test
  public void testArrayBuilderWithUUIDBytes() {
    byte[] uuid = new byte[] {0, 17, 34, 51, 68, 85, 102, 119, -120, -103, -86, -69, -52, -35, -18, -1};
    long msb = ByteBuffer.wrap(uuid, 0, 8).order(ByteOrder.BIG_ENDIAN).getLong();
    long lsb = ByteBuffer.wrap(uuid, 8, 8).order(ByteOrder.BIG_ENDIAN).getLong();
    UUID expected = new UUID(msb, lsb);

    VariantBuilder builder = new VariantBuilder();
    VariantArrayBuilder array = builder.startArray();
    array.appendInt(1);
    // appendUUIDBytes must go through onAppend() so that the element offset is recorded and
    // numValues is incremented. Otherwise the offset list is wrong and the UUID element is lost.
    array.appendUUIDBytes(ByteBuffer.wrap(uuid));
    array.appendInt(2);
    builder.endArray();

    VariantTestUtil.testVariant(builder.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      assertThat(v.numArrayElements()).isEqualTo(3);
      VariantTestUtil.checkType(v.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.INT);
      assertThat(v.getElementAtIndex(0).getInt()).isEqualTo(1);
      VariantTestUtil.checkType(v.getElementAtIndex(1), VariantUtil.PRIMITIVE, Variant.Type.UUID);
      assertThat(v.getElementAtIndex(1).getUUID()).isEqualTo(expected);
      VariantTestUtil.checkType(v.getElementAtIndex(2), VariantUtil.PRIMITIVE, Variant.Type.INT);
      assertThat(v.getElementAtIndex(2).getInt()).isEqualTo(2);
    });
  }

  @Test
  public void testEmptyArrayBuilder() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder a = b.startArray();
    b.endArray();
    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      assertThat(v.numArrayElements()).isEqualTo(0);
    });
  }

  @Test
  public void testLargeArraySizeBuilder() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder a = b.startArray();
    for (int i = 0; i < 511; i++) {
      a.appendInt(i);
    }
    b.endArray();
    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      assertThat(v.numArrayElements()).isEqualTo(511);
      for (int i = 0; i < 511; i++) {
        VariantTestUtil.checkType(v.getElementAtIndex(i), VariantUtil.PRIMITIVE, Variant.Type.INT);
        assertThat(v.getElementAtIndex(i).getInt()).isEqualTo(i);
      }
    });
  }

  @Test
  public void testMixedArrayBuilder() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder arrBuilder = b.startArray();
    arrBuilder.appendBoolean(true);
    VariantObjectBuilder obj = arrBuilder.startObject();
    obj.appendKey("key");
    obj.appendInt(321);
    arrBuilder.endObject();
    arrBuilder.appendLong(1234567890);
    {
      // build a nested array
      VariantArrayBuilder nestedBuilder = arrBuilder.startArray();
      {
        // build a nested empty array
        nestedBuilder.startArray();
        nestedBuilder.endArray();
      }
      nestedBuilder.appendString("variant");
      nestedBuilder.startObject();
      nestedBuilder.endObject();
      arrBuilder.endArray();
    }
    b.endArray();

    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      assertThat(v.numArrayElements()).isEqualTo(4);
      VariantTestUtil.checkType(v.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      assertThat(v.getElementAtIndex(0).getBoolean()).isTrue();

      VariantTestUtil.checkType(v.getElementAtIndex(1), VariantUtil.OBJECT, Variant.Type.OBJECT);
      assertThat(v.getElementAtIndex(1).numObjectElements()).isEqualTo(1);
      VariantTestUtil.checkType(
          v.getElementAtIndex(1).getFieldByKey("key"), VariantUtil.PRIMITIVE, Variant.Type.INT);
      assertThat(v.getElementAtIndex(1).getFieldByKey("key").getInt()).isEqualTo(321);

      VariantTestUtil.checkType(v.getElementAtIndex(2), VariantUtil.PRIMITIVE, Variant.Type.LONG);
      assertThat(v.getElementAtIndex(2).getLong()).isEqualTo(1234567890);

      VariantTestUtil.checkType(v.getElementAtIndex(3), VariantUtil.ARRAY, Variant.Type.ARRAY);
      Variant nested = v.getElementAtIndex(3);
      assertThat(nested.numArrayElements()).isEqualTo(3);
      VariantTestUtil.checkType(nested.getElementAtIndex(0), VariantUtil.ARRAY, Variant.Type.ARRAY);
      assertThat(nested.getElementAtIndex(0).numArrayElements()).isEqualTo(0);
      VariantTestUtil.checkType(nested.getElementAtIndex(1), VariantUtil.SHORT_STR, Variant.Type.STRING);
      assertThat(nested.getElementAtIndex(1).getString()).isEqualTo("variant");
      VariantTestUtil.checkType(nested.getElementAtIndex(2), VariantUtil.OBJECT, Variant.Type.OBJECT);
      assertThat(nested.getElementAtIndex(2).numObjectElements()).isEqualTo(0);
    });
  }

  private void buildNested(int i, VariantArrayBuilder obj) {
    if (i > 0) {
      obj.appendString("str" + i);
      buildNested(i - 1, obj.startArray());
      obj.endArray();
    }
  }

  @Test
  public void testNestedBuilder() {
    VariantBuilder b = new VariantBuilder();
    int depth = VariantUtil.MAX_VARIANT_DEPTH - 1;
    buildNested(depth, b.startArray());
    b.endArray();

    VariantTestUtil.testVariant(b.build(), v -> {
      Variant curr = v;
      for (int i = depth; i >= 0; i--) {
        VariantTestUtil.checkType(curr, VariantUtil.ARRAY, Variant.Type.ARRAY);
        if (i == 0) {
          assertThat(curr.numArrayElements()).isEqualTo(0);
        } else {
          assertThat(curr.numArrayElements()).isEqualTo(2);
          VariantTestUtil.checkType(curr.getElementAtIndex(0), VariantUtil.SHORT_STR, Variant.Type.STRING);
          assertThat(curr.getElementAtIndex(0).getString()).isEqualTo("str" + i);
          curr = curr.getElementAtIndex(1);
        }
      }
    });
  }

  private void testArrayOffsetSizeBuilder(String randomString) {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder arrBuilder = b.startArray();
    arrBuilder.appendString(randomString);
    arrBuilder.appendBoolean(true);
    arrBuilder.appendLong(1234567890);
    b.endArray();

    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      assertThat(v.numArrayElements()).isEqualTo(3);
      VariantTestUtil.checkType(v.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.STRING);
      assertThat(v.getElementAtIndex(0).getString()).isEqualTo(randomString);
      VariantTestUtil.checkType(v.getElementAtIndex(1), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      assertThat(v.getElementAtIndex(1).getBoolean()).isTrue();
      VariantTestUtil.checkType(v.getElementAtIndex(2), VariantUtil.PRIMITIVE, Variant.Type.LONG);
      assertThat(v.getElementAtIndex(2).getLong()).isEqualTo(1234567890);
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
  public void testMissingEndArray() {
    VariantBuilder b = new VariantBuilder();
    b.startArray();
    assertThatThrownBy(b::build)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call build() while an array is being built. Must call endArray() first.");
  }

  @Test
  public void testMissingStartArray() {
    VariantBuilder b = new VariantBuilder();
    assertThatThrownBy(b::endArray)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call endArray() without calling startArray() first.");
  }

  @Test
  public void testInvalidAppendDuringArray() {
    VariantBuilder b = new VariantBuilder();
    b.startArray();
    assertThatThrownBy(() -> b.appendInt(1))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call append() methods while an array is being built. Must call endArray() first.");
  }

  @Test
  public void testStartArrayEndObject() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder obj = b.startArray();
    assertThatThrownBy(obj::endObject)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call endObject() without calling startObject() first.");
  }

  @Test
  public void testOpenNestedObject() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder arr = b.startArray();
    arr.startObject();
    assertThatThrownBy(b::endArray)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call endArray() while a nested object/array is still open.");
  }

  @Test
  public void testOpenNestedObjectWithKey() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder arr = b.startArray();
    VariantObjectBuilder nested = arr.startObject();
    nested.appendKey("nested");
    assertThatThrownBy(b::endArray)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call endArray() while a nested object/array is still open.");
  }

  @Test
  public void testOpenNestedObjectWithKeyValue() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder arr = b.startArray();
    VariantObjectBuilder nested = arr.startObject();
    nested.appendKey("nested");
    nested.appendInt(1);
    assertThatThrownBy(b::endArray)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call endArray() while a nested object/array is still open.");
  }

  @Test
  public void testOpenNestedArray() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder arr = b.startArray();
    arr.startArray();
    assertThatThrownBy(b::endArray)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call endArray() while a nested object/array is still open.");
  }

  @Test
  public void testOpenNestedArrayWithElement() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder arr = b.startArray();
    VariantArrayBuilder nested = arr.startArray();
    nested.appendInt(1);
    assertThatThrownBy(b::endArray)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call endArray() while a nested object/array is still open.");
  }
}
