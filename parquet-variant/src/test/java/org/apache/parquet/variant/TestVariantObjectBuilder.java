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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestVariantObjectBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(TestVariantObjectBuilder.class);

  @Test
  public void testObjectBuilderWithUUIDBytes() {
    byte[] uuid = new byte[] {0, 17, 34, 51, 68, 85, 102, 119, -120, -103, -86, -69, -52, -35, -18, -1};
    long msb = ByteBuffer.wrap(uuid, 0, 8).order(ByteOrder.BIG_ENDIAN).getLong();
    long lsb = ByteBuffer.wrap(uuid, 8, 8).order(ByteOrder.BIG_ENDIAN).getLong();
    UUID expected = new UUID(msb, lsb);

    VariantBuilder builder = new VariantBuilder();
    VariantObjectBuilder object = builder.startObject();
    object.appendKey("id");
    // appendUUIDBytes must go through onAppend() so that numValues stays in sync with the
    // appended keys. Otherwise endObject() throws because keys (1) != values (0).
    object.appendUUIDBytes(ByteBuffer.wrap(uuid));
    builder.endObject();

    VariantTestUtil.testVariant(builder.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      assertThat(v.numObjectElements()).isEqualTo(1);
      VariantTestUtil.checkType(v.getFieldByKey("id"), VariantUtil.PRIMITIVE, Variant.Type.UUID);
      assertThat(v.getFieldByKey("id").getUUID()).isEqualTo(expected);
    });
  }

  @Test
  public void testEmptyObjectBuilder() {
    VariantBuilder b = new VariantBuilder();
    b.startObject();
    b.endObject();
    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      assertThat(v.numObjectElements()).isEqualTo(0);
    });
  }

  @Test
  public void testLargeObjectBuilder() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder o = b.startObject();
    for (int i = 0; i < 1234; i++) {
      o.appendKey("a" + i);
      o.appendLong(i);
    }
    b.endObject();
    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      assertThat(v.numObjectElements()).isEqualTo(1234);
      for (int i = 0; i < 1234; i++) {
        VariantTestUtil.checkType(v.getFieldByKey("a" + i), VariantUtil.PRIMITIVE, Variant.Type.LONG);
        assertThat(v.getFieldByKey("a" + i).getLong()).isEqualTo(i);
      }
    });
  }

  @Test
  public void testMixedObjectBuilder() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder objBuilder = b.startObject();
    objBuilder.appendKey("outer 2");
    objBuilder.appendLong(1234567890);
    objBuilder.appendKey("outer 1");
    objBuilder.appendBoolean(true);
    objBuilder.appendKey("outer 4");
    objBuilder.startArray();
    objBuilder.endArray();
    objBuilder.appendKey("outer 3");
    {
      // build a nested obj
      VariantObjectBuilder nestedBuilder = objBuilder.startObject();
      nestedBuilder.appendKey("nested 3");
      VariantArrayBuilder arr = nestedBuilder.startArray();
      arr.appendInt(321);
      nestedBuilder.endArray();
      nestedBuilder.appendKey("nested 1");
      {
        // build a nested empty obj
        nestedBuilder.startObject();
        nestedBuilder.endObject();
      }
      nestedBuilder.appendKey("nested 2");
      nestedBuilder.appendString("variant");
      objBuilder.endObject();
    }
    b.endObject();

    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      assertThat(v.numObjectElements()).isEqualTo(4);
      VariantTestUtil.checkType(v.getFieldByKey("outer 1"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      assertThat(v.getFieldByKey("outer 1").getBoolean()).isTrue();
      VariantTestUtil.checkType(v.getFieldByKey("outer 2"), VariantUtil.PRIMITIVE, Variant.Type.LONG);
      assertThat(v.getFieldByKey("outer 2").getLong()).isEqualTo(1234567890);
      VariantTestUtil.checkType(v.getFieldByKey("outer 3"), VariantUtil.OBJECT, Variant.Type.OBJECT);

      Variant nested = v.getFieldByKey("outer 3");
      assertThat(nested.numObjectElements()).isEqualTo(3);
      VariantTestUtil.checkType(nested.getFieldByKey("nested 1"), VariantUtil.OBJECT, Variant.Type.OBJECT);
      assertThat(nested.getFieldByKey("nested 1").numObjectElements()).isEqualTo(0);
      VariantTestUtil.checkType(nested.getFieldByKey("nested 2"), VariantUtil.SHORT_STR, Variant.Type.STRING);
      assertThat(nested.getFieldByKey("nested 2").getString()).isEqualTo("variant");
      VariantTestUtil.checkType(nested.getFieldByKey("nested 3"), VariantUtil.ARRAY, Variant.Type.ARRAY);
      assertThat(nested.getFieldByKey("nested 3").numArrayElements()).isEqualTo(1);
      VariantTestUtil.checkType(
          nested.getFieldByKey("nested 3").getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.INT);
      assertThat(nested.getFieldByKey("nested 3").getElementAtIndex(0).getInt())
          .isEqualTo(321);

      VariantTestUtil.checkType(v.getFieldByKey("outer 4"), VariantUtil.ARRAY, Variant.Type.ARRAY);
      assertThat(v.getFieldByKey("outer 4").numArrayElements()).isEqualTo(0);
    });
  }

  @Test
  public void testMixedBinaryBuilder() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder objBuilder = b.startObject();
    objBuilder.appendKey("as_binary");
    objBuilder.appendBinary(ByteBuffer.wrap(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    objBuilder.appendKey("in_array");
    VariantArrayBuilder arrBinary = objBuilder.startArray();
    arrBinary.appendBinary(ByteBuffer.wrap(new byte[] {}));
    arrBinary.appendBinary(ByteBuffer.wrap(new byte[] {10, 11, 12, 13, 14, 15, 16}));
    arrBinary.appendBinary(ByteBuffer.wrap(new byte[] {17, 18}));
    objBuilder.endArray();
    b.endObject();

    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      assertThat(v.numObjectElements()).isEqualTo(2);
      assertThat(v.getFieldByKey("as_binary").getBinary())
          .isEqualTo(ByteBuffer.wrap(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
      Variant nestedArray = v.getFieldByKey("in_array");
      VariantTestUtil.checkType(nestedArray, VariantUtil.ARRAY, Variant.Type.ARRAY);
      assertThat(nestedArray.numArrayElements()).isEqualTo(3);
      assertThat(nestedArray.getElementAtIndex(0).getBinary()).isEqualTo(ByteBuffer.wrap(new byte[] {}));
      assertThat(nestedArray.getElementAtIndex(1).getBinary())
          .isEqualTo(ByteBuffer.wrap(new byte[] {10, 11, 12, 13, 14, 15, 16}));
      assertThat(nestedArray.getElementAtIndex(2).getBinary()).isEqualTo(ByteBuffer.wrap(new byte[] {17, 18}));
    });
  }

  private void buildNested(int i, VariantObjectBuilder obj) {
    if (i > 0) {
      obj.appendKey("key" + i);
      obj.appendString("str" + i);
      obj.appendKey("duplicate");
      buildNested(i - 1, obj.startObject());
      obj.endObject();
    }
  }

  @Test
  public void testNestedBuilder() {
    VariantBuilder b = new VariantBuilder();
    int depth = VariantUtil.MAX_VARIANT_DEPTH - 1;
    buildNested(depth, b.startObject());
    b.endObject();

    VariantTestUtil.testVariant(b.build(), v -> {
      Variant curr = v;
      for (int i = depth; i >= 0; i--) {
        VariantTestUtil.checkType(curr, VariantUtil.OBJECT, Variant.Type.OBJECT);
        if (i == 0) {
          assertThat(curr.numObjectElements()).isEqualTo(0);
        } else {
          assertThat(curr.numObjectElements()).isEqualTo(2);
          VariantTestUtil.checkType(
              curr.getFieldByKey("key" + i), VariantUtil.SHORT_STR, Variant.Type.STRING);
          assertThat(curr.getFieldByKey("key" + i).getString()).isEqualTo("str" + i);
          curr = curr.getFieldByKey("duplicate");
        }
      }
    });
  }

  private void testObjectOffsetSizeBuilder(String randomString) {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder objBuilder = b.startObject();
    objBuilder.appendKey("key1");
    objBuilder.appendString(randomString);
    objBuilder.appendKey("key2");
    objBuilder.appendBoolean(true);
    objBuilder.appendKey("key3");
    objBuilder.appendLong(1234567890);
    b.endObject();

    Variant v = b.build();
    VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
    assertThat(v.numObjectElements()).isEqualTo(3);
    VariantTestUtil.checkType(v.getFieldByKey("key1"), VariantUtil.PRIMITIVE, Variant.Type.STRING);
    assertThat(v.getFieldByKey("key1").getString()).isEqualTo(randomString);
    VariantTestUtil.checkType(v.getFieldByKey("key2"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
    assertThat(v.getFieldByKey("key2").getBoolean()).isTrue();
    VariantTestUtil.checkType(v.getFieldByKey("key3"), VariantUtil.PRIMITIVE, Variant.Type.LONG);
    assertThat(v.getFieldByKey("key3").getLong()).isEqualTo(1234567890);
  }

  @Test
  public void testObjectTwoByteOffsetBuilder() {
    // a string larger than 255 bytes to push the offset size above 1 byte
    testObjectOffsetSizeBuilder(VariantTestUtil.randomString(300));
  }

  @Test
  public void testObjectThreeByteOffsetBuilder() {
    // a string larger than 65535 bytes to push the offset size above 2 bytes
    testObjectOffsetSizeBuilder(VariantTestUtil.randomString(70_000));
  }

  @Test
  public void testObjectFourByteOffsetBuilder() {
    // a string larger than 16777215 bytes to push the offset size above 3 bytes
    testObjectOffsetSizeBuilder(VariantTestUtil.randomString(16_800_000));
  }

  private void testObjectFieldIdSizeBuilder(int numKeys) {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder objBuilder = b.startObject();
    for (int i = 0; i < numKeys; i++) {
      objBuilder.appendKey("k" + i);
      objBuilder.appendLong(i);
    }
    b.endObject();

    Variant v = b.build();
    VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
    assertThat(v.numObjectElements()).isEqualTo(numKeys);
    // Only check a few keys, to avoid slowing down the test
    VariantTestUtil.checkType(v.getFieldByKey("k" + 0), VariantUtil.PRIMITIVE, Variant.Type.LONG);
    assertThat(v.getFieldByKey("k" + 0).getLong()).isEqualTo(0);
    VariantTestUtil.checkType(v.getFieldByKey("k" + (numKeys - 1)), VariantUtil.PRIMITIVE, Variant.Type.LONG);
    assertThat(v.getFieldByKey("k" + (numKeys - 1)).getLong()).isEqualTo(numKeys - 1);
  }

  @Test
  public void testObjectTwoByteFieldIdBuilder() {
    // need more than 255 dictionary entries to push field id size above 1 byte
    testObjectFieldIdSizeBuilder(300);
  }

  @Test
  public void testObjectThreeByteFieldIdBuilder() {
    // need more than 65535 dictionary entries to push field id size above 2 bytes
    testObjectFieldIdSizeBuilder(70_000);
  }

  @Test
  @Disabled("Test uses too much memory")
  public void testObjectFourByteFieldIdBuilder() {
    // need more than 16777215 dictionary entries to push field id size above 3 bytes
    testObjectFieldIdSizeBuilder(16_800_000);
  }

  @Test
  public void testDuplicateKeys() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder objBuilder = b.startObject();
    objBuilder.appendKey("duplicate");
    objBuilder.appendLong(0);
    objBuilder.appendKey("duplicate");
    objBuilder.appendLong(1);
    b.endObject();
    Variant v = b.build();
    assertThat(v.numObjectElements()).isEqualTo(1);
    VariantTestUtil.checkType(v.getFieldByKey("duplicate"), VariantUtil.PRIMITIVE, Variant.Type.LONG);
    assertThat(v.getFieldByKey("duplicate").getLong()).isEqualTo(1);
  }

  @Test
  public void testDuplicateKeysKeptValueLarger() {
    // The retained (last-written) value is larger than the first occurrence. The data size must be
    // computed from the retained value, otherwise the encoded object is truncated/corrupt.
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder objBuilder = b.startObject();
    objBuilder.appendKey("duplicate");
    objBuilder.appendInt(1); // 5 bytes
    objBuilder.appendKey("duplicate");
    objBuilder.appendString("hello"); // 6 bytes
    b.endObject();
    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      assertThat(v.numObjectElements()).isEqualTo(1);
      Variant variant = v.getFieldByKey("duplicate");
      VariantTestUtil.checkType(variant, VariantUtil.SHORT_STR, Variant.Type.STRING);
      assertThat(variant.getString()).isEqualTo("hello");
    });
  }

  @Test
  public void testDuplicateKeysKeptValueSmaller() {
    // The retained (last-written) value is smaller than the first occurrence. The data size must be
    // computed from the retained value, otherwise the encoded object reserves stale trailing bytes.
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder objBuilder = b.startObject();
    objBuilder.appendKey("duplicate");
    objBuilder.appendString("hello"); // 6 bytes
    objBuilder.appendKey("duplicate");
    objBuilder.appendInt(1); // 5 bytes
    b.endObject();
    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      assertThat(v.numObjectElements()).isEqualTo(1);
      Variant variant = v.getFieldByKey("duplicate");
      VariantTestUtil.checkType(variant, VariantUtil.PRIMITIVE, Variant.Type.INT);
      assertThat(variant.getInt()).isEqualTo(1);
    });
  }

  @Test
  public void testDuplicateKeysDifferentSizesAcrossMultipleKeys() {
    // Exercises deduplication across several keys where the retained values differ in size from the
    // first occurrence, ensuring offsets remain consistent for every field.
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder objBuilder = b.startObject();
    objBuilder.appendKey("a");
    objBuilder.appendInt(1); // 5 bytes
    objBuilder.appendKey("b");
    objBuilder.appendBoolean(true); // 1 byte
    objBuilder.appendKey("a");
    objBuilder.appendString("a-final"); // larger, retained for "a"
    objBuilder.appendKey("b");
    objBuilder.appendLong(123456789L); // 9 bytes, retained for "b"
    objBuilder.appendKey("c");
    objBuilder.appendString("c-only");
    b.endObject();
    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      assertThat(v.numObjectElements()).isEqualTo(3);
      assertThat(v.getFieldByKey("a").getString()).isEqualTo("a-final");
      assertThat(v.getFieldByKey("b").getLong()).isEqualTo(123456789L);
      assertThat(v.getFieldByKey("c").getString()).isEqualTo("c-only");
    });
  }

  @Test
  public void testSortingKeys() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder objBuilder = b.startObject();
    objBuilder.appendKey("1");
    objBuilder.appendString("1");
    objBuilder.appendKey("0");
    objBuilder.appendString("");
    objBuilder.appendKey("3");
    objBuilder.appendString("333");
    objBuilder.appendKey("2");
    objBuilder.appendString("22");
    b.endObject();
    Variant v = b.build();
    assertThat(v.numObjectElements()).isEqualTo(4);
    VariantTestUtil.checkType(v.getFieldByKey("0"), VariantUtil.SHORT_STR, Variant.Type.STRING);
    assertThat(v.getFieldByKey("0").getString()).isEqualTo("");
    VariantTestUtil.checkType(v.getFieldByKey("1"), VariantUtil.SHORT_STR, Variant.Type.STRING);
    assertThat(v.getFieldByKey("1").getString()).isEqualTo("1");
    VariantTestUtil.checkType(v.getFieldByKey("2"), VariantUtil.SHORT_STR, Variant.Type.STRING);
    assertThat(v.getFieldByKey("2").getString()).isEqualTo("22");
    VariantTestUtil.checkType(v.getFieldByKey("3"), VariantUtil.SHORT_STR, Variant.Type.STRING);
    assertThat(v.getFieldByKey("3").getString()).isEqualTo("333");
  }

  @Test
  public void testMissingEndObject() {
    VariantBuilder b = new VariantBuilder();
    b.startObject();
    assertThatThrownBy(b::build)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call build() while an object is being built. Must call endObject() first.");
  }

  @Test
  public void testMissingStartObject() {
    VariantBuilder b = new VariantBuilder();
    assertThatThrownBy(b::endObject)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call endObject() without calling startObject() first.");
  }

  @Test
  public void testMissingValue() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    obj.appendKey("a");
    assertThatThrownBy(b::endObject)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Number of object keys (1) do not match the number of values (0).");

    obj.appendInt(1);
    obj.appendKey("b");
    assertThatThrownBy(b::endObject)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Number of object keys (2) do not match the number of values (1).");
  }

  @Test
  public void testInvalidAppendDuringObjectAppend() {
    VariantBuilder b = new VariantBuilder();
    b.startObject();
    assertThatThrownBy(() -> b.appendInt(1))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot call append() methods while an object is being built. Must call endObject() first.");
  }

  @Test
  public void testMultipleAppendKey() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    obj.appendKey("a");
    assertThatThrownBy(() -> obj.appendKey("a"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call appendKey() before appending a value for the previous key.");
  }

  @Test
  public void testNoAppendKey() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    assertThatThrownBy(() -> obj.appendInt(1))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot append an object value before calling appendKey()");
  }

  @Test
  public void testMultipleAppendValue() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    obj.appendKey("a");
    obj.appendInt(1);
    assertThatThrownBy(() -> obj.appendInt(1))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot append an object value before calling appendKey()");
  }

  @Test
  public void testStartObjectEndArray() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    assertThatThrownBy(obj::endArray)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call endArray() without calling startArray() first.");
  }

  @Test
  public void testOpenNestedObject() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    obj.appendKey("outer");
    obj.startObject();
    assertThatThrownBy(b::endObject)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call endObject() while a nested object/array is still open.");
  }

  @Test
  public void testOpenNestedObjectWithKey() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    obj.appendKey("outer");
    VariantObjectBuilder nested = obj.startObject();
    nested.appendKey("nested");
    assertThatThrownBy(b::endObject)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call endObject() while a nested object/array is still open.");
  }

  @Test
  public void testOpenNestedObjectWithKeyValue() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    obj.appendKey("outer");
    VariantObjectBuilder nested = obj.startObject();
    nested.appendKey("nested");
    nested.appendInt(1);
    assertThatThrownBy(b::endObject)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call endObject() while a nested object/array is still open.");
  }

  @Test
  public void testOpenNestedArray() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    obj.appendKey("outer");
    obj.startArray();
    assertThatThrownBy(b::endObject)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call endObject() while a nested object/array is still open.");
  }

  @Test
  public void testOpenNestedArrayWithElement() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    obj.appendKey("outer");
    VariantArrayBuilder nested = obj.startArray();
    nested.appendInt(1);
    assertThatThrownBy(b::endObject)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call endObject() while a nested object/array is still open.");
  }
}
