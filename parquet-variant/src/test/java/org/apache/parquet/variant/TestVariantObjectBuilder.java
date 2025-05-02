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

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestVariantObjectBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(TestVariantObjectBuilder.class);

  @Test
  public void testEmptyObjectBuilder() {
    VariantBuilder b = new VariantBuilder();
    b.startObject();
    b.endObject();
    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(0, v.numObjectElements());
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
      Assert.assertEquals(1234, v.numObjectElements());
      for (int i = 0; i < 1234; i++) {
        VariantTestUtil.checkType(v.getFieldByKey("a" + i), VariantUtil.PRIMITIVE, Variant.Type.LONG);
        Assert.assertEquals(i, v.getFieldByKey("a" + i).getLong());
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
      Assert.assertEquals(4, v.numObjectElements());
      VariantTestUtil.checkType(v.getFieldByKey("outer 1"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getFieldByKey("outer 1").getBoolean());
      VariantTestUtil.checkType(v.getFieldByKey("outer 2"), VariantUtil.PRIMITIVE, Variant.Type.LONG);
      Assert.assertEquals(1234567890, v.getFieldByKey("outer 2").getLong());
      VariantTestUtil.checkType(v.getFieldByKey("outer 3"), VariantUtil.OBJECT, Variant.Type.OBJECT);

      Variant nested = v.getFieldByKey("outer 3");
      Assert.assertEquals(3, nested.numObjectElements());
      VariantTestUtil.checkType(nested.getFieldByKey("nested 1"), VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(0, nested.getFieldByKey("nested 1").numObjectElements());
      VariantTestUtil.checkType(nested.getFieldByKey("nested 2"), VariantUtil.SHORT_STR, Variant.Type.STRING);
      Assert.assertEquals("variant", nested.getFieldByKey("nested 2").getString());
      VariantTestUtil.checkType(nested.getFieldByKey("nested 3"), VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(1, nested.getFieldByKey("nested 3").numArrayElements());
      VariantTestUtil.checkType(
          nested.getFieldByKey("nested 3").getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(
          321, nested.getFieldByKey("nested 3").getElementAtIndex(0).getInt());

      VariantTestUtil.checkType(v.getFieldByKey("outer 4"), VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(0, v.getFieldByKey("outer 4").numArrayElements());
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
    buildNested(1000, b.startObject());
    b.endObject();

    VariantTestUtil.testVariant(b.build(), v -> {
      Variant curr = v;
      for (int i = 1000; i >= 0; i--) {
        VariantTestUtil.checkType(curr, VariantUtil.OBJECT, Variant.Type.OBJECT);
        if (i == 0) {
          Assert.assertEquals(0, curr.numObjectElements());
        } else {
          Assert.assertEquals(2, curr.numObjectElements());
          VariantTestUtil.checkType(
              curr.getFieldByKey("key" + i), VariantUtil.SHORT_STR, Variant.Type.STRING);
          Assert.assertEquals("str" + i, curr.getFieldByKey("key" + i).getString());
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
    Assert.assertEquals(3, v.numObjectElements());
    VariantTestUtil.checkType(v.getFieldByKey("key1"), VariantUtil.PRIMITIVE, Variant.Type.STRING);
    Assert.assertEquals(randomString, v.getFieldByKey("key1").getString());
    VariantTestUtil.checkType(v.getFieldByKey("key2"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
    Assert.assertTrue(v.getFieldByKey("key2").getBoolean());
    VariantTestUtil.checkType(v.getFieldByKey("key3"), VariantUtil.PRIMITIVE, Variant.Type.LONG);
    Assert.assertEquals(1234567890, v.getFieldByKey("key3").getLong());
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
    Assert.assertEquals(numKeys, v.numObjectElements());
    // Only check a few keys, to avoid slowing down the test
    VariantTestUtil.checkType(v.getFieldByKey("k" + 0), VariantUtil.PRIMITIVE, Variant.Type.LONG);
    Assert.assertEquals(0, v.getFieldByKey("k" + 0).getLong());
    VariantTestUtil.checkType(v.getFieldByKey("k" + (numKeys - 1)), VariantUtil.PRIMITIVE, Variant.Type.LONG);
    Assert.assertEquals(numKeys - 1, v.getFieldByKey("k" + (numKeys - 1)).getLong());
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
  @Ignore("Test uses too much memory")
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
    Assert.assertEquals(1, v.numObjectElements());
    VariantTestUtil.checkType(v.getFieldByKey("duplicate"), VariantUtil.PRIMITIVE, Variant.Type.LONG);
    Assert.assertEquals(1, v.getFieldByKey("duplicate").getLong());
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
    Assert.assertEquals(4, v.numObjectElements());
    VariantTestUtil.checkType(v.getFieldByKey("0"), VariantUtil.SHORT_STR, Variant.Type.STRING);
    Assert.assertEquals("", v.getFieldByKey("0").getString());
    VariantTestUtil.checkType(v.getFieldByKey("1"), VariantUtil.SHORT_STR, Variant.Type.STRING);
    Assert.assertEquals("1", v.getFieldByKey("1").getString());
    VariantTestUtil.checkType(v.getFieldByKey("2"), VariantUtil.SHORT_STR, Variant.Type.STRING);
    Assert.assertEquals("22", v.getFieldByKey("2").getString());
    VariantTestUtil.checkType(v.getFieldByKey("3"), VariantUtil.SHORT_STR, Variant.Type.STRING);
    Assert.assertEquals("333", v.getFieldByKey("3").getString());
  }

  @Test
  public void testMissingEndObject() {
    VariantBuilder b = new VariantBuilder();
    b.startObject();
    try {
      b.build();
      Assert.fail("Expected Exception when calling build() without endObject()");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testMissingStartObject() {
    VariantBuilder b = new VariantBuilder();
    try {
      b.endObject();
      Assert.fail("Expected Exception when calling endObject() without startObject()");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testMissingValue() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    obj.appendKey("a");
    try {
      b.endObject();
      Assert.fail("Expected Exception when calling endObject() with mismatched keys and values");
    } catch (Exception e) {
      // expected
    }

    obj.appendInt(1);
    obj.appendKey("b");
    try {
      b.endObject();
      Assert.fail("Expected Exception when calling endObject() with mismatched keys and values");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testInvalidAppendDuringObjectAppend() {
    VariantBuilder b = new VariantBuilder();
    b.startObject();
    try {
      b.appendInt(1);
      Assert.fail("Expected Exception when calling append() before endObject()");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testMultipleAppendKey() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    obj.appendKey("a");
    try {
      obj.appendKey("a");
      Assert.fail("Expected Exception when calling appendKey() multiple times without appending a value");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testNoAppendKey() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    try {
      obj.appendInt(1);
      Assert.fail("Expected Exception when appending a value, before appending a key");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testMultipleAppendValue() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    obj.appendKey("a");
    obj.appendInt(1);
    try {
      obj.appendInt(1);
      Assert.fail("Expected Exception when appending a value, before appending a key");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testStartObjectEndArray() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    try {
      obj.endArray();
      Assert.fail("Expected Exception when calling endArray() while building object");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testOpenNestedObject() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    obj.appendKey("outer");
    obj.startObject();
    try {
      b.endObject();
      Assert.fail("Expected Exception when calling endObject() with an open nested object");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testOpenNestedObjectWithKey() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    obj.appendKey("outer");
    VariantObjectBuilder nested = obj.startObject();
    nested.appendKey("nested");
    try {
      b.endObject();
      Assert.fail("Expected Exception when calling endObject() with an open nested object");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testOpenNestedObjectWithKeyValue() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    obj.appendKey("outer");
    VariantObjectBuilder nested = obj.startObject();
    nested.appendKey("nested");
    nested.appendInt(1);
    try {
      b.endObject();
      Assert.fail("Expected Exception when calling endObject() with an open nested object");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testOpenNestedArray() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    obj.appendKey("outer");
    obj.startArray();
    try {
      b.endObject();
      Assert.fail("Expected Exception when calling endObject() with an open nested array");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testOpenNestedArrayWithElement() {
    VariantBuilder b = new VariantBuilder();
    VariantObjectBuilder obj = b.startObject();
    obj.appendKey("outer");
    VariantArrayBuilder nested = obj.startArray();
    nested.appendInt(1);
    try {
      b.endObject();
      Assert.fail("Expected Exception when calling endObject() with an open nested array");
    } catch (Exception e) {
      // expected
    }
  }
}
