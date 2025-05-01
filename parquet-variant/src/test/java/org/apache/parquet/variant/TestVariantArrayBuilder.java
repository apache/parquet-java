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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestVariantArrayBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(TestVariantArrayBuilder.class);

  private static final byte[] VALUE_NULL = new byte[] {VariantTestUtil.primitiveHeader(0)};
  private static final byte[] VALUE_BOOL = new byte[] {VariantTestUtil.primitiveHeader(1)};
  private static final byte[] VALUE_INT =
      new byte[] {VariantTestUtil.primitiveHeader(5), (byte) 0xD2, 0x02, (byte) 0x96, 0x49};
  private static final byte[] VALUE_STRING =
      new byte[] {VariantTestUtil.primitiveHeader(16), 0x07, 0x00, 0x00, 0x00, 'v', 'a', 'r', 'i', 'a', 'n', 't'};
  private static final byte[] VALUE_SHORT_STRING = new byte[] {0b101, 'c'};
  private static final byte[] VALUE_DATE = new byte[] {0b101100, (byte) 0xE3, 0x4E, 0x00, 0x00};

  @Test
  public void testEmptyArrayBuilder() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder a = b.startArray();
    b.endArray(a);
    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(0, v.numArrayElements());
    });
  }

  @Test
  public void testLargeArraySizeBuilder() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder a = b.startArray();
    for (int i = 0; i < 511; i++) {
      b.startArrayElement(a);
      b.appendInt(i);
    }
    b.endArray(a);
    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(511, v.numArrayElements());
      for (int i = 0; i < 511; i++) {
        VariantTestUtil.checkType(v.getElementAtIndex(i), VariantUtil.PRIMITIVE, Variant.Type.INT);
        Assert.assertEquals(i, v.getElementAtIndex(i).getInt());
      }
    });
  }

  @Test
  public void testMixedArrayBuilder() {
    VariantBuilder b = new VariantBuilder();
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

  private void testArrayOffsetSizeBuilder(String randomString) {
    VariantBuilder b = new VariantBuilder();
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
      VariantTestUtil.checkType(v.getElementAtIndex(2), VariantUtil.PRIMITIVE, Variant.Type.LONG);
      Assert.assertEquals(1234567890, v.getElementAtIndex(2).getLong());
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
}
