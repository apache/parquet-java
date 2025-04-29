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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestVariantObject {
  private static final Logger LOG = LoggerFactory.getLogger(TestVariantObject.class);

  private static final byte[] VALUE_NULL = new byte[] {VariantTestUtil.primitiveHeader(0)};
  private static final byte[] VALUE_BOOL = new byte[] {VariantTestUtil.primitiveHeader(1)};
  private static final byte[] VALUE_INT =
      new byte[] {VariantTestUtil.primitiveHeader(5), (byte) 0xD2, 0x02, (byte) 0x96, 0x49};
  private static final byte[] VALUE_STRING =
      new byte[] {VariantTestUtil.primitiveHeader(16), 0x07, 0x00, 0x00, 0x00, 'v', 'a', 'r', 'i', 'a', 'n', 't'};
  private static final byte[] VALUE_DATE = new byte[] {0b101100, (byte) 0xE3, 0x4E, 0x00, 0x00};

  private static byte[] constructObject(Map<String, Integer> keys, Map<String, byte[]> fields, boolean orderedData) {
    int dataSize = 0;
    int maxId = 0;
    for (Map.Entry<String, byte[]> entry : fields.entrySet()) {
      dataSize += entry.getValue().length;
      maxId = Math.max(maxId, keys.get(entry.getKey()));
    }

    boolean isLarge = fields.size() > 0xFF;
    int fieldIdSize = VariantTestUtil.getMinIntegerSize(maxId);
    int offsetSize = VariantTestUtil.getMinIntegerSize(dataSize);
    // The space for header byte, object size, id list, and offset list.
    int headerSize = 1 + (isLarge ? 4 : 1) + fields.size() * fieldIdSize + (fields.size() + 1) * offsetSize;

    ByteBuffer output = ByteBuffer.allocate(headerSize + dataSize).order(ByteOrder.LITTLE_ENDIAN);

    output.put(VariantUtil.objectHeader(isLarge, fieldIdSize, offsetSize));

    if (isLarge) {
      output.putInt(fields.size());
    } else {
      output.put((byte) fields.size());
    }

    String[] sortedFieldNames = fields.keySet().toArray(new String[0]);
    Arrays.sort(sortedFieldNames);

    // write field ids
    for (String fieldName : sortedFieldNames) {
      int fieldId = keys.get(fieldName);
      VariantTestUtil.writeVarlenInt(output, fieldId, fieldIdSize);
    }

    // write offsets
    int currOffset = 0;
    for (String fieldName : sortedFieldNames) {
      int offsetToWrite = orderedData ? currOffset : dataSize - currOffset - fields.get(fieldName).length;
      VariantTestUtil.writeVarlenInt(output, offsetToWrite, offsetSize);
      currOffset += fields.get(fieldName).length;
    }
    VariantTestUtil.writeVarlenInt(output, orderedData ? currOffset : 0, offsetSize);

    // write data
    for (int i = 0; i < sortedFieldNames.length; ++i) {
      output.put(fields.get(sortedFieldNames[orderedData ? i : sortedFieldNames.length - i - 1]));
    }

    output.flip();
    return output.array();
  }

  private static ByteBuffer constructMetadata(Boolean isSorted, List<String> fieldNames) {
    if (fieldNames.isEmpty()) {
      return VariantTestUtil.EMPTY_METADATA;
    }

    int dataSize = 0;
    for (String fieldName : fieldNames) {
      dataSize += fieldName.length();
    }

    int offsetSize = VariantTestUtil.getMinIntegerSize(dataSize);
    int offsetListStart = 1 + offsetSize;
    int stringStart = offsetListStart + (fieldNames.size() + 1) * offsetSize;
    int metadataSize = stringStart + dataSize;

    ByteBuffer output = ByteBuffer.allocate(metadataSize).order(ByteOrder.LITTLE_ENDIAN);

    output.put(VariantTestUtil.metadataHeader(isSorted, offsetSize));
    VariantTestUtil.writeVarlenInt(output, fieldNames.size(), offsetSize);

    // write offsets
    int currentOffset = 0;
    for (String fieldName : fieldNames) {
      VariantTestUtil.writeVarlenInt(output, currentOffset, offsetSize);
      currentOffset += fieldName.length();
    }
    VariantTestUtil.writeVarlenInt(output, currentOffset, offsetSize);

    // write strings
    for (String fieldName : fieldNames) {
      output.put(fieldName.getBytes(StandardCharsets.UTF_8));
    }

    output.flip();
    return output;
  }

  @Test
  public void testEmptyObject() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {0b10, 0x00}), VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(0, v.numObjectElements());
    });
  }

  @Test
  public void testEmptyObjectBuilder() {
    VariantBuilder b = new VariantBuilder(true);
    VariantObjectBuilder o = b.startObject();
    b.endObject(o);
    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(0, v.numObjectElements());
    });
  }

  @Test
  public void testEmptyLargeObject() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {0b1000010, 0x00, 0x00, 0x00, 0x00}), VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(0, v.numObjectElements());
    });
  }

  @Test
  public void testLargeObjectBuilder() {
    VariantBuilder b = new VariantBuilder(true);
    VariantObjectBuilder o = b.startObject();
    for (int i = 0; i < 1234; i++) {
      b.appendObjectKey(o, "a" + i);
      b.appendLong(i);
    }
    b.endObject(o);
    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(1234, v.numObjectElements());
      for (int i = 0; i < 1234; i++) {
        Assert.assertEquals(i, v.getFieldByKey("a" + i).getLong());
      }
    });
  }

  @Test
  public void testUnsortedMetadataObject() {
    Map<String, Integer> keys = ImmutableMap.of("a", 2, "b", 1, "c", 0);
    Map<String, byte[]> fields = ImmutableMap.of("a", VALUE_INT, "b", VALUE_BOOL, "c", VALUE_STRING);

    Variant value = new Variant(
        ByteBuffer.wrap(constructObject(keys, fields, true)),
        constructMetadata(false, ImmutableList.of("c", "b", "a")));
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(3, v.numObjectElements());
      VariantTestUtil.checkType(v.getFieldByKey("a"), VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(1234567890, v.getFieldByKey("a").getInt());
      VariantTestUtil.checkType(v.getFieldByKey("b"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getFieldByKey("b").getBoolean());
      VariantTestUtil.checkType(v.getFieldByKey("c"), VariantUtil.PRIMITIVE, Variant.Type.STRING);
      Assert.assertEquals("variant", v.getFieldByKey("c").getString());
    });
  }

  @Test
  public void testMixedObject() {
    Map<String, Integer> keys = ImmutableMap.of("a", 0, "b", 1, "c", 2);
    byte[] nested = constructObject(keys, ImmutableMap.of("a", VALUE_DATE, "c", VALUE_NULL), false);
    Map<String, byte[]> fields = ImmutableMap.of("a", VALUE_INT, "b", VALUE_BOOL, "c", nested);

    Variant value = new Variant(
        ByteBuffer.wrap(constructObject(keys, fields, true)),
        constructMetadata(true, ImmutableList.of("a", "b", "c")));
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(3, v.numObjectElements());
      VariantTestUtil.checkType(v.getFieldByKey("a"), VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(1234567890, v.getFieldByKey("a").getInt());
      VariantTestUtil.checkType(v.getFieldByKey("b"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getFieldByKey("b").getBoolean());
      VariantTestUtil.checkType(v.getFieldByKey("c"), VariantUtil.OBJECT, Variant.Type.OBJECT);

      Variant nestedV = v.getFieldByKey("c");
      Assert.assertEquals(2, nestedV.numObjectElements());
      VariantTestUtil.checkType(nestedV.getFieldByKey("a"), VariantUtil.PRIMITIVE, Variant.Type.DATE);
      Assert.assertEquals(
          LocalDate.parse("2025-04-17"),
          LocalDate.ofEpochDay(nestedV.getFieldByKey("a").getInt()));
      VariantTestUtil.checkType(nestedV.getFieldByKey("c"), VariantUtil.PRIMITIVE, Variant.Type.NULL);
    });
  }

  @Test
  public void testMixedObjectBuilder() {
    VariantBuilder b = new VariantBuilder(true);
    VariantObjectBuilder objBuilder = b.startObject();
    b.appendObjectKey(objBuilder, "outer 1");
    b.appendBoolean(true);
    b.appendObjectKey(objBuilder, "outer 2");
    b.appendLong(1234567890);
    b.appendObjectKey(objBuilder, "outer 3");
    {
      // build a nested obj
      VariantObjectBuilder nestedBuilder = b.startObject();
      b.appendObjectKey(nestedBuilder, "nested 1");
      {
        // build a nested empty obj
        VariantObjectBuilder emptyBuilder = b.startObject();
        b.endObject(emptyBuilder);
      }
      b.appendObjectKey(nestedBuilder, "nested 2");
      b.appendString("variant");
      b.endObject(nestedBuilder);
    }
    b.endObject(objBuilder);

    VariantTestUtil.testVariant(b.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(3, v.numObjectElements());
      Assert.assertTrue(v.getFieldByKey("outer 1").getBoolean());
      Assert.assertEquals(1234567890, v.getFieldByKey("outer 2").getLong());
      VariantTestUtil.checkType(v.getFieldByKey("outer 3"), VariantUtil.OBJECT, Variant.Type.OBJECT);

      Variant nested = v.getFieldByKey("outer 3");
      Assert.assertEquals(2, nested.numObjectElements());
      VariantTestUtil.checkType(nested.getFieldByKey("nested 1"), VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(0, nested.getFieldByKey("nested 1").numObjectElements());
      Assert.assertEquals("variant", nested.getFieldByKey("nested 2").getString());
    });
  }

  @Test
  public void testUnsortedDataObject() {
    Map<String, Integer> keys = ImmutableMap.of("a", 0, "b", 1, "c", 2);
    Map<String, byte[]> fields = ImmutableMap.of("a", VALUE_INT, "b", VALUE_BOOL, "c", VALUE_STRING);

    Variant value = new Variant(
        ByteBuffer.wrap(constructObject(keys, fields, false)),
        constructMetadata(true, ImmutableList.of("a", "b", "c")));
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(3, v.numObjectElements());
      VariantTestUtil.checkType(v.getFieldByKey("a"), VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(1234567890, v.getFieldByKey("a").getInt());
      VariantTestUtil.checkType(v.getFieldByKey("b"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getFieldByKey("b").getBoolean());
      VariantTestUtil.checkType(v.getFieldByKey("c"), VariantUtil.PRIMITIVE, Variant.Type.STRING);
      Assert.assertEquals("variant", v.getFieldByKey("c").getString());
    });
  }

  private void testObjectOffsetSize(String randomString) {
    Variant value = new Variant(
        ByteBuffer.wrap(constructObject(
            ImmutableMap.of("a", 0, "b", 1, "c", 2),
            ImmutableMap.of(
                "a", VariantTestUtil.constructString(randomString), "b", VALUE_BOOL, "c", VALUE_INT),
            true)),
        constructMetadata(true, ImmutableList.of("a", "b", "c")));
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(3, v.numObjectElements());
      VariantTestUtil.checkType(v.getFieldByKey("a"), VariantUtil.PRIMITIVE, Variant.Type.STRING);
      Assert.assertEquals(randomString, v.getFieldByKey("a").getString());
      VariantTestUtil.checkType(v.getFieldByKey("b"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getFieldByKey("b").getBoolean());
      VariantTestUtil.checkType(v.getFieldByKey("c"), VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(1234567890, v.getFieldByKey("c").getInt());
    });
  }

  @Test
  public void testObjectTwoByteOffset() {
    // a string larger than 255 bytes to push the offset size above 1 byte
    testObjectOffsetSize(VariantTestUtil.randomString(300));
  }

  @Test
  public void testObjectThreeByteOffset() {
    // a string larger than 65535 bytes to push the offset size above 2 bytes
    testObjectOffsetSize(VariantTestUtil.randomString(70_000));
  }

  @Test
  public void testObjectFourByteOffset() {
    // a string larger than 16777215 bytes to push the offset size above 3 bytes
    testObjectOffsetSize(VariantTestUtil.randomString(16_800_000));
  }

  private void testObjectFieldIdSize(int numExtraKeys) {
    List<String> fieldNames = new ArrayList<>();
    for (int i = 0; i < numExtraKeys; i++) {
      fieldNames.add("a" + i);
    }
    fieldNames.add("z1");
    fieldNames.add("z2");

    Variant value = new Variant(
        ByteBuffer.wrap(constructObject(
            ImmutableMap.of("z1", numExtraKeys, "z2", numExtraKeys + 1),
            ImmutableMap.of("z1", VALUE_BOOL, "z2", VALUE_INT),
            true)),
        constructMetadata(true, fieldNames));
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(2, v.numObjectElements());
      VariantTestUtil.checkType(v.getFieldByKey("z1"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getFieldByKey("z1").getBoolean());
      VariantTestUtil.checkType(v.getFieldByKey("z2"), VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(1234567890, v.getFieldByKey("z2").getInt());
    });
  }

  @Test
  public void testObjectTwoByteFieldId() {
    // need more than 255 dictionary entries to push field id size above 1 byte
    testObjectFieldIdSize(300);
  }

  @Test
  public void testObjectThreeByteFieldId() {
    // need more than 65535 dictionary entries to push field id size above 2 bytes
    testObjectFieldIdSize(70_000);
  }

  @Test
  public void testObjectFourByteFieldId() {
    // need more than 16777215 dictionary entries to push field id size above 3 bytes
    testObjectFieldIdSize(16_800_000);
  }

  private void testObjectOffsetSizeBuilder(String randomString) {
    VariantBuilder b = new VariantBuilder(true);
    VariantObjectBuilder objBuilder = b.startObject();
    b.appendObjectKey(objBuilder, "key1");
    b.appendString(randomString);
    b.appendObjectKey(objBuilder, "key2");
    b.appendBoolean(true);
    b.appendObjectKey(objBuilder, "key3");
    b.appendLong(1234567890);
    b.endObject(objBuilder);

    Variant v = b.build();
    VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
    Assert.assertEquals(3, v.numObjectElements());
    VariantTestUtil.checkType(v.getFieldByKey("key1"), VariantUtil.PRIMITIVE, Variant.Type.STRING);
    Assert.assertEquals(randomString, v.getFieldByKey("key1").getString());
    VariantTestUtil.checkType(v.getFieldByKey("key2"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
    Assert.assertTrue(v.getFieldByKey("key2").getBoolean());
    VariantTestUtil.checkType(v.getFieldByKey("key3"), VariantUtil.PRIMITIVE, Variant.Type.INT);
    Assert.assertEquals(1234567890, v.getFieldByKey("key3").getInt());
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
    VariantBuilder b = new VariantBuilder(true);
    VariantObjectBuilder objBuilder = b.startObject();
    for (int i = 0; i < numKeys; i++) {
      b.appendObjectKey(objBuilder, "k" + i);
      b.appendLong(i);
    }
    b.endObject(objBuilder);

    Variant v = b.build();
    VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
    Assert.assertEquals(numKeys, v.numObjectElements());
    // Only check a few keys, to avoid slowing down the test
    Assert.assertEquals(0, v.getFieldByKey("k" + 0).getLong());
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
  public void testLargeObject() {
    Map<String, Integer> keys = new HashMap<>();
    Map<String, byte[]> fields = new HashMap<>();
    for (int i = 0; i < 1000; i++) {
      String name = String.format("a%04d", i);
      keys.put(name, i);
      fields.put(name, VariantTestUtil.constructString(VariantTestUtil.randomString(5)));
    }

    List<String> sortedKeys = new ArrayList<>(keys.keySet());
    Collections.sort(sortedKeys);

    Variant value =
        new Variant(ByteBuffer.wrap(constructObject(keys, fields, false)), constructMetadata(true, sortedKeys));
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(1000, v.numObjectElements());

      for (int i = 0; i < 1000; i++) {
        String name = String.format("a%04d", i);
        VariantTestUtil.checkType(v.getFieldByKey(name), VariantUtil.PRIMITIVE, Variant.Type.STRING);
        Assert.assertEquals(
            new String(fields.get(name), 5, fields.get(name).length - 5),
            v.getFieldByKey(name).getString());
      }
    });
  }

  @Test
  public void testInvalidObject() {
    try {
      // An array header
      Variant value = new Variant(ByteBuffer.wrap(new byte[] {0b10011}), VariantTestUtil.EMPTY_METADATA);
      value.numObjectElements();
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertEquals("Cannot read ARRAY value as OBJECT", e.getMessage());
    }
  }

  @Test
  public void testDuplicateKeys() {
    // disallow duplicate keys
    try {
      VariantBuilder b = new VariantBuilder(false);
      VariantObjectBuilder objBuilder = b.startObject();
      b.appendObjectKey(objBuilder, "duplicate");
      b.appendLong(0);
      b.appendObjectKey(objBuilder, "duplicate");
      b.appendLong(1);
      b.endObject(objBuilder);
      b.build();
      Assert.fail("Expected VariantDuplicateKeyException with duplicate keys");
    } catch (Exception e) {
      // Expected
      Assert.assertEquals("Failed to build Variant because of duplicate object key: duplicate", e.getMessage());
    }

    // allow duplicate keys
    try {
      VariantBuilder b = new VariantBuilder(true);
      VariantObjectBuilder objBuilder = b.startObject();
      b.appendObjectKey(objBuilder, "duplicate");
      b.appendLong(0);
      b.appendObjectKey(objBuilder, "duplicate");
      b.appendLong(1);
      b.endObject(objBuilder);
      Variant v = b.build();
      Assert.assertEquals(1, v.numObjectElements());
      Assert.assertEquals(1, v.getFieldByKey("duplicate").getLong());
    } catch (Exception e) {
      Assert.fail("Unexpected exception: " + e);
    }
  }
}
