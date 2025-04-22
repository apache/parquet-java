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
import java.security.SecureRandom;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Consumer;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestVariantObject {
  private static final Logger LOG = LoggerFactory.getLogger(TestVariantObject.class);
  private static final String RANDOM_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

  /** Random number generator for generating random strings */
  private static SecureRandom random = new SecureRandom(new byte[] {1, 2, 3, 4, 5});

  private static final ByteBuffer EMPTY_METADATA = ByteBuffer.wrap(new byte[] {0b1});

  private static final byte[] VALUE_NULL = new byte[] {primitiveHeader(0)};
  private static final byte[] VALUE_BOOL = new byte[] {primitiveHeader(1)};
  private static final byte[] VALUE_INT = new byte[] {primitiveHeader(5), (byte) 0xD2, 0x02, (byte) 0x96, 0x49};
  private static final byte[] VALUE_STRING =
      new byte[] {primitiveHeader(16), 0x07, 0x00, 0x00, 0x00, 'v', 'a', 'r', 'i', 'a', 'n', 't'};
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

  private static byte metadataHeader(boolean isSorted, int offsetSize) {
    return (byte) (((offsetSize - 1) << 6) | (isSorted ? 0b10000 : 0) | 0b0001);
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

  private static byte[] constructObject(Map<String, Integer> keys, Map<String, byte[]> fields, boolean orderedData) {
    int dataSize = 0;
    int maxId = 0;
    for (Map.Entry<String, byte[]> entry : fields.entrySet()) {
      dataSize += entry.getValue().length;
      maxId = Math.max(maxId, keys.get(entry.getKey()));
    }

    boolean isLarge = fields.size() > 0xFF;
    int fieldIdSize = getMinIntegerSize(maxId);
    int offsetSize = getMinIntegerSize(dataSize);
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
      writeVarlenInt(output, fieldId, fieldIdSize);
    }

    // write offsets
    int currOffset = 0;
    for (String fieldName : sortedFieldNames) {
      int offsetToWrite = orderedData ? currOffset : dataSize - currOffset - fields.get(fieldName).length;
      writeVarlenInt(output, offsetToWrite, offsetSize);
      currOffset += fields.get(fieldName).length;
    }
    writeVarlenInt(output, orderedData ? currOffset : 0, offsetSize);

    // write data
    for (int i = 0; i < sortedFieldNames.length; ++i) {
      output.put(fields.get(sortedFieldNames[orderedData ? i : sortedFieldNames.length - i - 1]));
    }

    output.flip();
    return output.array();
  }

  private static ByteBuffer constructMetadata(Boolean isSorted, List<String> fieldNames) {
    if (fieldNames.isEmpty()) {
      return EMPTY_METADATA;
    }

    int dataSize = 0;
    for (String fieldName : fieldNames) {
      dataSize += fieldName.length();
    }

    int offsetSize = getMinIntegerSize(dataSize);
    int offsetListStart = 1 + offsetSize;
    int stringStart = offsetListStart + (fieldNames.size() + 1) * offsetSize;
    int metadataSize = stringStart + dataSize;

    ByteBuffer output = ByteBuffer.allocate(metadataSize).order(ByteOrder.LITTLE_ENDIAN);

    output.put(metadataHeader(isSorted, offsetSize));
    writeVarlenInt(output, fieldNames.size(), offsetSize);

    // write offsets
    int currentOffset = 0;
    for (String fieldName : fieldNames) {
      writeVarlenInt(output, currentOffset, offsetSize);
      currentOffset += fieldName.length();
    }
    writeVarlenInt(output, currentOffset, offsetSize);

    // write strings
    for (String fieldName : fieldNames) {
      output.put(fieldName.getBytes(StandardCharsets.UTF_8));
    }

    output.flip();
    return output;
  }

  @Test
  public void testEmptyObject() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {0b10, 0x00}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(0, v.numObjectElements());
    });
  }

  @Test
  public void testEmptyLargeObject() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {0b1000010, 0x00, 0x00, 0x00, 0x00}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(0, v.numObjectElements());
    });
  }

  @Test
  public void testUnsortedMetadataObject() {
    Map<String, Integer> keys = ImmutableMap.of("a", 2, "b", 1, "c", 0);
    Map<String, byte[]> fields = ImmutableMap.of("a", VALUE_INT, "b", VALUE_BOOL, "c", VALUE_STRING);

    Variant value = new Variant(
        ByteBuffer.wrap(constructObject(keys, fields, true)),
        constructMetadata(false, ImmutableList.of("c", "b", "a")));
    testVariant(value, v -> {
      checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(3, v.numObjectElements());
      checkType(v.getFieldByKey("a"), VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(1234567890, v.getFieldByKey("a").getInt());
      checkType(v.getFieldByKey("b"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getFieldByKey("b").getBoolean());
      checkType(v.getFieldByKey("c"), VariantUtil.PRIMITIVE, Variant.Type.STRING);
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
    testVariant(value, v -> {
      checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(3, v.numObjectElements());
      checkType(v.getFieldByKey("a"), VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(1234567890, v.getFieldByKey("a").getInt());
      checkType(v.getFieldByKey("b"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getFieldByKey("b").getBoolean());
      checkType(v.getFieldByKey("c"), VariantUtil.OBJECT, Variant.Type.OBJECT);

      Variant nestedV = v.getFieldByKey("c");
      Assert.assertEquals(2, nestedV.numObjectElements());
      checkType(nestedV.getFieldByKey("a"), VariantUtil.PRIMITIVE, Variant.Type.DATE);
      Assert.assertEquals(
          LocalDate.parse("2025-04-17"),
          LocalDate.ofEpochDay(nestedV.getFieldByKey("a").getInt()));
      checkType(nestedV.getFieldByKey("c"), VariantUtil.PRIMITIVE, Variant.Type.NULL);
    });
  }

  @Test
  public void testUnsortedDataObject() {
    Map<String, Integer> keys = ImmutableMap.of("a", 0, "b", 1, "c", 2);
    Map<String, byte[]> fields = ImmutableMap.of("a", VALUE_INT, "b", VALUE_BOOL, "c", VALUE_STRING);

    Variant value = new Variant(
        ByteBuffer.wrap(constructObject(keys, fields, false)),
        constructMetadata(true, ImmutableList.of("a", "b", "c")));
    testVariant(value, v -> {
      checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(3, v.numObjectElements());
      checkType(v.getFieldByKey("a"), VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(1234567890, v.getFieldByKey("a").getInt());
      checkType(v.getFieldByKey("b"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getFieldByKey("b").getBoolean());
      checkType(v.getFieldByKey("c"), VariantUtil.PRIMITIVE, Variant.Type.STRING);
      Assert.assertEquals("variant", v.getFieldByKey("c").getString());
    });
  }

  private void testObjectOffsetSize(String randomString) {
    Variant value = new Variant(
        ByteBuffer.wrap(constructObject(
            ImmutableMap.of("a", 0, "b", 1, "c", 2),
            ImmutableMap.of("a", constructString(randomString), "b", VALUE_BOOL, "c", VALUE_INT),
            true)),
        constructMetadata(true, ImmutableList.of("a", "b", "c")));
    testVariant(value, v -> {
      checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(3, v.numObjectElements());
      checkType(v.getFieldByKey("a"), VariantUtil.PRIMITIVE, Variant.Type.STRING);
      Assert.assertEquals(randomString, v.getFieldByKey("a").getString());
      checkType(v.getFieldByKey("b"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getFieldByKey("b").getBoolean());
      checkType(v.getFieldByKey("c"), VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(1234567890, v.getFieldByKey("c").getInt());
    });
  }

  @Test
  public void testObjectTwoByteOffset() {
    // a string larger than 255 bytes to push the offset size above 1 byte
    testObjectOffsetSize(randomString(300));
  }

  @Test
  public void testObjectThreeByteOffset() {
    // a string larger than 65535 bytes to push the offset size above 2 bytes
    testObjectOffsetSize(randomString(70_000));
  }

  @Test
  public void testObjectFourByteOffset() {
    // a string larger than 16777215 bytes to push the offset size above 3 bytes
    testObjectOffsetSize(randomString(16_800_000));
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
    testVariant(value, v -> {
      checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(2, v.numObjectElements());
      checkType(v.getFieldByKey("z1"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getFieldByKey("z1").getBoolean());
      checkType(v.getFieldByKey("z2"), VariantUtil.PRIMITIVE, Variant.Type.INT);
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

  @Test
  public void testLargeObject() {
    Map<String, Integer> keys = new HashMap<>();
    Map<String, byte[]> fields = new HashMap<>();
    for (int i = 0; i < 1000; i++) {
      String name = String.format("a%04d", i);
      keys.put(name, i);
      fields.put(name, constructString(randomString(5)));
    }

    List<String> sortedKeys = new ArrayList<>(keys.keySet());
    Collections.sort(sortedKeys);

    Variant value =
        new Variant(ByteBuffer.wrap(constructObject(keys, fields, false)), constructMetadata(true, sortedKeys));
    testVariant(value, v -> {
      checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(1000, v.numObjectElements());

      for (int i = 0; i < 1000; i++) {
        String name = String.format("a%04d", i);
        checkType(v.getFieldByKey(name), VariantUtil.PRIMITIVE, Variant.Type.STRING);
        Assert.assertEquals(
            new String(fields.get(name), 5, fields.get(name).length - 5),
            v.getFieldByKey(name).getString());
      }
    });
  }
}
