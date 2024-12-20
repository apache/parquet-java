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

import com.beust.jcommander.internal.Maps;
import com.fasterxml.jackson.core.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Consumer;
import org.apache.commons.compress.utils.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestVariantEncoding {
  private static final Logger LOG = LoggerFactory.getLogger(TestVariantEncoding.class);
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

  private void checkType(Variant v, int expectedBasicType, VariantUtil.Type expectedType) {
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
  public void testNull() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {primitiveHeader(0)}), EMPTY_METADATA);
    testVariant(value, v -> checkType(v, VariantUtil.NULL, VariantUtil.Type.NULL));
  }

  @Test
  public void testTrue() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {primitiveHeader(1)}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.BOOLEAN);
      Assert.assertTrue(v.getBoolean());
    });
  }

  @Test
  public void testFalse() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {primitiveHeader(2)}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.BOOLEAN);
      Assert.assertFalse(v.getBoolean());
    });
  }

  @Test
  public void testLong() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          primitiveHeader(6), (byte) 0xB1, 0x1C, 0x6C, (byte) 0xB1, (byte) 0xF4, 0x10, 0x22, 0x11
        }),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.LONG);
      Assert.assertEquals(1234567890987654321L, v.getLong());
    });
  }

  @Test
  public void testNegativeLong() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          primitiveHeader(6),
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF
        }),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.LONG);
      Assert.assertEquals(-1L, v.getLong());
    });
  }

  @Test
  public void testInt() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {primitiveHeader(5), (byte) 0xD2, 0x02, (byte) 0x96, 0x49}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.INT);
      Assert.assertEquals(1234567890, v.getInt());
    });
  }

  @Test
  public void testNegativeInt() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {primitiveHeader(5), (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.INT);
      Assert.assertEquals(-1, v.getInt());
    });
  }

  @Test
  public void testShort() {
    Variant value =
        new Variant(ByteBuffer.wrap(new byte[] {primitiveHeader(4), (byte) 0xD2, 0x04}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.SHORT);
      Assert.assertEquals((short) 1234, v.getShort());
    });
  }

  @Test
  public void testNegativeShort() {
    Variant value =
        new Variant(ByteBuffer.wrap(new byte[] {primitiveHeader(4), (byte) 0xFF, (byte) 0xFF}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.SHORT);
      Assert.assertEquals((short) -1, v.getShort());
    });
  }

  @Test
  public void testByte() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {primitiveHeader(3), 34}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.BYTE);
      Assert.assertEquals((byte) 34, v.getByte());
    });
  }

  @Test
  public void testNegativeByte() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {primitiveHeader(3), (byte) 0xFF}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.BYTE);
      Assert.assertEquals((byte) -1, v.getByte());
    });
  }

  @Test
  public void testFloat() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {primitiveHeader(14), (byte) 0xD2, 0x02, (byte) 0x96, 0x49}),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.FLOAT);
      Assert.assertEquals(Float.intBitsToFloat(1234567890), v.getFloat(), 0);
    });
  }

  @Test
  public void testNegativeFloat() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {primitiveHeader(14), 0x00, 0x00, 0x00, (byte) 0x80}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.FLOAT);
      Assert.assertEquals(-0.0F, v.getFloat(), 0);
    });
  }

  @Test
  public void testDouble() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          primitiveHeader(7), (byte) 0xB1, 0x1C, 0x6C, (byte) 0xB1, (byte) 0xF4, 0x10, 0x22, 0x11
        }),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.DOUBLE);
      Assert.assertEquals(Double.longBitsToDouble(1234567890987654321L), v.getDouble(), 0);
    });
  }

  @Test
  public void testNegativeDouble() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {primitiveHeader(7), 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (byte) 0x80}),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.DOUBLE);
      Assert.assertEquals(-0.0D, v.getDouble(), 0);
    });
  }

  @Test
  public void testDecimal4() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {primitiveHeader(8), 0x04, (byte) 0xD2, 0x02, (byte) 0x96, 0x49}),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.DECIMAL4);
      Assert.assertEquals(new BigDecimal("123456.7890"), v.getDecimal());
    });
  }

  @Test
  public void testNegativeDecimal4() {
    Variant value = new Variant(
        ByteBuffer.wrap(
            new byte[] {primitiveHeader(8), 0x04, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.DECIMAL4);
      Assert.assertEquals(new BigDecimal("-0.0001"), v.getDecimal());
    });
  }

  @Test
  public void testDecimal8() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          primitiveHeader(9), 0x09, (byte) 0xB1, 0x1C, 0x6C, (byte) 0xB1, (byte) 0xF4, 0x10, 0x22, 0x11
        }),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.DECIMAL8);
      Assert.assertEquals(new BigDecimal("1234567890.987654321"), v.getDecimal());
    });
  }

  @Test
  public void testNegativeDecimal8() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          primitiveHeader(9),
          0x09,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF
        }),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.DECIMAL8);
      Assert.assertEquals(new BigDecimal("-0.000000001"), v.getDecimal());
    });
  }

  @Test
  public void testDecimal16() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          primitiveHeader(10),
          0x09,
          0x15,
          0x71,
          0x34,
          (byte) 0xB0,
          (byte) 0xB8,
          (byte) 0x87,
          0x10,
          (byte) 0x89,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00
        }),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.DECIMAL16);
      Assert.assertEquals(new BigDecimal("9876543210.123456789"), v.getDecimal());
    });
  }

  @Test
  public void testNegativeDecimal16() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          primitiveHeader(10),
          0x09,
          (byte) 0xEB,
          (byte) 0x8E,
          (byte) 0xCB,
          0x4F,
          0x47,
          0x78,
          (byte) 0xEF,
          0x76,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF
        }),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.DECIMAL16);
      Assert.assertEquals(new BigDecimal("-9876543210.123456789"), v.getDecimal());
    });
  }

  @Test
  public void testDate() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {primitiveHeader(11), (byte) 0xE3, 0x4E, 0x00, 0x00}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.DATE);
      Assert.assertEquals(LocalDate.parse("2025-04-17"), LocalDate.ofEpochDay(v.getInt()));
    });
  }

  @Test
  public void testNegativeDate() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {primitiveHeader(11), (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.DATE);
      Assert.assertEquals(LocalDate.parse("1969-12-31"), LocalDate.ofEpochDay(v.getInt()));
    });
  }

  @Test
  public void testTimestamp() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          primitiveHeader(12), (byte) 0xC0, 0x77, (byte) 0xA1, (byte) 0xEA, (byte) 0xF4, 0x32, 0x06, 0x00
        }),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.TIMESTAMP);
      Assert.assertEquals(
          Instant.parse("2025-04-17T08:09:10.123456+00:00"),
          Instant.EPOCH.plus(v.getLong(), ChronoUnit.MICROS));
    });
  }

  @Test
  public void testNegativeTimestamp() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          primitiveHeader(12),
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF
        }),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.TIMESTAMP);
      Assert.assertEquals(
          Instant.parse("1969-12-31T23:59:59.999999+00:00"),
          Instant.EPOCH.plus(v.getLong(), ChronoUnit.MICROS));
    });
  }

  @Test
  public void testTimestampNtz() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          primitiveHeader(13), (byte) 0xC0, 0x77, (byte) 0xA1, (byte) 0xEA, (byte) 0xF4, 0x32, 0x06, 0x00
        }),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.TIMESTAMP_NTZ);
      Assert.assertEquals(
          Instant.parse("2025-04-17T08:09:10.123456+00:00"),
          Instant.EPOCH.plus(v.getLong(), ChronoUnit.MICROS));
    });
  }

  @Test
  public void testNegativeTimestampNtz() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          primitiveHeader(13),
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF
        }),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.TIMESTAMP_NTZ);
      Assert.assertEquals(
          Instant.parse("1969-12-31T23:59:59.999999+00:00"),
          Instant.EPOCH.plus(v.getLong(), ChronoUnit.MICROS));
    });
  }

  @Test
  public void testBinary() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {primitiveHeader(15), 0x05, 0x00, 0x00, 0x00, 'a', 'b', 'c', 'd', 'e'}),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.BINARY);
      Assert.assertArrayEquals(new byte[] {'a', 'b', 'c', 'd', 'e'}, v.getBinary());
    });
  }

  @Test
  public void testString() {
    Variant value = new Variant(
        ByteBuffer.wrap(
            new byte[] {primitiveHeader(16), 0x07, 0x00, 0x00, 0x00, 'v', 'a', 'r', 'i', 'a', 'n', 't'}),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.STRING);
      Assert.assertEquals("variant", v.getString());
    });
  }

  @Test
  public void testShortString() {
    Variant value =
        new Variant(ByteBuffer.wrap(new byte[] {0b11101, 'v', 'a', 'r', 'i', 'a', 'n', 't'}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.SHORT_STR, VariantUtil.Type.STRING);
      Assert.assertEquals("variant", v.getString());
    });
  }

  @Test
  public void testTime() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          primitiveHeader(17),
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0xCA,
          (byte) 0x1D,
          (byte) 0x14,
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0x00
        }),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.TIME);
      Assert.assertEquals(LocalTime.parse("23:59:59.123456"), LocalTime.ofNanoOfDay(v.getLong() * 1_000));
    });
  }

  @Test
  public void testTimestampNanos() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          primitiveHeader(18), 0x15, (byte) 0xC9, (byte) 0xBB, (byte) 0x86, (byte) 0xB4, 0x0C, 0x37, 0x18
        }),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.TIMESTAMP_NANOS);
      Assert.assertEquals(
          Instant.parse("2025-04-17T08:09:10.123456789+00:00"),
          Instant.EPOCH.plus(v.getLong(), ChronoUnit.NANOS));
    });
  }

  @Test
  public void testNegativeTimestampNanos() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          primitiveHeader(18),
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF
        }),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.TIMESTAMP_NANOS);
      Assert.assertEquals(
          Instant.parse("1969-12-31T23:59:59.999999999+00:00"),
          Instant.EPOCH.plus(v.getLong(), ChronoUnit.NANOS));
    });
  }

  @Test
  public void testTimestampNanosNtz() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          primitiveHeader(19), 0x15, (byte) 0xC9, (byte) 0xBB, (byte) 0x86, (byte) 0xB4, 0x0C, 0x37, 0x18
        }),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.TIMESTAMP_NANOS_NTZ);
      Assert.assertEquals(
          Instant.parse("2025-04-17T08:09:10.123456789+00:00"),
          Instant.EPOCH.plus(v.getLong(), ChronoUnit.NANOS));
    });
  }

  @Test
  public void testNegativeTimestampNanosNtz() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          primitiveHeader(19),
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF
        }),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.TIMESTAMP_NANOS_NTZ);
      Assert.assertEquals(
          Instant.parse("1969-12-31T23:59:59.999999999+00:00"),
          Instant.EPOCH.plus(v.getLong(), ChronoUnit.NANOS));
    });
  }

  @Test
  public void testUUID() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          primitiveHeader(20),
          0x00,
          0x11,
          0x22,
          0x33,
          0x44,
          0x55,
          0x66,
          0x77,
          (byte) 0x88,
          (byte) 0x99,
          (byte) 0xAA,
          (byte) 0xBB,
          (byte) 0xCC,
          (byte) 0xDD,
          (byte) 0xEE,
          (byte) 0xFF
        }),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.UUID);
      Assert.assertEquals(UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"), v.getUUID());
    });
  }

  @Test
  public void testEmptyArray() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {0b0011, 0x00}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.ARRAY, VariantUtil.Type.ARRAY);
      Assert.assertEquals(0, v.numArrayElements());
    });
  }

  @Test
  public void testEmptyLargeArray() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {0b10011, 0x00, 0x00, 0x00, 0x00}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.ARRAY, VariantUtil.Type.ARRAY);
      Assert.assertEquals(0, v.numArrayElements());
    });
  }

  @Test
  public void testLargeArraySize() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {0b10011, (byte) 0xFF, (byte) 0x01, 0x00, 0x00}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.ARRAY, VariantUtil.Type.ARRAY);
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
      checkType(v, VariantUtil.ARRAY, VariantUtil.Type.ARRAY);
      Assert.assertEquals(5, v.numArrayElements());
      checkType(v.getElementAtIndex(0), VariantUtil.PRIMITIVE, VariantUtil.Type.DATE);
      Assert.assertEquals(
          LocalDate.parse("2025-04-17"),
          LocalDate.ofEpochDay(v.getElementAtIndex(0).getInt()));
      checkType(v.getElementAtIndex(1), VariantUtil.PRIMITIVE, VariantUtil.Type.BOOLEAN);
      Assert.assertTrue(v.getElementAtIndex(1).getBoolean());
      checkType(v.getElementAtIndex(2), VariantUtil.PRIMITIVE, VariantUtil.Type.INT);
      Assert.assertEquals(1234567890, v.getElementAtIndex(2).getInt());
      checkType(v.getElementAtIndex(3), VariantUtil.PRIMITIVE, VariantUtil.Type.STRING);
      Assert.assertEquals("variant", v.getElementAtIndex(3).getString());
      checkType(v.getElementAtIndex(4), VariantUtil.ARRAY, VariantUtil.Type.ARRAY);

      Variant nestedV = v.getElementAtIndex(4);
      Assert.assertEquals(3, nestedV.numArrayElements());
      checkType(nestedV.getElementAtIndex(0), VariantUtil.PRIMITIVE, VariantUtil.Type.INT);
      Assert.assertEquals(1234567890, nestedV.getElementAtIndex(0).getInt());
      checkType(nestedV.getElementAtIndex(1), VariantUtil.PRIMITIVE, VariantUtil.Type.NULL);
      checkType(nestedV.getElementAtIndex(2), VariantUtil.SHORT_STR, VariantUtil.Type.STRING);
      Assert.assertEquals("c", nestedV.getElementAtIndex(2).getString());
    });
  }

  public void testArrayOffsetSize(String randomString) {
    Variant value = new Variant(
        ByteBuffer.wrap(constructArray(constructString(randomString), VALUE_BOOL, VALUE_INT)), EMPTY_METADATA);

    testVariant(value, v -> {
      checkType(v, VariantUtil.ARRAY, VariantUtil.Type.ARRAY);
      Assert.assertEquals(3, v.numArrayElements());
      checkType(v.getElementAtIndex(0), VariantUtil.PRIMITIVE, VariantUtil.Type.STRING);
      Assert.assertEquals(randomString, v.getElementAtIndex(0).getString());
      checkType(v.getElementAtIndex(1), VariantUtil.PRIMITIVE, VariantUtil.Type.BOOLEAN);
      Assert.assertTrue(v.getElementAtIndex(1).getBoolean());
      checkType(v.getElementAtIndex(2), VariantUtil.PRIMITIVE, VariantUtil.Type.INT);
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
  public void testEmptyObject() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {0b10, 0x00}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.OBJECT, VariantUtil.Type.OBJECT);
      Assert.assertEquals(0, v.numObjectElements());
    });
  }

  @Test
  public void testEmptyLargeObject() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {0b1000010, 0x00, 0x00, 0x00, 0x00}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.OBJECT, VariantUtil.Type.OBJECT);
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
      checkType(v, VariantUtil.OBJECT, VariantUtil.Type.OBJECT);
      Assert.assertEquals(3, v.numObjectElements());
      checkType(v.getFieldByKey("a"), VariantUtil.PRIMITIVE, VariantUtil.Type.INT);
      Assert.assertEquals(1234567890, v.getFieldByKey("a").getInt());
      checkType(v.getFieldByKey("b"), VariantUtil.PRIMITIVE, VariantUtil.Type.BOOLEAN);
      Assert.assertTrue(v.getFieldByKey("b").getBoolean());
      checkType(v.getFieldByKey("c"), VariantUtil.PRIMITIVE, VariantUtil.Type.STRING);
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
      checkType(v, VariantUtil.OBJECT, VariantUtil.Type.OBJECT);
      Assert.assertEquals(3, v.numObjectElements());
      checkType(v.getFieldByKey("a"), VariantUtil.PRIMITIVE, VariantUtil.Type.INT);
      Assert.assertEquals(1234567890, v.getFieldByKey("a").getInt());
      checkType(v.getFieldByKey("b"), VariantUtil.PRIMITIVE, VariantUtil.Type.BOOLEAN);
      Assert.assertTrue(v.getFieldByKey("b").getBoolean());
      checkType(v.getFieldByKey("c"), VariantUtil.OBJECT, VariantUtil.Type.OBJECT);

      Variant nestedV = v.getFieldByKey("c");
      Assert.assertEquals(2, nestedV.numObjectElements());
      checkType(nestedV.getFieldByKey("a"), VariantUtil.PRIMITIVE, VariantUtil.Type.DATE);
      Assert.assertEquals(
          LocalDate.parse("2025-04-17"),
          LocalDate.ofEpochDay(nestedV.getFieldByKey("a").getInt()));
      checkType(nestedV.getFieldByKey("c"), VariantUtil.PRIMITIVE, VariantUtil.Type.NULL);
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
      checkType(v, VariantUtil.OBJECT, VariantUtil.Type.OBJECT);
      Assert.assertEquals(3, v.numObjectElements());
      checkType(v.getFieldByKey("a"), VariantUtil.PRIMITIVE, VariantUtil.Type.INT);
      Assert.assertEquals(1234567890, v.getFieldByKey("a").getInt());
      checkType(v.getFieldByKey("b"), VariantUtil.PRIMITIVE, VariantUtil.Type.BOOLEAN);
      Assert.assertTrue(v.getFieldByKey("b").getBoolean());
      checkType(v.getFieldByKey("c"), VariantUtil.PRIMITIVE, VariantUtil.Type.STRING);
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
      checkType(v, VariantUtil.OBJECT, VariantUtil.Type.OBJECT);
      Assert.assertEquals(3, v.numObjectElements());
      checkType(v.getFieldByKey("a"), VariantUtil.PRIMITIVE, VariantUtil.Type.STRING);
      Assert.assertEquals(randomString, v.getFieldByKey("a").getString());
      checkType(v.getFieldByKey("b"), VariantUtil.PRIMITIVE, VariantUtil.Type.BOOLEAN);
      Assert.assertTrue(v.getFieldByKey("b").getBoolean());
      checkType(v.getFieldByKey("c"), VariantUtil.PRIMITIVE, VariantUtil.Type.INT);
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
    List<String> fieldNames = Lists.newArrayList();
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
      checkType(v, VariantUtil.OBJECT, VariantUtil.Type.OBJECT);
      Assert.assertEquals(2, v.numObjectElements());
      checkType(v.getFieldByKey("z1"), VariantUtil.PRIMITIVE, VariantUtil.Type.BOOLEAN);
      Assert.assertTrue(v.getFieldByKey("z1").getBoolean());
      checkType(v.getFieldByKey("z2"), VariantUtil.PRIMITIVE, VariantUtil.Type.INT);
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
    Map<String, Integer> keys = Maps.newHashMap();
    Map<String, byte[]> fields = Maps.newHashMap();
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
      checkType(v, VariantUtil.OBJECT, VariantUtil.Type.OBJECT);
      Assert.assertEquals(1000, v.numObjectElements());

      for (int i = 0; i < 1000; i++) {
        String name = String.format("a%04d", i);
        checkType(v.getFieldByKey(name), VariantUtil.PRIMITIVE, VariantUtil.Type.STRING);
        Assert.assertEquals(
            new String(fields.get(name), 5, fields.get(name).length - 5),
            v.getFieldByKey(name).getString());
      }
    });
  }
}
