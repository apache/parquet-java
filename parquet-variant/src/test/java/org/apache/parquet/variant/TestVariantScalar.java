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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Consumer;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestVariantScalar {
  private static final Logger LOG = LoggerFactory.getLogger(TestVariantScalar.class);

  private static final ByteBuffer EMPTY_METADATA = ByteBuffer.wrap(new byte[] {0b1});

  private void checkType(Variant v, int expectedBasicType, Variant.Type expectedType) {
    Assert.assertEquals(expectedBasicType, v.value.get(v.value.position()) & VariantUtil.BASIC_TYPE_MASK);
    Assert.assertEquals(expectedType, v.getType());
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

  @Test
  public void testNull() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {primitiveHeader(0)}), EMPTY_METADATA);
    testVariant(value, v -> checkType(v, VariantUtil.NULL, Variant.Type.NULL));
  }

  @Test
  public void testTrue() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {primitiveHeader(1)}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getBoolean());
    });
  }

  @Test
  public void testFalse() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {primitiveHeader(2)}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
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
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.LONG);
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
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.LONG);
      Assert.assertEquals(-1L, v.getLong());
    });
  }

  @Test
  public void testInt() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {primitiveHeader(5), (byte) 0xD2, 0x02, (byte) 0x96, 0x49}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(1234567890, v.getInt());
    });
  }

  @Test
  public void testNegativeInt() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {primitiveHeader(5), (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(-1, v.getInt());
    });
  }

  @Test
  public void testShort() {
    Variant value =
        new Variant(ByteBuffer.wrap(new byte[] {primitiveHeader(4), (byte) 0xD2, 0x04}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.SHORT);
      Assert.assertEquals((short) 1234, v.getShort());
    });
  }

  @Test
  public void testNegativeShort() {
    Variant value =
        new Variant(ByteBuffer.wrap(new byte[] {primitiveHeader(4), (byte) 0xFF, (byte) 0xFF}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.SHORT);
      Assert.assertEquals((short) -1, v.getShort());
    });
  }

  @Test
  public void testByte() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {primitiveHeader(3), 34}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BYTE);
      Assert.assertEquals((byte) 34, v.getByte());
    });
  }

  @Test
  public void testNegativeByte() {
    Variant value = new Variant(ByteBuffer.wrap(new byte[] {primitiveHeader(3), (byte) 0xFF}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BYTE);
      Assert.assertEquals((byte) -1, v.getByte());
    });
  }

  @Test
  public void testFloat() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {primitiveHeader(14), (byte) 0xD2, 0x02, (byte) 0x96, 0x49}),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.FLOAT);
      Assert.assertEquals(Float.intBitsToFloat(1234567890), v.getFloat(), 0);
    });
  }

  @Test
  public void testNegativeFloat() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {primitiveHeader(14), 0x00, 0x00, 0x00, (byte) 0x80}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.FLOAT);
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
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DOUBLE);
      Assert.assertEquals(Double.longBitsToDouble(1234567890987654321L), v.getDouble(), 0);
    });
  }

  @Test
  public void testNegativeDouble() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {primitiveHeader(7), 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (byte) 0x80}),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DOUBLE);
      Assert.assertEquals(-0.0D, v.getDouble(), 0);
    });
  }

  @Test
  public void testDecimal4() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {primitiveHeader(8), 0x04, (byte) 0xD2, 0x02, (byte) 0x96, 0x49}),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL4);
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
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL4);
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
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL8);
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
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL8);
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
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL16);
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
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL16);
      Assert.assertEquals(new BigDecimal("-9876543210.123456789"), v.getDecimal());
    });
  }

  @Test
  public void testDate() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {primitiveHeader(11), (byte) 0xE3, 0x4E, 0x00, 0x00}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DATE);
      Assert.assertEquals(LocalDate.parse("2025-04-17"), LocalDate.ofEpochDay(v.getInt()));
    });
  }

  @Test
  public void testNegativeDate() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {primitiveHeader(11), (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DATE);
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
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_TZ);
      Assert.assertEquals(
          Instant.parse("2025-04-17T08:09:10.123456Z"), Instant.EPOCH.plus(v.getLong(), ChronoUnit.MICROS));
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
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_TZ);
      Assert.assertEquals(
          Instant.parse("1969-12-31T23:59:59.999999Z"), Instant.EPOCH.plus(v.getLong(), ChronoUnit.MICROS));
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
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NTZ);
      Assert.assertEquals(
          Instant.parse("2025-04-17T08:09:10.123456Z"), Instant.EPOCH.plus(v.getLong(), ChronoUnit.MICROS));
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
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NTZ);
      Assert.assertEquals(
          Instant.parse("1969-12-31T23:59:59.999999Z"), Instant.EPOCH.plus(v.getLong(), ChronoUnit.MICROS));
    });
  }

  @Test
  public void testBinary() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {primitiveHeader(15), 0x05, 0x00, 0x00, 0x00, 'a', 'b', 'c', 'd', 'e'}),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BINARY);
      Assert.assertEquals(ByteBuffer.wrap(new byte[] {'a', 'b', 'c', 'd', 'e'}), v.getBinary());
    });
  }

  @Test
  public void testString() {
    Variant value = new Variant(
        ByteBuffer.wrap(
            new byte[] {primitiveHeader(16), 0x07, 0x00, 0x00, 0x00, 'v', 'a', 'r', 'i', 'a', 'n', 't'}),
        EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.STRING);
      Assert.assertEquals("variant", v.getString());
    });
  }

  @Test
  public void testShortString() {
    Variant value =
        new Variant(ByteBuffer.wrap(new byte[] {0b11101, 'v', 'a', 'r', 'i', 'a', 'n', 't'}), EMPTY_METADATA);
    testVariant(value, v -> {
      checkType(v, VariantUtil.SHORT_STR, Variant.Type.STRING);
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
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIME);
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
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS);
      Assert.assertEquals(
          Instant.parse("2025-04-17T08:09:10.123456789Z"), Instant.EPOCH.plus(v.getLong(), ChronoUnit.NANOS));
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
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS);
      Assert.assertEquals(
          Instant.parse("1969-12-31T23:59:59.999999999Z"), Instant.EPOCH.plus(v.getLong(), ChronoUnit.NANOS));
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
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS_NTZ);
      Assert.assertEquals(
          Instant.parse("2025-04-17T08:09:10.123456789Z"), Instant.EPOCH.plus(v.getLong(), ChronoUnit.NANOS));
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
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS_NTZ);
      Assert.assertEquals(
          Instant.parse("1969-12-31T23:59:59.999999999Z"), Instant.EPOCH.plus(v.getLong(), ChronoUnit.NANOS));
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
      checkType(v, VariantUtil.PRIMITIVE, Variant.Type.UUID);
      Assert.assertEquals(UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"), v.getUUID());
    });
  }
}
