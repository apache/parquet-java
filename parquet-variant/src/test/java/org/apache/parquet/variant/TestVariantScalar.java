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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestVariantScalar {
  private static final Logger LOG = LoggerFactory.getLogger(TestVariantScalar.class);

  @Test
  public void testNull() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(0)}), VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> VariantTestUtil.checkType(v, VariantUtil.NULL, Variant.Type.NULL));
  }

  @Test
  public void testTrue() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(1)}), VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      assertThat(v.getBoolean()).isTrue();
    });
  }

  @Test
  public void testFalse() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(2)}), VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      assertThat(v.getBoolean()).isFalse();
    });
  }

  @Test
  public void testLong() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(6),
          (byte) 0xB1,
          0x1C,
          0x6C,
          (byte) 0xB1,
          (byte) 0xF4,
          0x10,
          0x22,
          0x11
        }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.LONG);
      assertThat(v.getLong()).isEqualTo(1234567890987654321L);
    });
  }

  @Test
  public void testNegativeLong() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(6),
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF
        }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.LONG);
      assertThat(v.getLong()).isEqualTo(-1L);
    });
  }

  @Test
  public void testInt() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(5), (byte) 0xD2, 0x02, (byte) 0x96, 0x49}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.INT);
      assertThat(v.getInt()).isEqualTo(1234567890);
    });
  }

  @Test
  public void testNegativeInt() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(5), (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
        }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.INT);
      assertThat(v.getInt()).isEqualTo(-1);
    });
  }

  @Test
  public void testShort() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(4), (byte) 0xD2, 0x04}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.SHORT);
      assertThat(v.getShort()).isEqualTo((short) 1234);
    });
  }

  @Test
  public void testNegativeShort() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(4), (byte) 0xFF, (byte) 0xFF}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.SHORT);
      assertThat(v.getShort()).isEqualTo((short) -1);
    });
  }

  @Test
  public void testByte() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(3), 34}), VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BYTE);
      assertThat(v.getByte()).isEqualTo((byte) 34);
    });
  }

  @Test
  public void testNegativeByte() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(3), (byte) 0xFF}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BYTE);
      assertThat(v.getByte()).isEqualTo((byte) -1);
    });
  }

  @Test
  public void testFloat() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(14), (byte) 0xD2, 0x02, (byte) 0x96, 0x49}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.FLOAT);
      assertThat(v.getFloat()).isEqualTo(Float.intBitsToFloat(1234567890));
    });
  }

  @Test
  public void testNegativeFloat() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(14), 0x00, 0x00, 0x00, (byte) 0x80}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.FLOAT);
      assertThat(v.getFloat()).isEqualTo(-0.0F);
    });
  }

  @Test
  public void testDouble() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(7),
          (byte) 0xB1,
          0x1C,
          0x6C,
          (byte) 0xB1,
          (byte) 0xF4,
          0x10,
          0x22,
          0x11
        }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DOUBLE);
      assertThat(v.getDouble()).isEqualTo(Double.longBitsToDouble(1234567890987654321L));
    });
  }

  @Test
  public void testNegativeDouble() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(7), 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (byte) 0x80
        }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DOUBLE);
      assertThat(v.getDouble()).isEqualTo(-0.0D);
    });
  }

  @Test
  public void testDecimal4() {
    Variant value = new Variant(
        ByteBuffer.wrap(
            new byte[] {VariantTestUtil.primitiveHeader(8), 0x04, (byte) 0xD2, 0x02, (byte) 0x96, 0x49}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL4);
      assertThat(v.getDecimal()).isEqualTo(new BigDecimal("123456.7890"));
    });
  }

  @Test
  public void testNegativeDecimal4() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(8), 0x04, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
        }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL4);
      assertThat(v.getDecimal()).isEqualTo(new BigDecimal("-0.0001"));
    });
  }

  @Test
  public void testDecimal8() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(9),
          0x09,
          (byte) 0xB1,
          0x1C,
          0x6C,
          (byte) 0xB1,
          (byte) 0xF4,
          0x10,
          0x22,
          0x11
        }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL8);
      assertThat(v.getDecimal()).isEqualTo(new BigDecimal("1234567890.987654321"));
    });
  }

  @Test
  public void testNegativeDecimal8() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(9),
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
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL8);
      assertThat(v.getDecimal()).isEqualTo(new BigDecimal("-0.000000001"));
    });
  }

  @Test
  public void testDecimal16() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(10),
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
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL16);
      assertThat(v.getDecimal()).isEqualTo(new BigDecimal("9876543210.123456789"));
    });
  }

  @Test
  public void testNegativeDecimal16() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(10),
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
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL16);
      assertThat(v.getDecimal()).isEqualTo(new BigDecimal("-9876543210.123456789"));
    });
  }

  @Test
  public void testDate() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(11), (byte) 0xE3, 0x4E, 0x00, 0x00}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DATE);
      assertThat(LocalDate.ofEpochDay(v.getInt())).isEqualTo(LocalDate.parse("2025-04-17"));
    });
  }

  @Test
  public void testNegativeDate() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(11), (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
        }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DATE);
      assertThat(LocalDate.ofEpochDay(v.getInt())).isEqualTo(LocalDate.parse("1969-12-31"));
    });
  }

  @Test
  public void testTimestampTz() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(12),
          (byte) 0xC0,
          0x77,
          (byte) 0xA1,
          (byte) 0xEA,
          (byte) 0xF4,
          0x32,
          0x06,
          0x00
        }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_TZ);
      assertThat(Instant.EPOCH.plus(v.getLong(), ChronoUnit.MICROS))
          .isEqualTo(Instant.parse("2025-04-17T08:09:10.123456Z"));
    });
  }

  @Test
  public void testNegativeTimestampTz() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(12),
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF
        }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_TZ);
      assertThat(Instant.EPOCH.plus(v.getLong(), ChronoUnit.MICROS))
          .isEqualTo(Instant.parse("1969-12-31T23:59:59.999999Z"));
    });
  }

  @Test
  public void testTimestampNtz() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(13),
          (byte) 0xC0,
          0x77,
          (byte) 0xA1,
          (byte) 0xEA,
          (byte) 0xF4,
          0x32,
          0x06,
          0x00
        }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NTZ);
      assertThat(Instant.EPOCH.plus(v.getLong(), ChronoUnit.MICROS))
          .isEqualTo(Instant.parse("2025-04-17T08:09:10.123456Z"));
    });
  }

  @Test
  public void testNegativeTimestampNtz() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(13),
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF
        }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NTZ);
      assertThat(Instant.EPOCH.plus(v.getLong(), ChronoUnit.MICROS))
          .isEqualTo(Instant.parse("1969-12-31T23:59:59.999999Z"));
    });
  }

  @Test
  public void testBinary() {
    Variant value = new Variant(
        ByteBuffer.wrap(
            new byte[] {VariantTestUtil.primitiveHeader(15), 0x05, 0x00, 0x00, 0x00, 'a', 'b', 'c', 'd', 'e'
            }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BINARY);
      assertThat(v.getBinary()).isEqualTo(ByteBuffer.wrap(new byte[] {'a', 'b', 'c', 'd', 'e'}));
    });
  }

  @Test
  public void testString() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(16), 0x07, 0x00, 0x00, 0x00, 'v', 'a', 'r', 'i', 'a', 'n', 't'
        }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.STRING);
      assertThat(v.getString()).isEqualTo("variant");
    });
  }

  @Test
  public void testShortString() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {0b11101, 'v', 'a', 'r', 'i', 'a', 'n', 't'}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.SHORT_STR, Variant.Type.STRING);
      assertThat(v.getString()).isEqualTo("variant");
    });
  }

  @Test
  public void testTime() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(17),
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0xCA,
          (byte) 0x1D,
          (byte) 0x14,
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0x00
        }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIME);
      assertThat(LocalTime.ofNanoOfDay(v.getLong() * 1_000)).isEqualTo(LocalTime.parse("23:59:59.123456"));
    });
  }

  @Test
  public void testTimestampNanosTz() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(18),
          0x15,
          (byte) 0xC9,
          (byte) 0xBB,
          (byte) 0x86,
          (byte) 0xB4,
          0x0C,
          0x37,
          0x18
        }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS_TZ);
      assertThat(Instant.EPOCH.plus(v.getLong(), ChronoUnit.NANOS))
          .isEqualTo(Instant.parse("2025-04-17T08:09:10.123456789Z"));
    });
  }

  @Test
  public void testNegativeTimestampNanosTz() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(18),
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF
        }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS_TZ);
      assertThat(Instant.EPOCH.plus(v.getLong(), ChronoUnit.NANOS))
          .isEqualTo(Instant.parse("1969-12-31T23:59:59.999999999Z"));
    });
  }

  @Test
  public void testTimestampNanosNtz() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(19),
          0x15,
          (byte) 0xC9,
          (byte) 0xBB,
          (byte) 0x86,
          (byte) 0xB4,
          0x0C,
          0x37,
          0x18
        }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS_NTZ);
      assertThat(Instant.EPOCH.plus(v.getLong(), ChronoUnit.NANOS))
          .isEqualTo(Instant.parse("2025-04-17T08:09:10.123456789Z"));
    });
  }

  @Test
  public void testNegativeTimestampNanosNtz() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(19),
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF
        }),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS_NTZ);
      assertThat(Instant.EPOCH.plus(v.getLong(), ChronoUnit.NANOS))
          .isEqualTo(Instant.parse("1969-12-31T23:59:59.999999999Z"));
    });
  }

  @Test
  public void testUUID() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {
          VariantTestUtil.primitiveHeader(20),
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
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.UUID);
      assertThat(v.getUUID()).isEqualTo(UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"));
    });
  }

  @Test
  public void testInvalidType() {
    assertThatThrownBy(() -> new Variant(ByteBuffer.wrap(new byte[] {(byte) 0xFC}), VariantTestUtil.EMPTY_METADATA))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown primitive type in variant: 63");
  }

  @Test
  public void testInvalidBoolean() {
    assertVariantLongReadInvalid("Cannot read LONG value as BOOLEAN", Variant::getBoolean);
  }

  @Test
  public void testInvalidLong() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(16), 0, 0, 0, 0}),
        VariantTestUtil.EMPTY_METADATA);
    assertThatThrownBy(value::getLong)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot read STRING value as one of [BYTE, SHORT, INT, DATE, LONG, TIMESTAMP_TZ, TIMESTAMP_NTZ, TIME, TIMESTAMP_NANOS_TZ, TIMESTAMP_NANOS_NTZ]");
  }

  @Test
  public void testInvalidInt() {
    assertVariantLongReadInvalid("Cannot read LONG value as one of [BYTE, SHORT, INT, DATE]", Variant::getInt);
  }

  @Test
  public void testInvalidShort() {
    assertVariantLongReadInvalid("Cannot read LONG value as one of [BYTE, SHORT]", Variant::getShort);
  }

  @Test
  public void testInvalidByte() {
    assertVariantLongReadInvalid("Cannot read LONG value as BYTE", Variant::getByte);
  }

  @Test
  public void testInvalidFloat() {
    assertVariantLongReadInvalid("Cannot read LONG value as FLOAT", Variant::getFloat);
  }

  @Test
  public void testInvalidDouble() {
    assertVariantLongReadInvalid("Cannot read LONG value as DOUBLE", Variant::getDouble);
  }

  @Test
  public void testInvalidDecimal() {
    assertVariantLongReadInvalid(
        "Cannot read LONG value as one of [DECIMAL4, DECIMAL8, DECIMAL16]", Variant::getDecimal);
  }

  @Test
  public void testInvalidUUID() {
    assertVariantLongReadInvalid("Cannot read LONG value as UUID", Variant::getUUID);
  }

  @Test
  public void testInvalidString() {
    assertVariantLongReadInvalid("Cannot read LONG value as STRING", Variant::getString);
  }

  @Test
  public void testInvalidBinary() {
    assertVariantLongReadInvalid("Cannot read LONG value as BINARY", Variant::getBinary);
  }

  /**
   * Assert that reading a long value variant raises an IllegalArgumentException.
   * @param expectedErrorText expected text.
   * @param consumer variant consumer.
   */
  private static void assertVariantLongReadInvalid(String expectedErrorText, Consumer<Variant> consumer) {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(6), 0, 0, 0, 0, 0, 0, 0, 0}),
        VariantTestUtil.EMPTY_METADATA);
    assertThatThrownBy(() -> consumer.accept(value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedErrorText);
  }
}
