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
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;
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
  public void testNullBuilder() {
    VariantBuilder vb = new VariantBuilder(false);
    vb.appendNull();
    VariantTestUtil.testVariant(vb.build(), v -> VariantTestUtil.checkType(v, VariantUtil.NULL, Variant.Type.NULL));
  }

  @Test
  public void testTrue() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(1)}), VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getBoolean());
    });
  }

  @Test
  public void testFalse() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(2)}), VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertFalse(v.getBoolean());
    });
  }

  @Test
  public void testBooleanBuilder() {
    Arrays.asList(true, false).forEach(b -> {
      VariantBuilder vb = new VariantBuilder(false);
      vb.appendBoolean(b);
      VariantTestUtil.testVariant(vb.build(), v -> {
        VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
        Assert.assertEquals(b, v.getBoolean());
      });
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
      Assert.assertEquals(1234567890987654321L, v.getLong());
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
      Assert.assertEquals(-1L, v.getLong());
    });
  }

  @Test
  public void testInt() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(5), (byte) 0xD2, 0x02, (byte) 0x96, 0x49}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(1234567890, v.getInt());
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
      Assert.assertEquals(-1, v.getInt());
    });
  }

  @Test
  public void testShort() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(4), (byte) 0xD2, 0x04}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.SHORT);
      Assert.assertEquals((short) 1234, v.getShort());
    });
  }

  @Test
  public void testNegativeShort() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(4), (byte) 0xFF, (byte) 0xFF}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.SHORT);
      Assert.assertEquals((short) -1, v.getShort());
    });
  }

  @Test
  public void testByte() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(3), 34}), VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BYTE);
      Assert.assertEquals((byte) 34, v.getByte());
    });
  }

  @Test
  public void testNegativeByte() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(3), (byte) 0xFF}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BYTE);
      Assert.assertEquals((byte) -1, v.getByte());
    });
  }

  @Test
  public void testIntegerBuilder() {
    Arrays.asList(
            0L,
            (long) Byte.MIN_VALUE,
            (long) Byte.MAX_VALUE,
            (long) Short.MIN_VALUE,
            (long) Short.MAX_VALUE,
            (long) Integer.MIN_VALUE,
            (long) Integer.MAX_VALUE,
            Long.MIN_VALUE,
            Long.MAX_VALUE)
        .forEach(l -> {
          VariantBuilder vb2 = new VariantBuilder(false);
          vb2.appendLong(l);
          VariantTestUtil.testVariant(vb2.build(), v -> {
            if (Byte.MIN_VALUE <= l && l <= Byte.MAX_VALUE) {
              VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BYTE);
            } else if (Short.MIN_VALUE <= l && l <= Short.MAX_VALUE) {
              VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.SHORT);
            } else if (Integer.MIN_VALUE <= l && l <= Integer.MAX_VALUE) {
              VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.INT);
            } else {
              VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.LONG);
            }
            Assert.assertEquals((long) l, v.getLong());
          });
        });

    Arrays.asList(
            0,
            (int) Byte.MIN_VALUE,
            (int) Byte.MAX_VALUE,
            (int) Short.MIN_VALUE,
            (int) Short.MAX_VALUE,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE)
        .forEach(i -> {
          VariantBuilder vb2 = new VariantBuilder(false);
          vb2.appendLong((long) i);
          VariantTestUtil.testVariant(vb2.build(), v -> {
            if (Byte.MIN_VALUE <= i && i <= Byte.MAX_VALUE) {
              VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BYTE);
            } else if (Short.MIN_VALUE <= i && i <= Short.MAX_VALUE) {
              VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.SHORT);
            } else {
              VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.INT);
            }
            Assert.assertEquals((int) i, v.getInt());
          });
        });

    Arrays.asList((short) 0, (short) Byte.MIN_VALUE, (short) Byte.MAX_VALUE, Short.MIN_VALUE, Short.MAX_VALUE)
        .forEach(s -> {
          VariantBuilder vb2 = new VariantBuilder(false);
          vb2.appendLong(s);
          VariantTestUtil.testVariant(vb2.build(), v -> {
            if (Byte.MIN_VALUE <= s && s <= Byte.MAX_VALUE) {
              VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BYTE);
            } else {
              VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.SHORT);
            }
            Assert.assertEquals((short) s, v.getShort());
          });
        });

    Arrays.asList((byte) 0, Byte.MIN_VALUE, Byte.MAX_VALUE).forEach(b -> {
      VariantBuilder vb2 = new VariantBuilder(false);
      vb2.appendLong(b);
      VariantTestUtil.testVariant(vb2.build(), v -> {
        VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BYTE);
        Assert.assertEquals((byte) b, v.getByte());
      });
    });
  }

  @Test
  public void testFloat() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(14), (byte) 0xD2, 0x02, (byte) 0x96, 0x49}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.FLOAT);
      Assert.assertEquals(Float.intBitsToFloat(1234567890), v.getFloat(), 0);
    });
  }

  @Test
  public void testNegativeFloat() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(14), 0x00, 0x00, 0x00, (byte) 0x80}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.FLOAT);
      Assert.assertEquals(-0.0F, v.getFloat(), 0);
    });
  }

  @Test
  public void testFloatBuilder() {
    Arrays.asList(Float.MIN_VALUE, 0f, Float.MAX_VALUE).forEach(f -> {
      VariantBuilder vb2 = new VariantBuilder(false);
      vb2.appendFloat(f);
      VariantTestUtil.testVariant(vb2.build(), v -> {
        VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.FLOAT);
        Assert.assertEquals(f, v.getFloat(), 0);
      });
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
      Assert.assertEquals(Double.longBitsToDouble(1234567890987654321L), v.getDouble(), 0);
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
      Assert.assertEquals(-0.0D, v.getDouble(), 0);
    });
  }

  @Test
  public void testDoubleBuilder() {
    Arrays.asList(Double.MIN_VALUE, 0d, Double.MAX_VALUE).forEach(d -> {
      VariantBuilder vb2 = new VariantBuilder(false);
      vb2.appendDouble(d);
      VariantTestUtil.testVariant(vb2.build(), v -> {
        VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DOUBLE);
        Assert.assertEquals(d, v.getDouble(), 0);
      });
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
      Assert.assertEquals(new BigDecimal("123456.7890"), v.getDecimal());
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
      Assert.assertEquals(new BigDecimal("-0.0001"), v.getDecimal());
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
      Assert.assertEquals(new BigDecimal("1234567890.987654321"), v.getDecimal());
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
      Assert.assertEquals(new BigDecimal("-0.000000001"), v.getDecimal());
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
      Assert.assertEquals(new BigDecimal("9876543210.123456789"), v.getDecimal());
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
      Assert.assertEquals(new BigDecimal("-9876543210.123456789"), v.getDecimal());
    });
  }

  @Test
  public void testDecimalBuilder() {
    // decimal4
    Arrays.asList(new BigDecimal("123.456"), new BigDecimal("-987.654")).forEach(d -> {
      VariantBuilder vb2 = new VariantBuilder(false);
      vb2.appendDecimal(d);
      VariantTestUtil.testVariant(vb2.build(), v -> {
        VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL4);
        Assert.assertEquals(d, v.getDecimal());
      });
    });

    // decimal8
    Arrays.asList(new BigDecimal("10.2147483647"), new BigDecimal("-1021474836.47"))
        .forEach(d -> {
          VariantBuilder vb2 = new VariantBuilder(false);
          vb2.appendDecimal(d);
          VariantTestUtil.testVariant(vb2.build(), v -> {
            VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL8);
            Assert.assertEquals(d, v.getDecimal());
          });
        });

    // decimal16
    Arrays.asList(new BigDecimal("109223372036854775.807"), new BigDecimal("-109.223372036854775807"))
        .forEach(d -> {
          VariantBuilder vb2 = new VariantBuilder(false);
          vb2.appendDecimal(d);
          VariantTestUtil.testVariant(vb2.build(), v -> {
            VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL16);
            Assert.assertEquals(d, v.getDecimal());
          });
        });
  }

  @Test
  public void testDate() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(11), (byte) 0xE3, 0x4E, 0x00, 0x00}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DATE);
      Assert.assertEquals(LocalDate.parse("2025-04-17"), LocalDate.ofEpochDay(v.getInt()));
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
      Assert.assertEquals(LocalDate.parse("1969-12-31"), LocalDate.ofEpochDay(v.getInt()));
    });
  }

  @Test
  public void testDateBuilder() {
    VariantBuilder vb = new VariantBuilder(false);
    int days = Math.toIntExact(LocalDate.of(2024, 12, 16).toEpochDay());
    vb.appendDate(days);
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DATE);
      Assert.assertEquals(days, v.getInt());
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
      Assert.assertEquals(
          Instant.parse("2025-04-17T08:09:10.123456Z"), Instant.EPOCH.plus(v.getLong(), ChronoUnit.MICROS));
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
      Assert.assertEquals(
          Instant.parse("1969-12-31T23:59:59.999999Z"), Instant.EPOCH.plus(v.getLong(), ChronoUnit.MICROS));
    });
  }

  @Test
  public void testTimestampTzBuilder() {
    DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE_TIME;
    VariantBuilder vb = new VariantBuilder(false);
    long micros = VariantTestUtil.microsSinceEpoch(Instant.from(dtf.parse("2024-12-16T10:23:45.321456-08:00")));
    vb.appendTimestampTz(micros);
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_TZ);
      Assert.assertEquals(micros, v.getLong());
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
      Assert.assertEquals(
          Instant.parse("2025-04-17T08:09:10.123456Z"), Instant.EPOCH.plus(v.getLong(), ChronoUnit.MICROS));
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
      Assert.assertEquals(
          Instant.parse("1969-12-31T23:59:59.999999Z"), Instant.EPOCH.plus(v.getLong(), ChronoUnit.MICROS));
    });
  }

  @Test
  public void testTimestampNtzBuilder() {
    DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE_TIME;
    VariantBuilder vb = new VariantBuilder(false);
    long micros = VariantTestUtil.microsSinceEpoch(Instant.from(dtf.parse("2024-01-01T23:00:00.000001Z")));
    vb.appendTimestampNtz(micros);
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NTZ);
      Assert.assertEquals(micros, v.getLong());
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
      Assert.assertEquals(ByteBuffer.wrap(new byte[] {'a', 'b', 'c', 'd', 'e'}), v.getBinary());
    });
  }

  @Test
  public void testBinaryBuilder() {
    VariantBuilder vb = new VariantBuilder(false);
    byte[] binary = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    vb.appendBinary(binary);
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BINARY);
      Assert.assertEquals(ByteBuffer.wrap(binary), v.getBinary());
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
      Assert.assertEquals("variant", v.getString());
    });
  }

  @Test
  public void testShortString() {
    Variant value = new Variant(
        ByteBuffer.wrap(new byte[] {0b11101, 'v', 'a', 'r', 'i', 'a', 'n', 't'}),
        VariantTestUtil.EMPTY_METADATA);
    VariantTestUtil.testVariant(value, v -> {
      VariantTestUtil.checkType(v, VariantUtil.SHORT_STR, Variant.Type.STRING);
      Assert.assertEquals("variant", v.getString());
    });
  }

  @Test
  public void testStringBuilder() {
    IntStream.range(VariantUtil.MAX_SHORT_STR_SIZE - 3, VariantUtil.MAX_SHORT_STR_SIZE + 3)
        .forEach(len -> {
          VariantBuilder vb2 = new VariantBuilder(false);
          String s = VariantTestUtil.randomString(len);
          vb2.appendString(s);
          VariantTestUtil.testVariant(vb2.build(), v -> {
            if (len <= VariantUtil.MAX_SHORT_STR_SIZE) {
              VariantTestUtil.checkType(v, VariantUtil.SHORT_STR, Variant.Type.STRING);
            } else {
              VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.STRING);
            }
            Assert.assertEquals(s, v.getString());
          });
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
      Assert.assertEquals(LocalTime.parse("23:59:59.123456"), LocalTime.ofNanoOfDay(v.getLong() * 1_000));
    });
  }

  @Test
  public void testTimeBuilder() {
    for (String timeStr : Arrays.asList(
        "00:00:00.000000", "00:00:00.000120", "12:00:00.000000", "12:00:00.002300", "23:59:59.999999")) {
      VariantBuilder vb = new VariantBuilder(false);
      long micros = LocalTime.parse(timeStr).toNanoOfDay() / 1_000;
      vb.appendTime(micros);
      VariantTestUtil.testVariant(vb.build(), v -> {
        VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIME);
        Assert.assertEquals(micros, v.getLong());
      });
    }
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
      Assert.assertEquals(
          Instant.parse("2025-04-17T08:09:10.123456789Z"), Instant.EPOCH.plus(v.getLong(), ChronoUnit.NANOS));
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
      Assert.assertEquals(
          Instant.parse("1969-12-31T23:59:59.999999999Z"), Instant.EPOCH.plus(v.getLong(), ChronoUnit.NANOS));
    });
  }

  @Test
  public void testTimestampNanosBuilder() {
    DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE_TIME;
    VariantBuilder vb = new VariantBuilder(false);
    long nanos = VariantTestUtil.nanosSinceEpoch(Instant.from(dtf.parse("2024-12-16T10:23:45.321456987-08:00")));
    vb.appendTimestampNanosTz(nanos);
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS_TZ);
      Assert.assertEquals(nanos, v.getLong());
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
      Assert.assertEquals(
          Instant.parse("2025-04-17T08:09:10.123456789Z"), Instant.EPOCH.plus(v.getLong(), ChronoUnit.NANOS));
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
      Assert.assertEquals(
          Instant.parse("1969-12-31T23:59:59.999999999Z"), Instant.EPOCH.plus(v.getLong(), ChronoUnit.NANOS));
    });
  }

  @Test
  public void testTimestampNanosNtzBuilder() {
    DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE_TIME;
    VariantBuilder vb = new VariantBuilder(false);
    long nanos = VariantTestUtil.nanosSinceEpoch(Instant.from(dtf.parse("2024-01-01T23:00:00.839280983Z")));
    vb.appendTimestampNanosNtz(nanos);
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS_NTZ);
      Assert.assertEquals(nanos, v.getLong());
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
      Assert.assertEquals(UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"), v.getUUID());
    });
  }

  @Test
  public void testUUIDBuilder() {
    VariantBuilder vb = new VariantBuilder(false);
    byte[] uuid = new byte[] {0, 17, 34, 51, 68, 85, 102, 119, -120, -103, -86, -69, -52, -35, -18, -1};
    long msb = ByteBuffer.wrap(uuid, 0, 8).order(ByteOrder.BIG_ENDIAN).getLong();
    long lsb = ByteBuffer.wrap(uuid, 8, 8).order(ByteOrder.BIG_ENDIAN).getLong();
    UUID expected = new UUID(msb, lsb);

    vb.appendUUID(expected);
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.UUID);
      Assert.assertEquals(expected, v.getUUID());
    });
  }

  @Test
  public void testInvalidType() {
    try {
      Variant value = new Variant(ByteBuffer.wrap(new byte[] {(byte) 0xFC}), VariantTestUtil.EMPTY_METADATA);
      value.getBoolean();
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertEquals(
          "Cannot read unknownType(basicType: 0, valueHeader: 63) value as BOOLEAN", e.getMessage());
    }
  }

  @Test
  public void testInvalidBoolean() {
    try {
      Variant value = new Variant(
          ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(6)}), VariantTestUtil.EMPTY_METADATA);
      value.getBoolean();
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertEquals("Cannot read LONG value as BOOLEAN", e.getMessage());
    }
  }

  @Test
  public void testInvalidLong() {
    try {
      Variant value = new Variant(
          ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(16)}), VariantTestUtil.EMPTY_METADATA);
      value.getLong();
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertEquals(
          "Cannot read STRING value as one of [BYTE, SHORT, INT, DATE, LONG, TIMESTAMP_TZ, TIMESTAMP_NTZ, TIME, TIMESTAMP_NANOS_TZ, TIMESTAMP_NANOS_NTZ]",
          e.getMessage());
    }
  }

  @Test
  public void testInvalidInt() {
    try {
      Variant value = new Variant(
          ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(6)}), VariantTestUtil.EMPTY_METADATA);
      value.getInt();
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertEquals("Cannot read LONG value as one of [BYTE, SHORT, INT, DATE]", e.getMessage());
    }
  }

  @Test
  public void testInvalidShort() {
    try {
      Variant value = new Variant(
          ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(6)}), VariantTestUtil.EMPTY_METADATA);
      value.getShort();
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertEquals("Cannot read LONG value as one of [BYTE, SHORT]", e.getMessage());
    }
  }

  @Test
  public void testInvalidByte() {
    try {
      Variant value = new Variant(
          ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(6)}), VariantTestUtil.EMPTY_METADATA);
      value.getByte();
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertEquals("Cannot read LONG value as BYTE", e.getMessage());
    }
  }

  @Test
  public void testInvalidFloat() {
    try {
      Variant value = new Variant(
          ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(6)}), VariantTestUtil.EMPTY_METADATA);
      value.getFloat();
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertEquals("Cannot read LONG value as FLOAT", e.getMessage());
    }
  }

  @Test
  public void testInvalidDouble() {
    try {
      Variant value = new Variant(
          ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(6)}), VariantTestUtil.EMPTY_METADATA);
      value.getDouble();
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertEquals("Cannot read LONG value as DOUBLE", e.getMessage());
    }
  }

  @Test
  public void testInvalidDecimal() {
    try {
      Variant value = new Variant(
          ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(6), 0}),
          VariantTestUtil.EMPTY_METADATA);
      value.getDecimal();
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertEquals("Cannot read LONG value as one of [DECIMAL4, DECIMAL8, DECIMAL16]", e.getMessage());
    }
  }

  @Test
  public void testInvalidUUID() {
    try {
      Variant value = new Variant(
          ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(6)}), VariantTestUtil.EMPTY_METADATA);
      value.getUUID();
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertEquals("Cannot read LONG value as UUID", e.getMessage());
    }
  }

  @Test
  public void testInvalidString() {
    try {
      Variant value = new Variant(
          ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(6)}), VariantTestUtil.EMPTY_METADATA);
      value.getString();
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertEquals("Cannot read LONG value as STRING", e.getMessage());
    }
  }

  @Test
  public void testInvalidBinary() {
    try {
      Variant value = new Variant(
          ByteBuffer.wrap(new byte[] {VariantTestUtil.primitiveHeader(6)}), VariantTestUtil.EMPTY_METADATA);
      value.getBinary();
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertEquals("Cannot read LONG value as BINARY", e.getMessage());
    }
  }
}
