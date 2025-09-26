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
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestVariantScalarBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(TestVariantScalarBuilder.class);

  @Test
  public void testNullBuilder() {
    VariantBuilder vb = new VariantBuilder();
    vb.appendNull();
    VariantTestUtil.testVariant(vb.build(), v -> VariantTestUtil.checkType(v, VariantUtil.NULL, Variant.Type.NULL));

    try {
      vb.appendNull();
      Assert.fail("Expected Exception when appending multiple values");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testBooleanBuilder() {
    Arrays.asList(true, false).forEach(b -> {
      VariantBuilder vb = new VariantBuilder();
      vb.appendBoolean(b);
      VariantTestUtil.testVariant(vb.build(), v -> {
        VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
        Assert.assertEquals(b, v.getBoolean());
      });

      try {
        vb.appendBoolean(true);
        Assert.fail("Expected Exception when appending multiple values");
      } catch (Exception e) {
        // expected
      }
    });
  }

  @Test
  public void testLongBuilder() {
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
          VariantBuilder vb = new VariantBuilder();
          vb.appendLong(l);
          VariantTestUtil.testVariant(vb.build(), v -> {
            VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.LONG);
            Assert.assertEquals((long) l, v.getLong());
          });

          try {
            vb.appendLong(1L);
            Assert.fail("Expected Exception when appending multiple values");
          } catch (Exception e) {
            // expected
          }
        });
  }

  @Test
  public void testIntBuilder() {
    Arrays.asList(
            0,
            (int) Byte.MIN_VALUE,
            (int) Byte.MAX_VALUE,
            (int) Short.MIN_VALUE,
            (int) Short.MAX_VALUE,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE)
        .forEach(i -> {
          VariantBuilder vb = new VariantBuilder();
          vb.appendInt(i);
          VariantTestUtil.testVariant(vb.build(), v -> {
            VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.INT);
            Assert.assertEquals((int) i, v.getInt());
          });

          try {
            vb.appendInt(1);
            Assert.fail("Expected Exception when appending multiple values");
          } catch (Exception e) {
            // expected
          }
        });
  }

  @Test
  public void testShortBuilder() {
    Arrays.asList((short) 0, (short) Byte.MIN_VALUE, (short) Byte.MAX_VALUE, Short.MIN_VALUE, Short.MAX_VALUE)
        .forEach(s -> {
          VariantBuilder vb = new VariantBuilder();
          vb.appendShort(s);
          VariantTestUtil.testVariant(vb.build(), v -> {
            VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.SHORT);
            Assert.assertEquals((short) s, v.getShort());
          });

          try {
            vb.appendShort((short) 1);
            Assert.fail("Expected Exception when appending multiple values");
          } catch (Exception e) {
            // expected
          }
        });
  }

  @Test
  public void testByteBuilder() {
    Arrays.asList((byte) 0, Byte.MIN_VALUE, Byte.MAX_VALUE).forEach(b -> {
      VariantBuilder vb = new VariantBuilder();
      vb.appendByte(b);
      VariantTestUtil.testVariant(vb.build(), v -> {
        VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BYTE);
        Assert.assertEquals((byte) b, v.getByte());
      });

      try {
        vb.appendByte((byte) 1);
        Assert.fail("Expected Exception when appending multiple values");
      } catch (Exception e) {
        // expected
      }
    });
  }

  @Test
  public void testFloatBuilder() {
    Arrays.asList(Float.MIN_VALUE, 0f, -0f, Float.MAX_VALUE).forEach(f -> {
      VariantBuilder vb = new VariantBuilder();
      vb.appendFloat(f);
      VariantTestUtil.testVariant(vb.build(), v -> {
        VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.FLOAT);
        Assert.assertEquals(f, v.getFloat(), 0);
      });

      try {
        vb.appendFloat(1.2f);
        Assert.fail("Expected Exception when appending multiple values");
      } catch (Exception e) {
        // expected
      }
    });
  }

  @Test
  public void testDoubleBuilder() {
    Arrays.asList(Double.MIN_VALUE, 0d, -0d, Double.MAX_VALUE).forEach(d -> {
      VariantBuilder vb = new VariantBuilder();
      vb.appendDouble(d);
      VariantTestUtil.testVariant(vb.build(), v -> {
        VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DOUBLE);
        Assert.assertEquals(d, v.getDouble(), 0);
      });

      try {
        vb.appendDouble(1.2);
        Assert.fail("Expected Exception when appending multiple values");
      } catch (Exception e) {
        // expected
      }
    });
  }

  @Test
  public void testDecimalBuilder() {
    // decimal4
    Arrays.asList(new BigDecimal("123.456"), new BigDecimal("-987.654")).forEach(d -> {
      VariantBuilder vb = new VariantBuilder();
      vb.appendDecimal(d);
      VariantTestUtil.testVariant(vb.build(), v -> {
        VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL4);
        Assert.assertEquals(d, v.getDecimal());
      });
    });

    // decimal8
    Arrays.asList(new BigDecimal("10.2147483647"), new BigDecimal("-1021474836.47"))
        .forEach(d -> {
          VariantBuilder vb = new VariantBuilder();
          vb.appendDecimal(d);
          VariantTestUtil.testVariant(vb.build(), v -> {
            VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL8);
            Assert.assertEquals(d, v.getDecimal());
          });
        });

    // decimal16
    Arrays.asList(new BigDecimal("109223372036854775.807"), new BigDecimal("-109.223372036854775807"))
        .forEach(d -> {
          VariantBuilder vb = new VariantBuilder();
          vb.appendDecimal(d);
          VariantTestUtil.testVariant(vb.build(), v -> {
            VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL16);
            Assert.assertEquals(d, v.getDecimal());
          });
        });

    VariantBuilder vb = new VariantBuilder();
    vb.appendDecimal(new BigDecimal("10.2147483647"));
    try {
      vb.appendDecimal(new BigDecimal("10.2147483647"));
      Assert.fail("Expected Exception when appending multiple values");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testDecimalBuilderUsesOnlyPrecision() {

    BigDecimal smallPrecisionLargeScale = new BigDecimal("1").scaleByPowerOfTen(-20);
    VariantBuilder vb1 = new VariantBuilder();
    vb1.appendDecimal(smallPrecisionLargeScale);
    VariantTestUtil.testVariant(vb1.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL4);
      Assert.assertEquals(smallPrecisionLargeScale, v.getDecimal());
    });

    BigDecimal mediumPrecisionLargeScale = new BigDecimal("1234567890").scaleByPowerOfTen(-25);
    VariantBuilder vb2 = new VariantBuilder();
    vb2.appendDecimal(mediumPrecisionLargeScale);
    VariantTestUtil.testVariant(vb2.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL8);
      Assert.assertEquals(mediumPrecisionLargeScale, v.getDecimal());
    });

    BigDecimal maxDecimal4Precision = new BigDecimal("123456789").scaleByPowerOfTen(-18);
    VariantBuilder vb3 = new VariantBuilder();
    vb3.appendDecimal(maxDecimal4Precision);
    VariantTestUtil.testVariant(vb3.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL4);
      Assert.assertEquals(maxDecimal4Precision, v.getDecimal());
    });

    BigDecimal maxDecimal8Precision = new BigDecimal("123456789012345678").scaleByPowerOfTen(-19);
    VariantBuilder vb4 = new VariantBuilder();
    vb4.appendDecimal(maxDecimal8Precision);
    VariantTestUtil.testVariant(vb4.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL8);
      Assert.assertEquals(maxDecimal8Precision, v.getDecimal());
    });
  }

  @Test
  public void testDateBuilder() {
    VariantBuilder vb = new VariantBuilder();
    int days = Math.toIntExact(LocalDate.of(2024, 12, 16).toEpochDay());
    vb.appendDate(days);
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DATE);
      Assert.assertEquals(days, v.getInt());
    });

    try {
      vb.appendDate(123);
      Assert.fail("Expected Exception when appending multiple values");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testTimestampTzBuilder() {
    VariantBuilder vb = new VariantBuilder();
    vb.appendTimestampTz(1734373425321456L);
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_TZ);
      Assert.assertEquals(1734373425321456L, v.getLong());
    });

    try {
      vb.appendTimestampTz(1734373425321456L);
      Assert.fail("Expected Exception when appending multiple values");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testTimestampNtzBuilder() {
    VariantBuilder vb = new VariantBuilder();
    vb.appendTimestampNtz(1734373425321456L);
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NTZ);
      Assert.assertEquals(1734373425321456L, v.getLong());
    });

    try {
      vb.appendTimestampNtz(1734373425321456L);
      Assert.fail("Expected Exception when appending multiple values");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testBinaryBuilder() {
    VariantBuilder vb = new VariantBuilder();
    vb.appendBinary(ByteBuffer.wrap(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BINARY);
      Assert.assertEquals(ByteBuffer.wrap(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}), v.getBinary());
    });

    try {
      vb.appendBinary(ByteBuffer.wrap(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
      Assert.fail("Expected Exception when appending multiple values");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testStringBuilder() {
    IntStream.range(VariantUtil.MAX_SHORT_STR_SIZE - 3, VariantUtil.MAX_SHORT_STR_SIZE + 3)
        .forEach(len -> {
          VariantBuilder vb = new VariantBuilder();
          String s = VariantTestUtil.randomString(len);
          vb.appendString(s);
          VariantTestUtil.testVariant(vb.build(), v -> {
            if (len <= VariantUtil.MAX_SHORT_STR_SIZE) {
              VariantTestUtil.checkType(v, VariantUtil.SHORT_STR, Variant.Type.STRING);
            } else {
              VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.STRING);
            }
            Assert.assertEquals(s, v.getString());
          });
        });

    VariantBuilder vb = new VariantBuilder();
    vb.appendString(VariantTestUtil.randomString(10));
    try {
      vb.appendString(VariantTestUtil.randomString(10));
      Assert.fail("Expected Exception when appending multiple values");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testTimeBuilder() {
    for (String timeStr : Arrays.asList(
        "00:00:00.000000", "00:00:00.000120", "12:00:00.000000", "12:00:00.002300", "23:59:59.999999")) {
      VariantBuilder vb = new VariantBuilder();
      long micros = LocalTime.parse(timeStr).toNanoOfDay() / 1_000;
      vb.appendTime(micros);
      VariantTestUtil.testVariant(vb.build(), v -> {
        VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIME);
        Assert.assertEquals(micros, v.getLong());
      });
    }

    // test negative time
    try {
      VariantBuilder vb = new VariantBuilder();
      vb.appendTime(-1);
      Assert.fail("Expected Exception when adding a negative time value");
    } catch (IllegalArgumentException e) {
      // expected
    }

    VariantBuilder vb = new VariantBuilder();
    vb.appendTime(123456);
    try {
      vb.appendTime(123456);
      Assert.fail("Expected Exception when appending multiple values");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testTimestampNanosBuilder() {
    VariantBuilder vb = new VariantBuilder();
    vb.appendTimestampNanosTz(1734373425321456987L);
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS_TZ);
      Assert.assertEquals(1734373425321456987L, v.getLong());
    });

    try {
      vb.appendTimestampNanosTz(1734373425321456987L);
      Assert.fail("Expected Exception when appending multiple values");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testTimestampNanosNtzBuilder() {
    VariantBuilder vb = new VariantBuilder();
    vb.appendTimestampNanosNtz(1734373425321456987L);
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS_NTZ);
      Assert.assertEquals(1734373425321456987L, v.getLong());
    });

    try {
      vb.appendTimestampNanosNtz(1734373425321456987L);
      Assert.fail("Expected Exception when appending multiple values");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testUUIDBuilder() {
    VariantBuilder vb = new VariantBuilder();
    byte[] uuid = new byte[] {0, 17, 34, 51, 68, 85, 102, 119, -120, -103, -86, -69, -52, -35, -18, -1};
    long msb = ByteBuffer.wrap(uuid, 0, 8).order(ByteOrder.BIG_ENDIAN).getLong();
    long lsb = ByteBuffer.wrap(uuid, 8, 8).order(ByteOrder.BIG_ENDIAN).getLong();
    UUID expected = new UUID(msb, lsb);

    vb.appendUUID(expected);
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.UUID);
      Assert.assertEquals(expected, v.getUUID());
    });

    try {
      vb.appendUUID(expected);
      Assert.fail("Expected Exception when appending multiple values");
    } catch (Exception e) {
      // expected
    }
  }
}
