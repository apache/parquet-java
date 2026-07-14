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

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.IntStream;
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

    assertThatThrownBy(vb::appendNull)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call multiple append() methods.");
  }

  @Test
  public void testBooleanBuilder() {
    Arrays.asList(true, false).forEach(b -> {
      VariantBuilder vb = new VariantBuilder();
      vb.appendBoolean(b);
      VariantTestUtil.testVariant(vb.build(), v -> {
        VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN);
        assertThat(v.getBoolean()).isEqualTo(b);
      });

      assertThatThrownBy(() -> vb.appendBoolean(true))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Cannot call multiple append() methods.");
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
            assertThat(v.getLong()).isEqualTo((long) l);
          });

          assertThatThrownBy(() -> vb.appendLong(1L))
              .isInstanceOf(IllegalStateException.class)
              .hasMessage("Cannot call multiple append() methods.");
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
            assertThat(v.getInt()).isEqualTo((int) i);
          });

          assertThatThrownBy(() -> vb.appendInt(1))
              .isInstanceOf(IllegalStateException.class)
              .hasMessage("Cannot call multiple append() methods.");
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
            assertThat(v.getShort()).isEqualTo((short) s);
          });

          assertThatThrownBy(() -> vb.appendShort((short) 1))
              .isInstanceOf(IllegalStateException.class)
              .hasMessage("Cannot call multiple append() methods.");
        });
  }

  @Test
  public void testByteBuilder() {
    Arrays.asList((byte) 0, Byte.MIN_VALUE, Byte.MAX_VALUE).forEach(b -> {
      VariantBuilder vb = new VariantBuilder();
      vb.appendByte(b);
      VariantTestUtil.testVariant(vb.build(), v -> {
        VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BYTE);
        assertThat(v.getByte()).isEqualTo((byte) b);
      });

      assertThatThrownBy(() -> vb.appendByte((byte) 1))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Cannot call multiple append() methods.");
    });
  }

  @Test
  public void testFloatBuilder() {
    Arrays.asList(Float.MIN_VALUE, 0f, -0f, Float.MAX_VALUE).forEach(f -> {
      VariantBuilder vb = new VariantBuilder();
      vb.appendFloat(f);
      VariantTestUtil.testVariant(vb.build(), v -> {
        VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.FLOAT);
        assertThat(v.getFloat()).isEqualTo(f);
      });

      assertThatThrownBy(() -> vb.appendFloat(1.2f))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Cannot call multiple append() methods.");
    });
  }

  @Test
  public void testFloatBuilderDoesNotWriteTooManyBytes() throws Exception {
    VariantBuilder vb = new VariantBuilder();

    Field writeBufferField = VariantBuilder.class.getDeclaredField("writeBuffer");
    writeBufferField.setAccessible(true);
    Field writePosField = VariantBuilder.class.getDeclaredField("writePos");
    writePosField.setAccessible(true);

    byte[] buffer = (byte[]) writeBufferField.get(vb);
    for (int i = 0; i < 20; i++) {
      buffer[i] = (byte) 0xFF;
    }

    float testFloat = 1.23456f;
    vb.appendFloat(testFloat);

    int writePos = (Integer) writePosField.get(vb);
    assertThat(writePos)
        .as("writePos should be exactly 5 after appendFloat")
        .isEqualTo(5);

    int modifiedBytes = 0;
    for (int i = 0; i < 10; i++) {
      if (buffer[i] != (byte) 0xFF) {
        modifiedBytes++;
      }
    }
    assertThat(modifiedBytes)
        .as("appendFloat should write exactly 5 bytes (1 header + 4 data)")
        .isEqualTo(5);

    for (int i = 5; i < 10; i++) {
      assertThat(buffer[i])
          .as("Byte at position " + i + " should not be modified by appendFloat")
          .isEqualTo((byte) 0xFF);
    }

    Variant variant = vb.build();
    assertThat(variant.getFloat())
        .as("Float value should be preserved correctly")
        .isEqualTo(testFloat);
  }

  @Test
  public void testDoubleBuilder() {
    Arrays.asList(Double.MIN_VALUE, 0d, -0d, Double.MAX_VALUE).forEach(d -> {
      VariantBuilder vb = new VariantBuilder();
      vb.appendDouble(d);
      VariantTestUtil.testVariant(vb.build(), v -> {
        VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DOUBLE);
        assertThat(v.getDouble()).isEqualTo(d);
      });

      assertThatThrownBy(() -> vb.appendDouble(1.2))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Cannot call multiple append() methods.");
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
        assertThat(v.getDecimal()).isEqualTo(d);
      });
    });

    // decimal8
    Arrays.asList(new BigDecimal("10.2147483647"), new BigDecimal("-1021474836.47"))
        .forEach(d -> {
          VariantBuilder vb = new VariantBuilder();
          vb.appendDecimal(d);
          VariantTestUtil.testVariant(vb.build(), v -> {
            VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL8);
            assertThat(v.getDecimal()).isEqualTo(d);
          });
        });

    // decimal16
    Arrays.asList(new BigDecimal("109223372036854775.807"), new BigDecimal("-109.223372036854775807"))
        .forEach(d -> {
          VariantBuilder vb = new VariantBuilder();
          vb.appendDecimal(d);
          VariantTestUtil.testVariant(vb.build(), v -> {
            VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL16);
            assertThat(v.getDecimal()).isEqualTo(d);
          });
        });

    VariantBuilder vb = new VariantBuilder();
    vb.appendDecimal(new BigDecimal("10.2147483647"));
    assertThatThrownBy(() -> vb.appendDecimal(new BigDecimal("10.2147483647")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call multiple append() methods.");
  }

  @Test
  public void testDecimalBuilderUsesOnlyPrecision() {

    BigDecimal smallPrecisionLargeScale = new BigDecimal("1").scaleByPowerOfTen(-20);
    VariantBuilder vb1 = new VariantBuilder();
    vb1.appendDecimal(smallPrecisionLargeScale);
    VariantTestUtil.testVariant(vb1.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL4);
      assertThat(v.getDecimal()).isEqualTo(smallPrecisionLargeScale);
    });

    BigDecimal mediumPrecisionLargeScale = new BigDecimal("1234567890").scaleByPowerOfTen(-25);
    VariantBuilder vb2 = new VariantBuilder();
    vb2.appendDecimal(mediumPrecisionLargeScale);
    VariantTestUtil.testVariant(vb2.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL8);
      assertThat(v.getDecimal()).isEqualTo(mediumPrecisionLargeScale);
    });

    BigDecimal maxDecimal4Precision = new BigDecimal("123456789").scaleByPowerOfTen(-18);
    VariantBuilder vb3 = new VariantBuilder();
    vb3.appendDecimal(maxDecimal4Precision);
    VariantTestUtil.testVariant(vb3.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL4);
      assertThat(v.getDecimal()).isEqualTo(maxDecimal4Precision);
    });

    BigDecimal maxDecimal8Precision = new BigDecimal("123456789012345678").scaleByPowerOfTen(-19);
    VariantBuilder vb4 = new VariantBuilder();
    vb4.appendDecimal(maxDecimal8Precision);
    VariantTestUtil.testVariant(vb4.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL8);
      assertThat(v.getDecimal()).isEqualTo(maxDecimal8Precision);
    });
  }

  @Test
  public void testDateBuilder() {
    VariantBuilder vb = new VariantBuilder();
    int days = Math.toIntExact(LocalDate.of(2024, 12, 16).toEpochDay());
    vb.appendDate(days);
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DATE);
      assertThat(v.getInt()).isEqualTo(days);
    });

    assertThatThrownBy(() -> vb.appendDate(123))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call multiple append() methods.");
  }

  @Test
  public void testTimestampTzBuilder() {
    VariantBuilder vb = new VariantBuilder();
    vb.appendTimestampTz(1734373425321456L);
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_TZ);
      assertThat(v.getLong()).isEqualTo(1734373425321456L);
    });

    assertThatThrownBy(() -> vb.appendTimestampTz(1734373425321456L))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call multiple append() methods.");
  }

  @Test
  public void testTimestampNtzBuilder() {
    VariantBuilder vb = new VariantBuilder();
    vb.appendTimestampNtz(1734373425321456L);
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NTZ);
      assertThat(v.getLong()).isEqualTo(1734373425321456L);
    });

    assertThatThrownBy(() -> vb.appendTimestampNtz(1734373425321456L))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call multiple append() methods.");
  }

  @Test
  public void testBinaryBuilder() {
    VariantBuilder vb = new VariantBuilder();
    vb.appendBinary(ByteBuffer.wrap(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BINARY);
      assertThat(v.getBinary()).isEqualTo(ByteBuffer.wrap(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    });

    assertThatThrownBy(() -> vb.appendBinary(ByteBuffer.wrap(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call multiple append() methods.");
  }

  @Test
  public void testBinaryBuilderDoesNotMutateCallerBuffer() {
    ByteBuffer buf = ByteBuffer.wrap(new byte[] {0, 1, 2, 3});
    int positionBefore = buf.position();
    int remainingBefore = buf.remaining();
    VariantBuilder vb = new VariantBuilder();
    vb.appendBinary(buf);
    assertThat(buf.position()).isEqualTo(positionBefore);
    assertThat(buf.remaining()).isEqualTo(remainingBefore);
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
            assertThat(v.getString()).isEqualTo(s);
          });
        });

    VariantBuilder vb = new VariantBuilder();
    vb.appendString(VariantTestUtil.randomString(10));
    assertThatThrownBy(() -> vb.appendString(VariantTestUtil.randomString(10)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call multiple append() methods.");
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
        assertThat(v.getLong()).isEqualTo(micros);
      });
    }

    // test negative time
    assertThatThrownBy(() -> new VariantBuilder().appendTime(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Time value (-1) cannot be negative.");

    VariantBuilder vb = new VariantBuilder();
    vb.appendTime(123456);
    assertThatThrownBy(() -> vb.appendTime(123456))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call multiple append() methods.");
  }

  @Test
  public void testTimestampNanosBuilder() {
    VariantBuilder vb = new VariantBuilder();
    vb.appendTimestampNanosTz(1734373425321456987L);
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS_TZ);
      assertThat(v.getLong()).isEqualTo(1734373425321456987L);
    });

    assertThatThrownBy(() -> vb.appendTimestampNanosTz(1734373425321456987L))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call multiple append() methods.");
  }

  @Test
  public void testTimestampNanosNtzBuilder() {
    VariantBuilder vb = new VariantBuilder();
    vb.appendTimestampNanosNtz(1734373425321456987L);
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS_NTZ);
      assertThat(v.getLong()).isEqualTo(1734373425321456987L);
    });

    assertThatThrownBy(() -> vb.appendTimestampNanosNtz(1734373425321456987L))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call multiple append() methods.");
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
      assertThat(v.getUUID()).isEqualTo(expected);
    });

    assertThatThrownBy(() -> vb.appendUUID(expected))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call multiple append() methods.");
  }

  @Test
  public void testUUIDBytesBuilder() {
    byte[] uuid = new byte[] {0, 17, 34, 51, 68, 85, 102, 119, -120, -103, -86, -69, -52, -35, -18, -1};
    long msb = ByteBuffer.wrap(uuid, 0, 8).order(ByteOrder.BIG_ENDIAN).getLong();
    long lsb = ByteBuffer.wrap(uuid, 8, 8).order(ByteOrder.BIG_ENDIAN).getLong();
    UUID expected = new UUID(msb, lsb);

    VariantBuilder vb = new VariantBuilder();
    vb.appendUUIDBytes(ByteBuffer.wrap(uuid));
    VariantTestUtil.testVariant(vb.build(), v -> {
      VariantTestUtil.checkType(v, VariantUtil.PRIMITIVE, Variant.Type.UUID);
      assertThat(v.getUUID()).isEqualTo(expected);
    });

    // appendUUIDBytes must go through onAppend(), so a second append on the root builder
    // (which already holds a value) must be rejected instead of producing a multi-value buffer.
    assertThatThrownBy(() -> vb.appendUUIDBytes(ByteBuffer.wrap(uuid)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot call multiple append() methods.");
  }
}
