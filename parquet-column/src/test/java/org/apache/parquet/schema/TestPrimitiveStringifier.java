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
package org.apache.parquet.schema;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.parquet.schema.PrimitiveStringifier.DATE_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.DEFAULT_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.INTERVAL_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIMESTAMP_MICROS_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIMESTAMP_MICROS_UTC_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIMESTAMP_MILLIS_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIMESTAMP_MILLIS_UTC_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIMESTAMP_NANOS_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIMESTAMP_NANOS_UTC_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIME_NANOS_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIME_NANOS_UTC_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIME_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIME_UTC_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.UNSIGNED_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.UTF8_STRINGIFIER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;

public class TestPrimitiveStringifier {

  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  @Test
  public void testDefaultStringifier() {
    PrimitiveStringifier stringifier = DEFAULT_STRINGIFIER;

    assertThat(stringifier.stringify(true)).isEqualTo("true");
    assertThat(stringifier.stringify(false)).isEqualTo("false");

    assertThat(stringifier.stringify(0.0)).isEqualTo("0.0");
    assertThat(stringifier.stringify(123456.7891234567)).isEqualTo("123456.7891234567");
    assertThat(stringifier.stringify(-98765.43219876543)).isEqualTo("-98765.43219876543");

    assertThat(stringifier.stringify(0.0f)).isEqualTo("0.0");
    assertThat(stringifier.stringify(987.6543f)).isEqualTo("987.6543");
    assertThat(stringifier.stringify(-123.4567f)).isEqualTo("-123.4567");

    assertThat(stringifier.stringify(0)).isEqualTo("0");
    assertThat(stringifier.stringify(1234567890)).isEqualTo("1234567890");
    assertThat(stringifier.stringify(-987654321)).isEqualTo("-987654321");

    assertThat(stringifier.stringify(0l)).isEqualTo("0");
    assertThat(stringifier.stringify(1234567890123456789l)).isEqualTo("1234567890123456789");
    assertThat(stringifier.stringify(-987654321987654321l)).isEqualTo("-987654321987654321");

    assertThat(stringifier.stringify(null)).isEqualTo("null");
    assertThat(stringifier.stringify(Binary.EMPTY)).isEqualTo("0x");
    assertThat(stringifier.stringify(toBinary(0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF)))
        .isEqualTo("0x0123456789ABCDEF");
  }

  @Test
  public void testUnsignedStringifier() {
    PrimitiveStringifier stringifier = UNSIGNED_STRINGIFIER;

    assertThat(stringifier.stringify(0)).isEqualTo("0");
    assertThat(stringifier.stringify(2147483647)).isEqualTo("2147483647");
    assertThat(stringifier.stringify(0xFFFFFFFF)).isEqualTo("4294967295");

    assertThat(stringifier.stringify(0l)).isEqualTo("0");
    assertThat(stringifier.stringify(9223372036854775807l)).isEqualTo("9223372036854775807");
    assertThat(stringifier.stringify(0xFFFFFFFFFFFFFFFFl)).isEqualTo("18446744073709551615");

    checkThrowingUnsupportedException(stringifier, Integer.TYPE, Long.TYPE);
  }

  @Test
  public void testUTF8Stringifier() {
    PrimitiveStringifier stringifier = UTF8_STRINGIFIER;

    assertThat(stringifier.stringify(null)).isEqualTo("null");
    assertThat(stringifier.stringify(Binary.EMPTY)).isEqualTo("");
    assertThat(stringifier.stringify(Binary.fromString("This is a UTF-8 test")))
        .isEqualTo("This is a UTF-8 test");
    assertThat(stringifier.stringify(Binary.fromConstantByteArray("これはUTF-8のテストです".getBytes(UTF_8))))
        .isEqualTo("これはUTF-8のテストです");

    checkThrowingUnsupportedException(stringifier, Binary.class);
  }

  @Test
  public void testIntervalStringifier() {
    PrimitiveStringifier stringifier = INTERVAL_STRINGIFIER;

    assertThat(stringifier.stringify(null)).isEqualTo("null");

    assertThat(stringifier.stringify(Binary.EMPTY)).isEqualTo("<INVALID>");
    assertThat(stringifier.stringify(Binary.fromConstantByteArray(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})))
        .isEqualTo("<INVALID>");
    assertThat(stringifier.stringify(
            Binary.fromReusedByteArray(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})))
        .isEqualTo("<INVALID>");

    ByteBuffer buffer = ByteBuffer.allocate(12);
    assertThat(stringifier.stringify(Binary.fromConstantByteBuffer(buffer)))
        .isEqualTo("interval(0 months, 0 days, 0 millis)");

    buffer.putInt(0x03000000);
    buffer.putInt(0x06000000);
    buffer.putInt(0x09000000);
    buffer.flip();
    assertThat(stringifier.stringify(Binary.fromConstantByteBuffer(buffer)))
        .isEqualTo("interval(3 months, 6 days, 9 millis)");

    buffer.clear();
    buffer.putInt(0xFFFFFFFF);
    buffer.putInt(0xFEFFFFFF);
    buffer.putInt(0xFDFFFFFF);
    buffer.flip();
    assertThat(stringifier.stringify(Binary.fromReusedByteBuffer(buffer)))
        .isEqualTo("interval(4294967295 months, 4294967294 days, 4294967293 millis)");

    checkThrowingUnsupportedException(stringifier, Binary.class);
  }

  @Test
  public void testDateStringifier() {
    PrimitiveStringifier stringifier = DATE_STRINGIFIER;

    assertThat(stringifier.stringify(0)).isEqualTo("1970-01-01");

    Calendar cal = Calendar.getInstance(UTC);
    cal.clear();
    cal.set(2017, Calendar.DECEMBER, 14);
    assertThat(stringifier.stringify((int) MILLISECONDS.toDays(cal.getTimeInMillis())))
        .isEqualTo("2017-12-14");

    cal.clear();
    cal.set(1583, Calendar.AUGUST, 3);
    assertThat(stringifier.stringify((int) MILLISECONDS.toDays(cal.getTimeInMillis())))
        .isEqualTo("1583-08-03");

    checkThrowingUnsupportedException(stringifier, Integer.TYPE);
  }

  @Test
  public void testTimestampMillisStringifier() {
    for (PrimitiveStringifier stringifier :
        List.of(TIMESTAMP_MILLIS_STRINGIFIER, TIMESTAMP_MILLIS_UTC_STRINGIFIER)) {
      String timezoneAmendment = (stringifier == TIMESTAMP_MILLIS_STRINGIFIER ? "" : "+0000");

      assertThat(stringifier.stringify(0l))
          .isEqualTo(withZoneString("1970-01-01T00:00:00.000", timezoneAmendment));

      Calendar cal = Calendar.getInstance(UTC);
      cal.clear();
      cal.set(2017, Calendar.DECEMBER, 15, 10, 9, 54);
      cal.set(Calendar.MILLISECOND, 120);
      assertThat(stringifier.stringify(cal.getTimeInMillis()))
          .isEqualTo(withZoneString("2017-12-15T10:09:54.120", timezoneAmendment));

      cal.clear();
      cal.set(1948, Calendar.NOVEMBER, 23, 20, 19, 1);
      cal.set(Calendar.MILLISECOND, 9);
      assertThat(stringifier.stringify(cal.getTimeInMillis()))
          .isEqualTo(withZoneString("1948-11-23T20:19:01.009", timezoneAmendment));

      checkThrowingUnsupportedException(stringifier, Long.TYPE);
    }
  }

  @Test
  public void testTimestampMicrosStringifier() {
    for (PrimitiveStringifier stringifier :
        List.of(TIMESTAMP_MICROS_STRINGIFIER, TIMESTAMP_MICROS_UTC_STRINGIFIER)) {
      String timezoneAmendment = (stringifier == TIMESTAMP_MICROS_STRINGIFIER ? "" : "+0000");

      assertThat(stringifier.stringify(0l))
          .isEqualTo(withZoneString("1970-01-01T00:00:00.000000", timezoneAmendment));

      Calendar cal = Calendar.getInstance(UTC);
      cal.clear();
      cal.set(2053, Calendar.JULY, 10, 22, 13, 24);
      cal.set(Calendar.MILLISECOND, 84);
      long micros = cal.getTimeInMillis() * 1000 + 900;
      assertThat(stringifier.stringify(micros))
          .isEqualTo(withZoneString("2053-07-10T22:13:24.084900", timezoneAmendment));

      cal.clear();
      cal.set(1848, Calendar.MARCH, 15, 9, 23, 59);
      cal.set(Calendar.MILLISECOND, 765);
      micros = cal.getTimeInMillis() * 1000 - 1;
      assertThat(stringifier.stringify(micros))
          .isEqualTo(withZoneString("1848-03-15T09:23:59.764999", timezoneAmendment));

      checkThrowingUnsupportedException(stringifier, Long.TYPE);
    }
  }

  @Test
  public void testTimestampNanosStringifier() {
    for (PrimitiveStringifier stringifier : List.of(TIMESTAMP_NANOS_STRINGIFIER, TIMESTAMP_NANOS_UTC_STRINGIFIER)) {
      String timezoneAmendment = (stringifier == TIMESTAMP_NANOS_STRINGIFIER ? "" : "+0000");

      assertThat(stringifier.stringify(0l))
          .isEqualTo(withZoneString("1970-01-01T00:00:00.000000000", timezoneAmendment));

      Calendar cal = Calendar.getInstance(UTC);
      cal.clear();
      cal.set(2053, Calendar.JULY, 10, 22, 13, 24);
      cal.set(Calendar.MILLISECOND, 84);
      long nanos = cal.getTimeInMillis() * 1_000_000 + 536;
      assertThat(stringifier.stringify(nanos))
          .isEqualTo(withZoneString("2053-07-10T22:13:24.084000536", timezoneAmendment));

      cal.clear();
      cal.set(1848, Calendar.MARCH, 15, 9, 23, 59);
      cal.set(Calendar.MILLISECOND, 765);
      nanos = cal.getTimeInMillis() * 1_000_000 - 1;
      assertThat(stringifier.stringify(nanos))
          .isEqualTo(withZoneString("1848-03-15T09:23:59.764999999", timezoneAmendment));

      checkThrowingUnsupportedException(stringifier, Long.TYPE);
    }
  }

  @Test
  public void testTimeStringifier() {
    for (PrimitiveStringifier stringifier : List.of(TIME_STRINGIFIER, TIME_UTC_STRINGIFIER)) {
      String timezoneAmendment = (stringifier == TIME_STRINGIFIER ? "" : "+0000");

      assertThat(stringifier.stringify(0)).isEqualTo(withZoneString("00:00:00.000", timezoneAmendment));
      assertThat(stringifier.stringify(0l)).isEqualTo(withZoneString("00:00:00.000000", timezoneAmendment));

      assertThat(stringifier.stringify((int) convert(MILLISECONDS, 12, 34, 56, 789)))
          .isEqualTo(withZoneString("12:34:56.789", timezoneAmendment));
      assertThat(stringifier.stringify(convert(MICROSECONDS, 12, 34, 56, 789012)))
          .isEqualTo(withZoneString("12:34:56.789012", timezoneAmendment));

      assertThat(stringifier.stringify((int) convert(MILLISECONDS, -12, -34, -56, -789)))
          .isEqualTo(withZoneString("-12:34:56.789", timezoneAmendment));
      assertThat(stringifier.stringify(convert(MICROSECONDS, -12, -34, -56, -789012)))
          .isEqualTo(withZoneString("-12:34:56.789012", timezoneAmendment));

      assertThat(stringifier.stringify((int) convert(MILLISECONDS, 123, 12, 34, 567)))
          .isEqualTo(withZoneString("123:12:34.567", timezoneAmendment));
      assertThat(stringifier.stringify(convert(MICROSECONDS, 12345, 12, 34, 56789)))
          .isEqualTo(withZoneString("12345:12:34.056789", timezoneAmendment));

      assertThat(stringifier.stringify((int) convert(MILLISECONDS, -123, -12, -34, -567)))
          .isEqualTo(withZoneString("-123:12:34.567", timezoneAmendment));
      assertThat(stringifier.stringify(convert(MICROSECONDS, -12345, -12, -34, -56789)))
          .isEqualTo(withZoneString("-12345:12:34.056789", timezoneAmendment));

      checkThrowingUnsupportedException(stringifier, Integer.TYPE, Long.TYPE);
    }
  }

  @Test
  public void testTimeNanoStringifier() {
    for (PrimitiveStringifier stringifier : List.of(TIME_NANOS_STRINGIFIER, TIME_NANOS_UTC_STRINGIFIER)) {
      String timezoneAmendment = (stringifier == TIME_NANOS_STRINGIFIER ? "" : "+0000");

      assertThat(stringifier.stringify(0l)).isEqualTo(withZoneString("00:00:00.000000000", timezoneAmendment));

      assertThat(stringifier.stringify(convert(NANOSECONDS, 12, 34, 56, 789012987)))
          .isEqualTo(withZoneString("12:34:56.789012987", timezoneAmendment));
      assertThat(stringifier.stringify(convert(NANOSECONDS, -12, -34, -56, -789012)))
          .isEqualTo(withZoneString("-12:34:56.000789012", timezoneAmendment));
      assertThat(stringifier.stringify(convert(NANOSECONDS, 12345, 12, 34, 56789)))
          .isEqualTo(withZoneString("12345:12:34.000056789", timezoneAmendment));
      assertThat(stringifier.stringify(convert(NANOSECONDS, -12345, -12, -34, -56789)))
          .isEqualTo(withZoneString("-12345:12:34.000056789", timezoneAmendment));

      checkThrowingUnsupportedException(stringifier, Integer.TYPE, Long.TYPE);
    }
  }

  private String withZoneString(String expected, String zoneString) {
    return expected + zoneString;
  }

  private long convert(TimeUnit unit, long hours, long minutes, long seconds, long rest) {
    return unit.convert(hours, HOURS) + unit.convert(minutes, MINUTES) + unit.convert(seconds, SECONDS) + rest;
  }

  @Test
  public void testDecimalStringifier() {
    PrimitiveStringifier stringifier = PrimitiveStringifier.createDecimalStringifier(4);

    assertThat(stringifier.stringify(0)).isEqualTo("0.0000");
    assertThat(stringifier.stringify(1234567890)).isEqualTo("123456.7890");
    assertThat(stringifier.stringify(-987654321)).isEqualTo("-98765.4321");

    assertThat(stringifier.stringify(0l)).isEqualTo("0.0000");
    assertThat(stringifier.stringify(1234567890123456789l)).isEqualTo("123456789012345.6789");
    assertThat(stringifier.stringify(-987654321098765432l)).isEqualTo("-98765432109876.5432");

    assertThat(stringifier.stringify(null)).isEqualTo("null");
    assertThat(stringifier.stringify(Binary.EMPTY)).isEqualTo("<INVALID>");
    assertThat(stringifier.stringify(Binary.fromReusedByteArray(new byte[] {0})))
        .isEqualTo("0.0000");
    assertThat(stringifier.stringify(Binary.fromConstantByteArray(
            new BigInteger("98765432109876543210987654321098765432109876543210").toByteArray())))
        .isEqualTo("9876543210987654321098765432109876543210987654.3210");
    assertThat(stringifier.stringify(Binary.fromConstantByteArray(
            new BigInteger("-12345678901234567890123456789012345678901234567890").toByteArray())))
        .isEqualTo("-1234567890123456789012345678901234567890123456.7890");

    checkThrowingUnsupportedException(stringifier, Integer.TYPE, Long.TYPE, Binary.class);
  }

  @Test
  public void testFloat16Stringifier() {
    PrimitiveStringifier stringifier = PrimitiveStringifier.FLOAT16_STRINGIFIER;

    // Zeroes, NaN and infinities
    assertThat(stringifier.stringify(toBinary(0x00, 0x00))).isEqualTo("0.0");
    assertThat(stringifier.stringify(toBinary(0x00, 0x80))).isEqualTo("-0.0");
    assertThat(stringifier.stringify(toBinary(0x00, 0x7e))).isEqualTo(Float.toString(Float.NaN));
    assertThat(stringifier.stringify(toBinary(0x00, 0x7c))).isEqualTo(Float.toString(Float.POSITIVE_INFINITY));
    assertThat(stringifier.stringify(toBinary(0x00, 0xfc))).isEqualTo(Float.toString(Float.NEGATIVE_INFINITY));

    // Known values
    assertThat(stringifier.stringify(toBinary(0x01, 0x3c))).isEqualTo("1.0009766");
    assertThat(stringifier.stringify(toBinary(0x00, 0xc0))).isEqualTo("-2.0");
    assertThat(stringifier.stringify(toBinary(0x00, 0x04))).isEqualTo("6.1035156E-5");
    assertThat(stringifier.stringify(toBinary(0xff, 0x7b))).isEqualTo("65504.0");
    assertThat(stringifier.stringify(toBinary(0x55, 0x35))).isEqualTo("0.33325195");

    // Subnormals
    assertThat(stringifier.stringify(toBinary(0xff, 0x03))).isEqualTo("6.097555E-5");
    assertThat(stringifier.stringify(toBinary(0x01, 0x00))).isEqualTo("5.9604645E-8");
    assertThat(stringifier.stringify(toBinary(0xff, 0x83))).isEqualTo("-6.097555E-5");
    assertThat(stringifier.stringify(toBinary(0x01, 0x80))).isEqualTo("-5.9604645E-8");

    // Floats with absolute value above +/-65519 are rounded to +/-inf
    // when using round-to-even
    assertThat(stringifier.stringify(toBinary(0xff, 0x7b))).isEqualTo("65504.0");

    // Check if numbers are rounded to nearest even when they
    // cannot be accurately represented by Half
    assertThat(stringifier.stringify(toBinary(0x00, 0x68))).isEqualTo("2048.0");
    assertThat(stringifier.stringify(toBinary(0x00, 0x6c))).isEqualTo("4096.0");

    checkThrowingUnsupportedException(stringifier, Integer.TYPE, Long.TYPE, Binary.class);
  }

  @Test
  public void testUUIDStringifier() {
    PrimitiveStringifier stringifier = PrimitiveStringifier.UUID_STRINGIFIER;

    assertThat(stringifier.stringify(toBinary(
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
            0xff)))
        .isEqualTo("00112233-4455-6677-8899-aabbccddeeff");
    assertThat(stringifier.stringify(toBinary(
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00)))
        .isEqualTo("00000000-0000-0000-0000-000000000000");
    assertThat(stringifier.stringify(toBinary(
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff)))
        .isEqualTo("ffffffff-ffff-ffff-ffff-ffffffffffff");
    assertThat(stringifier.stringify(toBinary(
            0x0e, 0xb1, 0x49, 0x7c, 0x19, 0xb6, 0x42, 0xbc, 0xb0, 0x28, 0xb4, 0xb6, 0x12, 0xbe, 0xd1,
            0x41)))
        .isEqualTo("0eb1497c-19b6-42bc-b028-b4b612bed141");

    // Check that the stringifier does not care about the length, it always takes the first 16 bytes
    assertThat(stringifier.stringify(toBinary(
            0x87, 0xa0, 0x9c, 0xca, 0x3b, 0x1e, 0x4a, 0x0a, 0x9c, 0x77, 0x59, 0x19, 0x24, 0xc3, 0xb5, 0x7b,
            0x00, 0x00, 0x00)))
        .isEqualTo("87a09cca-3b1e-4a0a-9c77-591924c3b57b");

    // As there is no validation implemented, if the 16 bytes is not available, the array will be over-indexed
    assertThatThrownBy(() -> stringifier.stringify(toBinary(
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee)))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class);

    checkThrowingUnsupportedException(stringifier, Binary.class);
  }

  private Binary toBinary(int... bytes) {
    byte[] array = new byte[bytes.length];
    for (int i = 0; i < array.length; ++i) {
      array[i] = (byte) bytes[i];
    }
    return Binary.fromConstantByteArray(array);
  }

  private void checkThrowingUnsupportedException(PrimitiveStringifier stringifier, Class<?>... excludes) {
    Set<Class<?>> set = new HashSet<>(List.of(excludes));
    if (!set.contains(Integer.TYPE)) {
      assertThatThrownBy(() -> stringifier.stringify(0))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("stringify(int) was called on a non-int stringifier: " + stringifier);
    }
    if (!set.contains(Long.TYPE)) {
      assertThatThrownBy(() -> stringifier.stringify(0l))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("stringify(long) was called on a non-long stringifier: " + stringifier);
    }
    if (!set.contains(Float.TYPE)) {
      assertThatThrownBy(() -> stringifier.stringify(0.0f))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("stringify(float) was called on a non-float stringifier: " + stringifier);
    }
    if (!set.contains(Double.TYPE)) {
      assertThatThrownBy(() -> stringifier.stringify(0.0))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("stringify(double) was called on a non-double stringifier: " + stringifier);
    }
    if (!set.contains(Boolean.TYPE)) {
      assertThatThrownBy(() -> stringifier.stringify(false))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("stringify(boolean) was called on a non-boolean stringifier: " + stringifier);
    }
    if (!set.contains(Binary.class)) {
      assertThatThrownBy(() -> stringifier.stringify(Binary.EMPTY))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("stringify(Binary) was called on a non-Binary stringifier: " + stringifier);
    }
  }
}
