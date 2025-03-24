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

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestVariantEncoding {
  private static final Logger LOG = LoggerFactory.getLogger(TestVariantEncoding.class);
  private static final String RANDOM_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  private static final List<String> SAMPLE_JSON_VALUES = Arrays.asList(
      "null",
      "true",
      "false",
      "12",
      "-9876543210",
      "4.5678E123",
      "8.765E-2",
      "\"string value\"",
      "-9876.543",
      "234.456789",
      "{\"a\": 1, \"b\": {\"e\": -4, \"f\": 5.5}, \"c\": true}",
      "[1, -2, 4.5, -6.7, \"str\", true]");

  /** Random number generator for generating random strings */
  private static SecureRandom random = new SecureRandom();
  /** Object mapper for comparing json values */
  private final ObjectMapper mapper = new ObjectMapper();

  private void checkJson(String expected, String actual) {
    try {
      StreamReadConstraints.overrideDefaultStreamReadConstraints(
          StreamReadConstraints.builder().maxNestingDepth(100000).build());
      Assert.assertEquals(mapper.readTree(expected), mapper.readTree(actual));
    } catch (IOException e) {
      Assert.fail("Failed to parse json: " + e);
    }
  }

  private void checkJson(String jsonValue) {
    try {
      StreamReadConstraints.overrideDefaultStreamReadConstraints(
          StreamReadConstraints.builder().maxNestingDepth(100000).build());
      Variant v = VariantBuilder.parseJson(jsonValue);
      checkJson(jsonValue, v.toJson());
    } catch (IOException e) {
      Assert.fail("Failed to parse json: " + jsonValue + " " + e);
    }
  }

  private void checkType(Variant v, int expectedBasicType, VariantUtil.Type expectedType) {
    Assert.assertEquals(expectedBasicType, v.value[v.pos] & VariantUtil.BASIC_TYPE_MASK);
    Assert.assertEquals(expectedType, v.getType());
  }

  private long microsSinceEpoch(Instant instant) {
    return TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + instant.getNano() / 1000;
  }

  private long nanosSinceEpoch(Instant instant) {
    return TimeUnit.SECONDS.toNanos(instant.getEpochSecond()) + instant.getNano();
  }

  private String randomString(int len) {
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      sb.append(RANDOM_CHARS.charAt(random.nextInt(RANDOM_CHARS.length())));
    }
    return sb.toString();
  }

  @Test
  public void testNullJson() {
    checkJson("null");
  }

  @Test
  public void testBooleanJson() {
    Arrays.asList("true", "false").forEach(this::checkJson);
  }

  @Test
  public void testIntegerJson() {
    Arrays.asList(
            "0",
            Byte.toString(Byte.MIN_VALUE),
            Byte.toString(Byte.MAX_VALUE),
            Short.toString(Short.MIN_VALUE),
            Short.toString(Short.MAX_VALUE),
            Integer.toString(Integer.MIN_VALUE),
            Integer.toString(Integer.MAX_VALUE),
            Long.toString(Long.MIN_VALUE),
            Long.toString(Long.MAX_VALUE))
        .forEach(this::checkJson);
  }

  @Test
  public void testFloatJson() {
    Arrays.asList(
            Float.toString(Float.MIN_VALUE), Float.toString(Float.MAX_VALUE),
            Double.toString(Double.MIN_VALUE), Double.toString(Double.MAX_VALUE))
        .forEach(this::checkJson);
  }

  @Test
  public void testStringJson() {
    Arrays.asList("\"short string\"", "\"long string: " + new String(new char[1000]).replace("\0", "x") + "\"")
        .forEach(this::checkJson);
  }

  @Test
  public void testDecimalJson() {
    Arrays.asList(
            "12.34", "-43.21",
            "10.2147483647", "-1021474836.47",
            "109223372036854775.807", "-109.223372036854775807")
        .forEach(this::checkJson);
  }

  @Test
  public void testNullBuilder() {
    VariantBuilder vb = new VariantBuilder(false);
    vb.appendNull();
    checkType(vb.result(), VariantUtil.NULL, VariantUtil.Type.NULL);
  }

  @Test
  public void testBooleanBuilder() {
    Arrays.asList(true, false).forEach(b -> {
      VariantBuilder vb2 = new VariantBuilder(false);
      vb2.appendBoolean(b);
      checkType(vb2.result(), VariantUtil.PRIMITIVE, VariantUtil.Type.BOOLEAN);
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
          Variant v = vb2.result();
          if (Byte.MIN_VALUE <= l && l <= Byte.MAX_VALUE) {
            checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.BYTE);
          } else if (Short.MIN_VALUE <= l && l <= Short.MAX_VALUE) {
            checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.SHORT);
          } else if (Integer.MIN_VALUE <= l && l <= Integer.MAX_VALUE) {
            checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.INT);
          } else {
            checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.LONG);
          }
          Assert.assertEquals((long) l, v.getLong());
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
          Variant v = vb2.result();
          if (Byte.MIN_VALUE <= i && i <= Byte.MAX_VALUE) {
            checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.BYTE);
          } else if (Short.MIN_VALUE <= i && i <= Short.MAX_VALUE) {
            checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.SHORT);
          } else {
            checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.INT);
          }
          Assert.assertEquals((int) i, v.getInt());
        });

    Arrays.asList((short) 0, (short) Byte.MIN_VALUE, (short) Byte.MAX_VALUE, Short.MIN_VALUE, Short.MAX_VALUE)
        .forEach(s -> {
          VariantBuilder vb2 = new VariantBuilder(false);
          vb2.appendLong(s);
          Variant v = vb2.result();
          if (Byte.MIN_VALUE <= s && s <= Byte.MAX_VALUE) {
            checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.BYTE);
          } else {
            checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.SHORT);
          }
          Assert.assertEquals((short) s, v.getShort());
        });

    Arrays.asList((byte) 0, Byte.MIN_VALUE, Byte.MAX_VALUE).forEach(b -> {
      VariantBuilder vb2 = new VariantBuilder(false);
      vb2.appendLong(b);
      Variant v = vb2.result();
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.BYTE);
      Assert.assertEquals((byte) b, v.getByte());
    });
  }

  @Test
  public void testFloatBuilder() {
    Arrays.asList(Float.MIN_VALUE, Float.MAX_VALUE).forEach(f -> {
      VariantBuilder vb2 = new VariantBuilder(false);
      vb2.appendFloat(f);
      Variant v = vb2.result();
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.FLOAT);
      Assert.assertEquals(f, v.getFloat(), 0.000001);
    });
  }

  @Test
  public void testDoubleBuilder() {
    Arrays.asList(Double.MIN_VALUE, Double.MAX_VALUE).forEach(d -> {
      VariantBuilder vb2 = new VariantBuilder(false);
      vb2.appendDouble(d);
      Variant v = vb2.result();
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.DOUBLE);
      Assert.assertEquals(d, v.getDouble(), 0.000001);
    });
  }

  @Test
  public void testStringBuilder() {
    IntStream.range(VariantUtil.MAX_SHORT_STR_SIZE - 3, VariantUtil.MAX_SHORT_STR_SIZE + 3)
        .forEach(len -> {
          VariantBuilder vb2 = new VariantBuilder(false);
          String s = randomString(len);
          vb2.appendString(s);
          Variant v = vb2.result();
          if (len <= VariantUtil.MAX_SHORT_STR_SIZE) {
            checkType(v, VariantUtil.SHORT_STR, VariantUtil.Type.STRING);
          } else {
            checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.STRING);
          }
          Assert.assertEquals(s, v.getString());
        });
  }

  @Test
  public void testDecimalBuilder() {
    // decimal4
    Arrays.asList(new BigDecimal("123.456"), new BigDecimal("-987.654")).forEach(d -> {
      VariantBuilder vb2 = new VariantBuilder(false);
      vb2.appendDecimal(d);
      Variant v = vb2.result();
      checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.DECIMAL4);
      Assert.assertEquals(d, v.getDecimal());
    });

    // decimal8
    Arrays.asList(new BigDecimal("10.2147483647"), new BigDecimal("-1021474836.47"))
        .forEach(d -> {
          VariantBuilder vb2 = new VariantBuilder(false);
          vb2.appendDecimal(d);
          Variant v = vb2.result();
          checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.DECIMAL8);
          Assert.assertEquals(d, v.getDecimal());
        });

    // decimal16
    Arrays.asList(new BigDecimal("109223372036854775.807"), new BigDecimal("-109.223372036854775807"))
        .forEach(d -> {
          VariantBuilder vb2 = new VariantBuilder(false);
          vb2.appendDecimal(d);
          Variant v = vb2.result();
          checkType(v, VariantUtil.PRIMITIVE, VariantUtil.Type.DECIMAL16);
          Assert.assertEquals(d, v.getDecimal());
        });
  }

  @Test
  public void testDate() {
    VariantBuilder vb = new VariantBuilder(false);
    int days = Math.toIntExact(LocalDate.of(2024, 12, 16).toEpochDay());
    vb.appendDate(days);
    Assert.assertEquals("\"2024-12-16\"", vb.result().toJson());
    Assert.assertEquals(days, vb.result().getInt());
  }

  @Test
  public void testTimestamp() {
    DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE_TIME;
    VariantBuilder vb = new VariantBuilder(false);
    long micros = microsSinceEpoch(Instant.from(dtf.parse("2024-12-16T10:23:45.321456-08:00")));
    vb.appendTimestamp(micros);
    Assert.assertEquals("\"2024-12-16T18:23:45.321456+00:00\"", vb.result().toJson());
    Assert.assertEquals("\"2024-12-16T10:23:45.321456-08:00\"", vb.result().toJson(ZoneId.of("-08:00")));
    Assert.assertEquals("\"2024-12-16T19:23:45.321456+01:00\"", vb.result().toJson(ZoneId.of("+01:00")));
    Assert.assertEquals(micros, vb.result().getLong());
  }

  @Test
  public void testTimestampNtz() {
    DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE_TIME;
    VariantBuilder vb = new VariantBuilder(false);
    long micros = microsSinceEpoch(Instant.from(dtf.parse("2024-01-01T23:00:00.000001Z")));
    vb.appendTimestampNtz(micros);
    Assert.assertEquals("\"2024-01-01T23:00:00.000001\"", vb.result().toJson());
    Assert.assertEquals("\"2024-01-01T23:00:00.000001\"", vb.result().toJson(ZoneId.of("-08:00")));
    Assert.assertEquals(vb.result().toJson(ZoneId.of("-08:00")), vb.result().toJson(ZoneId.of("+02:00")));
    Assert.assertEquals(micros, vb.result().getLong());
  }

  @Test
  public void testTime() {
    for (String timeStr : Arrays.asList(
        "00:00:00.000000", "00:00:00.000120", "12:00:00.000000", "12:00:00.002300", "23:59:59.999999")) {
      VariantBuilder vb = new VariantBuilder(false);
      long micros = LocalTime.parse(timeStr).toNanoOfDay() / 1_000;
      vb.appendTime(micros);
      Assert.assertEquals(String.format("\"%s\"", timeStr), vb.result().toJson());
      Assert.assertEquals(micros, vb.result().getLong());
    }
  }

  @Test
  public void testTimestampNanos() {
    DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE_TIME;
    VariantBuilder vb = new VariantBuilder(false);
    long nanos = nanosSinceEpoch(Instant.from(dtf.parse("2024-12-16T10:23:45.321456987-08:00")));
    vb.appendTimestampNanos(nanos);
    Assert.assertEquals(
        "\"2024-12-16T18:23:45.321456987+00:00\"", vb.result().toJson());
    Assert.assertEquals(
        "\"2024-12-16T10:23:45.321456987-08:00\"", vb.result().toJson(ZoneId.of("-08:00")));
    Assert.assertEquals(
        "\"2024-12-16T19:23:45.321456987+01:00\"", vb.result().toJson(ZoneId.of("+01:00")));
    Assert.assertEquals(nanos, vb.result().getLong());
  }

  @Test
  public void testTimestampNanosNtz() {
    DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE_TIME;
    VariantBuilder vb = new VariantBuilder(false);
    long nanos = nanosSinceEpoch(Instant.from(dtf.parse("2024-01-01T23:00:00.839280983Z")));
    vb.appendTimestampNanosNtz(nanos);
    Assert.assertEquals("\"2024-01-01T23:00:00.839280983\"", vb.result().toJson());
    Assert.assertEquals("\"2024-01-01T23:00:00.839280983\"", vb.result().toJson(ZoneId.of("-08:00")));
    Assert.assertEquals(vb.result().toJson(ZoneId.of("-08:00")), vb.result().toJson(ZoneId.of("+02:00")));
    Assert.assertEquals(nanos, vb.result().getLong());
  }

  @Test
  public void testBinary() {
    VariantBuilder vb = new VariantBuilder(false);
    byte[] binary = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    vb.appendBinary(binary);
    Assert.assertEquals(
        "\"" + Base64.getEncoder().encodeToString(binary) + "\"",
        vb.result().toJson());
    Assert.assertArrayEquals(binary, vb.result().getBinary());
  }

  @Test
  public void testUUID() {
    VariantBuilder vb = new VariantBuilder(false);
    byte[] uuid = new byte[] {0, 17, 34, 51, 68, 85, 102, 119, -120, -103, -86, -69, -52, -35, -18, -1};
    long msb = ByteBuffer.wrap(uuid, 0, 8).order(ByteOrder.BIG_ENDIAN).getLong();
    long lsb = ByteBuffer.wrap(uuid, 8, 8).order(ByteOrder.BIG_ENDIAN).getLong();
    UUID expected = new UUID(msb, lsb);

    vb.appendUUID(expected);
    Assert.assertEquals(
        "\"00112233-4455-6677-8899-aabbccddeeff\"", vb.result().toJson());
    Assert.assertEquals(expected, vb.result().getUUID());
  }

  @Test
  public void testObject() {
    // simple object
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    for (int i = 0; i < SAMPLE_JSON_VALUES.size(); i++) {
      if (i > 0) sb.append(", ");
      sb.append("\"field" + i + "\": ").append(SAMPLE_JSON_VALUES.get(i));
    }
    sb.append("}");
    checkJson(sb.toString());

    // wide object
    sb = new StringBuilder();
    sb.append("{");
    for (int i = 0; i < 50000; i++) {
      if (i > 0) sb.append(", ");
      sb.append("\"field" + i + "\": ").append(SAMPLE_JSON_VALUES.get(i % SAMPLE_JSON_VALUES.size()));
    }
    sb.append("}");
    checkJson(sb.toString());

    // deep object
    sb = new StringBuilder();
    // Jackson object mapper hit a stack overflow if json is too deep
    for (int i = 0; i < 500; i++) {
      sb.append("{").append("\"field" + i + "\": ");
    }
    sb.append("{");
    for (int i = 0; i < SAMPLE_JSON_VALUES.size(); i++) {
      if (i > 0) sb.append(", ");
      sb.append("\"field" + i + "\": ").append(SAMPLE_JSON_VALUES.get(i));
    }
    sb.append("}");
    for (int i = 0; i < 500; i++) {
      sb.append("}");
    }
    checkJson(sb.toString());
  }

  @Test
  public void testGetObjectFields() throws IOException {
    // Create small object for linear search
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    for (int i = 0; i < Variant.BINARY_SEARCH_THRESHOLD / 2; i++) {
      if (i > 0) sb.append(", ");
      sb.append("\"field" + i + "\": ").append(i);
    }
    sb.append("}");
    Variant v = VariantBuilder.parseJson(sb.toString());
    Assert.assertEquals(Variant.BINARY_SEARCH_THRESHOLD / 2, v.numObjectElements());
    for (int i = 0; i < Variant.BINARY_SEARCH_THRESHOLD / 2; i++) {
      String actual = v.getFieldByKey("field" + i).toJson();
      Assert.assertEquals(String.valueOf(i), actual);
      // check by index
      Variant.ObjectField field = v.getFieldAtIndex(i);
      Assert.assertTrue(field.key.startsWith("field"));
      Assert.assertEquals(field.key.substring("field".length()), field.value.toJson());
    }

    // Create larger object for binary search
    sb = new StringBuilder();
    sb.append("{");
    for (int i = 0; i < 2 * Variant.BINARY_SEARCH_THRESHOLD; i++) {
      if (i > 0) sb.append(", ");
      sb.append("\"field" + i + "\": ").append(i);
    }
    sb.append("}");
    v = VariantBuilder.parseJson(sb.toString());
    Assert.assertEquals(2 * Variant.BINARY_SEARCH_THRESHOLD, v.numObjectElements());
    for (int i = 0; i < 2 * Variant.BINARY_SEARCH_THRESHOLD; i++) {
      String actual = v.getFieldByKey("field" + i).toJson();
      Assert.assertEquals(String.valueOf(i), actual);
      // check by index
      Variant.ObjectField field = v.getFieldAtIndex(i);
      Assert.assertTrue(field.key.startsWith("field"));
      Assert.assertEquals(field.key.substring("field".length()), field.value.toJson());
    }
  }

  @Test
  public void testArray() throws IOException {
    // simple array
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < SAMPLE_JSON_VALUES.size(); i++) {
      if (i > 0) sb.append(", ");
      sb.append(SAMPLE_JSON_VALUES.get(i));
    }
    sb.append("]");
    checkJson(sb.toString());
    // Check array elements
    Variant v = VariantBuilder.parseJson(sb.toString());
    Assert.assertEquals(SAMPLE_JSON_VALUES.size(), v.numArrayElements());
    for (int i = 0; i < SAMPLE_JSON_VALUES.size(); i++) {
      String actual = v.getElementAtIndex(i).toJson();
      checkJson(SAMPLE_JSON_VALUES.get(i), actual);
    }

    // large array
    sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < 50000; i++) {
      if (i > 0) sb.append(", ");
      sb.append(SAMPLE_JSON_VALUES.get(i % SAMPLE_JSON_VALUES.size()));
    }
    sb.append("]");
    checkJson(sb.toString());
    // Check array elements
    v = VariantBuilder.parseJson(sb.toString());
    Assert.assertEquals(50000, v.numArrayElements());
    for (int i = 0; i < 50000; i++) {
      String actual = v.getElementAtIndex(i).toJson();
      checkJson(SAMPLE_JSON_VALUES.get(i % SAMPLE_JSON_VALUES.size()), actual);
    }
  }

  @Test
  public void testAllowDuplicateKeys() {
    // disallow duplicate keys
    try {
      VariantBuilder.parseJson("{\"a\": 1, \"a\": 2}");
      Assert.fail("Expected VariantDuplicateKeyException with duplicate keys");
    } catch (IOException e) {
      Assert.fail("Expected VariantDuplicateKeyException with duplicate keys");
    } catch (VariantDuplicateKeyException e) {
      // Expected
    }

    // allow duplicate keys
    try {
      Variant v = VariantBuilder.parseJson("{\"a\": 1, \"a\": 2}", new VariantBuilder(true));
      Assert.assertEquals(1, v.numObjectElements());
      Assert.assertEquals(VariantUtil.Type.BYTE, v.getFieldByKey("a").getType());
      Assert.assertEquals(2, v.getFieldByKey("a").getLong());
    } catch (Exception e) {
      Assert.fail("Unexpected exception: " + e);
    }
  }

  @Test
  public void testTruncateTrailingZeroDecimal() {
    for (String[] strings : Arrays.asList(
        // decimal4
        // truncate all trailing zeros
        new String[] {"1234.0000", "1234"},
        // truncate some trailing zeros
        new String[] {"1234.5600", "1234.56"},
        // truncate no trailing zeros
        new String[] {"1234.5678", "1234.5678"},
        // decimal8
        // truncate all trailing zeros
        new String[] {"-10.0000000000", "-10"},
        // truncate some trailing zeros
        new String[] {"-10.2147000000", "-10.2147"},
        // truncate no trailing zeros
        new String[] {"-10.2147483647", "-10.2147483647"},
        // decimal16
        // truncate all trailing zeros
        new String[] {"1092233720368547.00000", "1092233720368547"},
        // truncate some trailing zeros
        new String[] {"1092233720368547.75800", "1092233720368547.758"},
        // truncate no trailing zeros
        new String[] {"1092233720368547.75807", "1092233720368547.75807"})) {
      VariantBuilder vb = new VariantBuilder(false);
      BigDecimal d = new BigDecimal(strings[0]);
      vb.appendDecimal(d);
      Variant v = vb.result();
      Assert.assertEquals(strings[0], v.toJson());
      Assert.assertEquals(strings[1], v.toJson(ZoneId.of("UTC"), true));
    }
  }

  @Test
  public void testTruncateTrailingZeroTimestamp() {
    DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE_TIME;
    for (String[] strings : Arrays.asList(
        // truncate all trailing zeros
        new String[] {"2024-12-16T10:23:45.000000-08:00", "2024-12-16T10:23:45-08:00"},
        // truncate all trailing zeros
        new String[] {"2024-12-16T10:23:45.123000-08:00", "2024-12-16T10:23:45.123-08:00"},
        // truncate no trailing zeros
        new String[] {"2024-12-16T10:23:45.123456-08:00", "2024-12-16T10:23:45.123456-08:00"})) {
      VariantBuilder vb = new VariantBuilder(false);
      long micros = microsSinceEpoch(Instant.from(dtf.parse(strings[0])));
      vb.appendTimestamp(micros);
      Variant v = vb.result();
      Assert.assertEquals(String.format("\"%s\"", strings[0]), v.toJson(ZoneId.of("-08:00")));
      Assert.assertEquals(String.format("\"%s\"", strings[1]), v.toJson(ZoneId.of("-08:00"), true));
    }
  }

  @Test
  public void testTruncateTrailingZeroTimestampNtz() {
    DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE_TIME;
    for (String[] strings : Arrays.asList(
        // truncate all trailing zeros
        new String[] {"2024-12-16T10:23:45.000000", "2024-12-16T10:23:45"},
        // truncate all trailing zeros
        new String[] {"2024-12-16T10:23:45.123000", "2024-12-16T10:23:45.123"},
        // truncate no trailing zeros
        new String[] {"2024-12-16T10:23:45.123456", "2024-12-16T10:23:45.123456"})) {
      VariantBuilder vb = new VariantBuilder(false);

      long micros = microsSinceEpoch(Instant.from(dtf.parse(String.format("%sZ", strings[0]))));
      vb.appendTimestampNtz(micros);
      Variant v = vb.result();
      Assert.assertEquals(String.format("\"%s\"", strings[0]), v.toJson(ZoneId.of("-08:00")));
      Assert.assertEquals(String.format("\"%s\"", strings[1]), v.toJson(ZoneId.of("-08:00"), true));
      Assert.assertEquals(micros, vb.result().getLong());
    }
  }

  @Test
  public void testTruncateTrailingZeroTime() {
    for (String[] strings : Arrays.asList(
        // truncate all trailing zeros
        new String[] {"10:23:45.000000", "10:23:45"},
        // truncate some trailing zeros
        new String[] {"10:23:45.123000", "10:23:45.123"},
        // truncate no trailing zeros
        new String[] {"10:23:45.123456", "10:23:45.123456"})) {
      VariantBuilder vb = new VariantBuilder(false);

      long micros = LocalTime.parse(strings[0]).toNanoOfDay() / 1_000;
      vb.appendTime(micros);
      Variant v = vb.result();
      Assert.assertEquals(String.format("\"%s\"", strings[0]), v.toJson(ZoneId.of("-08:00")));
      Assert.assertEquals(String.format("\"%s\"", strings[1]), v.toJson(ZoneId.of("-08:00"), true));
      Assert.assertEquals(micros, vb.result().getLong());
    }
  }

  @Test
  public void testTruncateTrailingZeroTimestampNanos() {
    DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE_TIME;
    for (String[] strings : Arrays.asList(
        // truncate all trailing zeros
        new String[] {"2024-12-16T10:23:45.000000000-08:00", "2024-12-16T10:23:45-08:00"},
        // truncate some trailing zeros
        new String[] {"2024-12-16T10:23:45.123450000-08:00", "2024-12-16T10:23:45.12345-08:00"},
        // truncate no trailing zeros
        new String[] {"2024-12-16T10:23:45.123456789-08:00", "2024-12-16T10:23:45.123456789-08:00"})) {
      VariantBuilder vb = new VariantBuilder(false);
      long nanos = nanosSinceEpoch(Instant.from(dtf.parse(strings[0])));
      vb.appendTimestampNanos(nanos);
      Variant v = vb.result();
      Assert.assertEquals(String.format("\"%s\"", strings[0]), v.toJson(ZoneId.of("-08:00")));
      Assert.assertEquals(String.format("\"%s\"", strings[1]), v.toJson(ZoneId.of("-08:00"), true));
    }
  }

  @Test
  public void testTruncateTrailingZeroTimestampNanosNtz() {
    DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE_TIME;
    for (String[] strings : Arrays.asList(
        // truncate all trailing zeros
        new String[] {"2024-12-16T10:23:45.000000000", "2024-12-16T10:23:45"},
        // truncate some trailing zeros
        new String[] {"2024-12-16T10:23:45.123450000", "2024-12-16T10:23:45.12345"},
        // truncate no trailing zeros
        new String[] {"2024-12-16T10:23:45.123456789", "2024-12-16T10:23:45.123456789"})) {
      VariantBuilder vb = new VariantBuilder(false);

      long nanos = nanosSinceEpoch(Instant.from(dtf.parse(String.format("%sZ", strings[0]))));
      vb.appendTimestampNanosNtz(nanos);
      Variant v = vb.result();
      Assert.assertEquals(String.format("\"%s\"", strings[0]), v.toJson(ZoneId.of("-08:00")));
      Assert.assertEquals(String.format("\"%s\"", strings[1]), v.toJson(ZoneId.of("-08:00"), true));
      Assert.assertEquals(nanos, vb.result().getLong());
    }
  }
}
