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

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Locale;
import java.util.UUID;

/**
 * This converts Variant scalar values to JSON.
 */
public class DefaultScalarToJson implements Variant.ScalarToJson {
  /** The format for a timestamp without time zone. */
  private static final DateTimeFormatter TIMESTAMP_NTZ_FORMATTER = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral('T')
      .appendPattern("HH:mm:ss")
      .appendFraction(ChronoField.MICRO_OF_SECOND, 6, 6, true)
      .toFormatter(Locale.US);

  /** The format for a timestamp without time zone, with nanosecond precision. */
  private static final DateTimeFormatter TIMESTAMP_NANOS_NTZ_FORMATTER = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral('T')
      .appendPattern("HH:mm:ss")
      .appendFraction(ChronoField.NANO_OF_SECOND, 9, 9, true)
      .toFormatter(Locale.US);

  /** The format for a timestamp with time zone. */
  private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
      .append(TIMESTAMP_NTZ_FORMATTER)
      .appendOffset("+HH:MM", "+00:00")
      .toFormatter(Locale.US);

  /** The format for a timestamp with time zone, with nanosecond precision. */
  private static final DateTimeFormatter TIMESTAMP_NANOS_FORMATTER = new DateTimeFormatterBuilder()
      .append(TIMESTAMP_NANOS_NTZ_FORMATTER)
      .appendOffset("+HH:MM", "+00:00")
      .toFormatter(Locale.US);

  /** The format for a time. */
  private static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder()
      .appendPattern("HH:mm:ss")
      .appendFraction(ChronoField.MICRO_OF_SECOND, 6, 6, true)
      .toFormatter(Locale.US);

  public void writeNull(JsonGenerator gen) throws IOException {
    gen.writeNull();
  }

  public void writeBoolean(JsonGenerator gen, boolean value) throws IOException {
    gen.writeBoolean(value);
  }

  public void writeByte(JsonGenerator gen, byte value) throws IOException {
    gen.writeNumber(value);
  }

  public void writeShort(JsonGenerator gen, short value) throws IOException {
    gen.writeNumber(value);
  }

  public void writeInt(JsonGenerator gen, int value) throws IOException {
    gen.writeNumber(value);
  }

  public void writeLong(JsonGenerator gen, long value) throws IOException {
    gen.writeNumber(value);
  }

  public void writeFloat(JsonGenerator gen, float value) throws IOException {
    gen.writeNumber(value);
  }

  public void writeDouble(JsonGenerator gen, double value) throws IOException {
    gen.writeNumber(value);
  }

  public void writeString(JsonGenerator gen, String value) throws IOException {
    gen.writeString(value);
  }

  public void writeBinary(JsonGenerator gen, byte[] value) throws IOException {
    gen.writeString(Base64.getEncoder().encodeToString(value));
  }

  public void writeDecimal(JsonGenerator gen, BigDecimal value) throws IOException {
    gen.writeNumber(value.toPlainString());
  }

  public void writeUUID(JsonGenerator gen, UUID value) throws IOException {
    gen.writeString(value.toString());
  }

  public void writeDate(JsonGenerator gen, int value) throws IOException {
    gen.writeString(LocalDate.ofEpochDay(value).toString());
  }

  public void writeTime(JsonGenerator gen, long microsSinceMidnight) throws IOException {
    gen.writeString(TIME_FORMATTER.format(LocalTime.ofNanoOfDay(microsSinceMidnight * 1_000)));
  }

  public void writeTimestamp(JsonGenerator gen, long microsSinceEpoch) throws IOException {
    gen.writeString(
        TIMESTAMP_FORMATTER.format(microsToInstant(microsSinceEpoch).atZone(ZoneOffset.UTC)));
  }

  public void writeTimestampNtz(JsonGenerator gen, long microsSinceEpoch) throws IOException {
    gen.writeString(
        TIMESTAMP_NTZ_FORMATTER.format(microsToInstant(microsSinceEpoch).atZone(ZoneOffset.UTC)));
  }

  public void writeTimestampNanos(JsonGenerator gen, long nanosSinceEpoch) throws IOException {
    gen.writeString(
        TIMESTAMP_NANOS_FORMATTER.format(nanosToInstant(nanosSinceEpoch).atZone(ZoneOffset.UTC)));
  }

  public void writeTimestampNanosNtz(JsonGenerator gen, long nanosSinceEpoch) throws IOException {
    gen.writeString(TIMESTAMP_NANOS_NTZ_FORMATTER.format(
        nanosToInstant(nanosSinceEpoch).atZone(ZoneOffset.UTC)));
  }

  protected Instant microsToInstant(long microsSinceEpoch) {
    return Instant.EPOCH.plus(microsSinceEpoch, ChronoUnit.MICROS);
  }

  protected Instant nanosToInstant(long timestampNanos) {
    return Instant.EPOCH.plus(timestampNanos, ChronoUnit.NANOS);
  }
}
