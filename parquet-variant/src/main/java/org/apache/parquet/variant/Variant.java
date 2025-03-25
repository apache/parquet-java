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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.CharArrayWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Locale;
import java.util.UUID;
import org.apache.parquet.cli.util.RuntimeIOException;

/**
 * This Variant class holds the Variant-encoded value and metadata binary values.
 */
public final class Variant {
  /** The buffer that contains the Variant value. */
  final byte[] value;
  /** The starting index into `value` where the Variant value begins. */
  final int valuePos;

  /** The buffer that contains the Variant metadata. */
  final byte[] metadata;
  /** The starting index into `metadata` where the Variant metadata begins. */
  final int metadataPos;

  /**
   * The threshold to switch from linear search to binary search when looking up a field by key in
   * an object. This is a performance optimization to avoid the overhead of binary search for a
   * short list.
   */
  static final int BINARY_SEARCH_THRESHOLD = 32;

  static final ZoneId UTC = ZoneId.of("UTC");

  public Variant(byte[] value, byte[] metadata) {
    this(value, 0, metadata, 0);
  }

  Variant(byte[] value, int valuePos, byte[] metadata, int metadataPos) {
    if (valuePos < 0 || valuePos >= value.length) {
      throw new IllegalArgumentException(
          String.format("Invalid valuePos: %d. value.length: %d", valuePos, value.length));
    }
    if (metadataPos < 0 || metadataPos >= metadata.length) {
      throw new IllegalArgumentException(
          String.format("Invalid metadataPos: %d. metadata.length: %d", metadataPos, metadata.length));
    }
    this.value = value;
    this.valuePos = valuePos;

    this.metadata = metadata;
    this.metadataPos = metadataPos;
    // There is currently only one allowed version.
    if ((metadata[metadataPos] & VariantUtil.VERSION_MASK) != VariantUtil.VERSION) {
      throw new UnsupportedOperationException(String.format(
          "Unsupported variant metadata version: %02X", metadata[metadataPos] & VariantUtil.VERSION_MASK));
    }
  }

  /**
   * @return the boolean value
   */
  public boolean getBoolean() {
    return VariantUtil.getBoolean(value, valuePos);
  }

  /**
   * @return the byte value
   */
  public byte getByte() {
    long longValue = VariantUtil.getLong(value, valuePos);
    if (longValue < Byte.MIN_VALUE || longValue > Byte.MAX_VALUE) {
      throw new IllegalStateException("Value out of range for byte: " + longValue);
    }
    return (byte) longValue;
  }

  /**
   * @return the short value
   */
  public short getShort() {
    long longValue = VariantUtil.getLong(value, valuePos);
    if (longValue < Short.MIN_VALUE || longValue > Short.MAX_VALUE) {
      throw new IllegalStateException("Value out of range for short: " + longValue);
    }
    return (short) longValue;
  }

  /**
   * @return the int value
   */
  public int getInt() {
    long longValue = VariantUtil.getLong(value, valuePos);
    if (longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
      throw new IllegalStateException("Value out of range for int: " + longValue);
    }
    return (int) longValue;
  }

  /**
   * @return the long value
   */
  public long getLong() {
    return VariantUtil.getLong(value, valuePos);
  }

  /**
   * @return the double value
   */
  public double getDouble() {
    return VariantUtil.getDouble(value, valuePos);
  }

  /**
   * @return the decimal value
   */
  public BigDecimal getDecimal() {
    return VariantUtil.getDecimal(value, valuePos);
  }

  /**
   * @return the float value
   */
  public float getFloat() {
    return VariantUtil.getFloat(value, valuePos);
  }

  /**
   * @return the binary value
   */
  public byte[] getBinary() {
    return VariantUtil.getBinary(value, valuePos);
  }

  /**
   * @return the UUID value
   */
  public UUID getUUID() {
    return VariantUtil.getUUID(value, valuePos);
  }

  /**
   * @return the string value
   */
  public String getString() {
    return VariantUtil.getString(value, valuePos);
  }

  /**
   * @return the type of the variant value
   */
  public VariantUtil.Type getType() {
    return VariantUtil.getType(value, valuePos);
  }

  /**
   * @return the number of object fields in the variant. `getType()` must be `Type.OBJECT`.
   */
  public int numObjectElements() {
    return VariantUtil.getObjectInfo(value, valuePos).numElements;
  }

  /**
   * Returns the object field Variant value whose key is equal to `key`.
   * Return null if the key is not found. `getType()` must be `Type.OBJECT`.
   * @param key the key to look up
   * @return the field value whose key is equal to `key`, or null if key is not found
   */
  public Variant getFieldByKey(String key) {
    VariantUtil.ObjectInfo info = VariantUtil.getObjectInfo(value, valuePos);
    // Use linear search for a short list. Switch to binary search when the length reaches
    // `BINARY_SEARCH_THRESHOLD`.
    if (info.numElements < BINARY_SEARCH_THRESHOLD) {
      for (int i = 0; i < info.numElements; ++i) {
        ObjectField field = getFieldAtIndex(
            i,
            value,
            metadata,
            metadataPos,
            info.idSize,
            info.offsetSize,
            valuePos + info.idStartOffset,
            valuePos + info.offsetStartOffset,
            valuePos + info.dataStartOffset);
        if (field.key.equals(key)) {
          return field.value;
        }
      }
    } else {
      int low = 0;
      int high = info.numElements - 1;
      while (low <= high) {
        // Use unsigned right shift to compute the middle of `low` and `high`. This is not only a
        // performance optimization, because it can properly handle the case where `low + high`
        // overflows int.
        int mid = (low + high) >>> 1;
        ObjectField field = getFieldAtIndex(
            mid,
            value,
            metadata,
            metadataPos,
            info.idSize,
            info.offsetSize,
            valuePos + info.idStartOffset,
            valuePos + info.offsetStartOffset,
            valuePos + info.dataStartOffset);
        int cmp = field.key.compareTo(key);
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          return field.value;
        }
      }
    }
    return null;
  }

  /**
   * A field in a Variant object.
   */
  public static final class ObjectField {
    public final String key;
    public final Variant value;

    public ObjectField(String key, Variant value) {
      this.key = key;
      this.value = value;
    }
  }

  /**
   * Returns the ObjectField at the `index` slot. Return null if `index` is out of the bound of
   * `[0, objectSize())`. `getType()` must be `Type.OBJECT`.
   * @param index the index of the object field to get
   * @return the ObjectField at the `index` slot, or null if `index` is out of bounds
   */
  public ObjectField getFieldAtIndex(int index) {
    VariantUtil.ObjectInfo info = VariantUtil.getObjectInfo(value, valuePos);
    if (index < 0 || index >= info.numElements) {
      return null;
    }
    return getFieldAtIndex(
        index,
        value,
        metadata,
        metadataPos,
        info.idSize,
        info.offsetSize,
        valuePos + info.idStartOffset,
        valuePos + info.offsetStartOffset,
        valuePos + info.dataStartOffset);
  }

  private static ObjectField getFieldAtIndex(
      int index,
      byte[] value,
      byte[] metadata,
      int metadataPos,
      int idSize,
      int offsetSize,
      int idStart,
      int offsetStart,
      int dataStart) {
    // idStart, offsetStart, and dataStart are absolute positions in the `value` buffer.
    int id = VariantUtil.readUnsigned(value, idStart + idSize * index, idSize);
    int offset = VariantUtil.readUnsigned(value, offsetStart + offsetSize * index, offsetSize);
    String key = VariantUtil.getMetadataKey(metadata, metadataPos, id);
    Variant v = new Variant(value, dataStart + offset, metadata, metadataPos);
    return new ObjectField(key, v);
  }

  /**
   * @return the number of array elements. `getType()` must be `Type.ARRAY`.
   */
  public int numArrayElements() {
    return VariantUtil.getArrayInfo(value, valuePos).numElements;
  }

  /**
   * Returns the array element Variant value at the `index` slot. Returns null if `index` is
   * out of the bound of `[0, arraySize())`. `getType()` must be `Type.ARRAY`.
   * @param index the index of the array element to get
   * @return the array element Variant at the `index` slot, or null if `index` is out of bounds
   */
  public Variant getElementAtIndex(int index) {
    VariantUtil.ArrayInfo info = VariantUtil.getArrayInfo(value, valuePos);
    if (index < 0 || index >= info.numElements) {
      return null;
    }
    return getElementAtIndex(
        index,
        value,
        metadata,
        metadataPos,
        info.offsetSize,
        valuePos + info.offsetStartOffset,
        valuePos + info.dataStartOffset);
  }

  private static Variant getElementAtIndex(
      int index, byte[] value, byte[] metadata, int metadataPos, int offsetSize, int offsetStart, int dataStart) {
    // offsetStart and dataStart are absolute positions in the `value` buffer.
    int offset = VariantUtil.readUnsigned(value, offsetStart + offsetSize * index, offsetSize);
    return new Variant(value, dataStart + offset, metadata, metadataPos);
  }

  /**
   * @return the JSON representation of the variant
   * @throws MalformedVariantException if the variant is malformed
   */
  public String toJson() {
    return toJson(UTC, false);
  }

  /**
   * @param zoneId The ZoneId to use for formatting timestamps
   * @return the JSON representation of the variant
   * @throws MalformedVariantException if the variant is malformed
   */
  public String toJson(ZoneId zoneId) {
    return toJson(zoneId, false);
  }

  /**
   * @param zoneId The ZoneId to use for formatting timestamps
   * @param truncateTrailingZeros Whether to truncate trailing zeros in decimal values or timestamps
   * @return the JSON representation of the variant
   * @throws MalformedVariantException if the variant is malformed
   */
  public String toJson(ZoneId zoneId, boolean truncateTrailingZeros) {
    try (CharArrayWriter writer = new CharArrayWriter();
        JsonGenerator gen = new JsonFactory().createGenerator(writer)) {
      toJsonImpl(value, valuePos, metadata, metadataPos, gen, zoneId, truncateTrailingZeros);
      gen.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeIOException("Failed to convert variant to json", e);
    }
  }

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

  /** The format for a timestamp without time zone, truncating trailing microsecond zeros. */
  private static final DateTimeFormatter TIMESTAMP_NTZ_TRUNC_FORMATTER = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral('T')
      .appendPattern("HH:mm:ss")
      .optionalStart()
      .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
      .optionalEnd()
      .toFormatter(Locale.US);

  /**
   * The format for a timestamp without time zone, with nanosecond precision, truncating
   * trailing nanosecond zeros.
   */
  private static final DateTimeFormatter TIMESTAMP_NANOS_NTZ_TRUNC_FORMATTER = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral('T')
      .appendPattern("HH:mm:ss")
      .optionalStart()
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
      .optionalEnd()
      .toFormatter(Locale.US);

  /** The format for a timestamp with time zone, truncating trailing microsecond zeros. */
  private static final DateTimeFormatter TIMESTAMP_TRUNC_FORMATTER = new DateTimeFormatterBuilder()
      .append(TIMESTAMP_NTZ_TRUNC_FORMATTER)
      .appendOffset("+HH:MM", "+00:00")
      .toFormatter(Locale.US);

  /**
   * The format for a timestamp with time zone, with nanosecond precision, truncating trailing
   * nanosecond zeros.
   */
  private static final DateTimeFormatter TIMESTAMP_NANOS_TRUNC_FORMATTER = new DateTimeFormatterBuilder()
      .append(TIMESTAMP_NANOS_NTZ_TRUNC_FORMATTER)
      .appendOffset("+HH:MM", "+00:00")
      .toFormatter(Locale.US);

  /** The format for a time, truncating trailing microsecond zeros. */
  private static final DateTimeFormatter TIME_TRUNC_FORMATTER = new DateTimeFormatterBuilder()
      .appendPattern("HH:mm:ss")
      .optionalStart()
      .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
      .optionalEnd()
      .toFormatter(Locale.US);

  private static Instant microsToInstant(long microsSinceEpoch) {
    return Instant.EPOCH.plus(microsSinceEpoch, ChronoUnit.MICROS);
  }

  private static Instant nanosToInstant(long timestampNanos) {
    return Instant.EPOCH.plus(timestampNanos, ChronoUnit.NANOS);
  }

  private static void toJsonImpl(
      byte[] value,
      int valuePos,
      byte[] metadata,
      int metadataPos,
      JsonGenerator gen,
      ZoneId zoneId,
      boolean truncateTrailingZeros)
      throws IOException {
    switch (VariantUtil.getType(value, valuePos)) {
      case OBJECT: {
        VariantUtil.ObjectInfo info = VariantUtil.getObjectInfo(value, valuePos);
        gen.writeStartObject();
        for (int i = 0; i < info.numElements; ++i) {
          ObjectField field = getFieldAtIndex(
              i,
              value,
              metadata,
              metadataPos,
              info.idSize,
              info.offsetSize,
              valuePos + info.idStartOffset,
              valuePos + info.offsetStartOffset,
              valuePos + info.dataStartOffset);
          gen.writeFieldName(field.key);
          toJsonImpl(
              field.value.value,
              field.value.valuePos,
              field.value.metadata,
              metadataPos,
              gen,
              zoneId,
              truncateTrailingZeros);
        }
        gen.writeEndObject();
        break;
      }
      case ARRAY: {
        VariantUtil.ArrayInfo info = VariantUtil.getArrayInfo(value, valuePos);
        gen.writeStartArray();
        for (int i = 0; i < info.numElements; ++i) {
          Variant v = getElementAtIndex(
              i,
              value,
              metadata,
              metadataPos,
              info.offsetSize,
              valuePos + info.offsetStartOffset,
              valuePos + info.dataStartOffset);
          toJsonImpl(v.value, v.valuePos, v.metadata, metadataPos, gen, zoneId, truncateTrailingZeros);
        }
        gen.writeEndArray();
        break;
      }
      case NULL:
        gen.writeNull();
        break;
      case BOOLEAN:
        gen.writeBoolean(VariantUtil.getBoolean(value, valuePos));
        break;
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        gen.writeNumber(VariantUtil.getLong(value, valuePos));
        break;
      case STRING:
        gen.writeString(VariantUtil.getString(value, valuePos));
        break;
      case DOUBLE:
        gen.writeNumber(VariantUtil.getDouble(value, valuePos));
        break;
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        if (truncateTrailingZeros) {
          gen.writeNumber(VariantUtil.getDecimal(value, valuePos)
              .stripTrailingZeros()
              .toPlainString());
        } else {
          gen.writeNumber(VariantUtil.getDecimal(value, valuePos).toPlainString());
        }
        break;
      case DATE:
        gen.writeString(LocalDate.ofEpochDay((int) VariantUtil.getLong(value, valuePos))
            .toString());
        break;
      case TIMESTAMP:
        if (truncateTrailingZeros) {
          gen.writeString(
              TIMESTAMP_TRUNC_FORMATTER.format(microsToInstant(VariantUtil.getLong(value, valuePos))
                  .atZone(zoneId)));
        } else {
          gen.writeString(TIMESTAMP_FORMATTER.format(microsToInstant(VariantUtil.getLong(value, valuePos))
              .atZone(zoneId)));
        }
        break;
      case TIMESTAMP_NTZ:
        if (truncateTrailingZeros) {
          gen.writeString(
              TIMESTAMP_NTZ_TRUNC_FORMATTER.format(microsToInstant(VariantUtil.getLong(value, valuePos))
                  .atZone(ZoneOffset.UTC)));
        } else {
          gen.writeString(TIMESTAMP_NTZ_FORMATTER.format(microsToInstant(VariantUtil.getLong(value, valuePos))
              .atZone(ZoneOffset.UTC)));
        }
        break;
      case FLOAT:
        gen.writeNumber(VariantUtil.getFloat(value, valuePos));
        break;
      case BINARY:
        gen.writeString(Base64.getEncoder().encodeToString(VariantUtil.getBinary(value, valuePos)));
        break;
      case TIME:
        if (truncateTrailingZeros) {
          gen.writeString(TIME_TRUNC_FORMATTER.format(
              LocalTime.ofNanoOfDay(VariantUtil.getLong(value, valuePos) * 1_000)));
        } else {
          gen.writeString(
              TIME_FORMATTER.format(LocalTime.ofNanoOfDay(VariantUtil.getLong(value, valuePos) * 1_000)));
        }
        break;
      case TIMESTAMP_NANOS:
        if (truncateTrailingZeros) {
          gen.writeString(TIMESTAMP_NANOS_TRUNC_FORMATTER.format(
              nanosToInstant(VariantUtil.getLong(value, valuePos)).atZone(zoneId)));
        } else {
          gen.writeString(TIMESTAMP_NANOS_FORMATTER.format(
              nanosToInstant(VariantUtil.getLong(value, valuePos)).atZone(zoneId)));
        }
        break;
      case TIMESTAMP_NANOS_NTZ:
        if (truncateTrailingZeros) {
          gen.writeString(TIMESTAMP_NANOS_NTZ_TRUNC_FORMATTER.format(
              nanosToInstant(VariantUtil.getLong(value, valuePos)).atZone(ZoneOffset.UTC)));
        } else {
          gen.writeString(TIMESTAMP_NANOS_NTZ_FORMATTER.format(
              nanosToInstant(VariantUtil.getLong(value, valuePos)).atZone(ZoneOffset.UTC)));
        }
        break;
      case UUID:
        gen.writeString(VariantUtil.getUUID(value, valuePos).toString());
        break;
      default:
        throw new IllegalArgumentException("Unsupported type: " + VariantUtil.getType(value, valuePos));
    }
  }
}
