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
import java.util.Arrays;
import java.util.Base64;
import java.util.Locale;
import java.util.UUID;
import org.apache.parquet.cli.util.RuntimeIOException;

/**
 * This Variant class holds the Variant-encoded value and metadata binary values.
 */
public final class Variant {
  final byte[] value;
  final byte[] metadata;
  /**
   * The starting index into `value` where the variant value starts. This is used to avoid copying
   * the value binary when reading a sub-variant in the array/object element.
   */
  final int pos;

  /**
   * The threshold to switch from linear search to binary search when looking up a field by key in
   * an object. This is a performance optimization to avoid the overhead of binary search for a
   * short list.
   */
  static final int BINARY_SEARCH_THRESHOLD = 32;

  static final ZoneId UTC = ZoneId.of("UTC");

  public Variant(byte[] value, byte[] metadata) {
    this(value, metadata, 0);
  }

  Variant(byte[] value, byte[] metadata, int pos) {
    this.value = value;
    this.metadata = metadata;
    this.pos = pos;
    // There is currently only one allowed version.
    if (metadata.length < 1 || (metadata[0] & VariantUtil.VERSION_MASK) != VariantUtil.VERSION) {
      throw new MalformedVariantException(String.format(
          "Unsupported variant metadata version: %02X", metadata[0] & VariantUtil.VERSION_MASK));
    }
  }

  public byte[] getValue() {
    if (pos == 0) {
      // Position 0 means the entire value is used. Return the original value.
      return value;
    }
    int size = VariantUtil.valueSize(value, pos);
    VariantUtil.checkIndex(pos + size - 1, value.length);
    return Arrays.copyOfRange(value, pos, pos + size);
  }

  public byte[] getMetadata() {
    return metadata;
  }

  /**
   * @return the boolean value
   */
  public boolean getBoolean() {
    return VariantUtil.getBoolean(value, pos);
  }

  /**
   * @return the byte value
   */
  public byte getByte() {
    long longValue = VariantUtil.getLong(value, pos);
    if (longValue < Byte.MIN_VALUE || longValue > Byte.MAX_VALUE) {
      throw new IllegalStateException("Value out of range for byte: " + longValue);
    }
    return (byte) longValue;
  }

  /**
   * @return the short value
   */
  public short getShort() {
    long longValue = VariantUtil.getLong(value, pos);
    if (longValue < Short.MIN_VALUE || longValue > Short.MAX_VALUE) {
      throw new IllegalStateException("Value out of range for short: " + longValue);
    }
    return (short) longValue;
  }

  /**
   * @return the int value
   */
  public int getInt() {
    long longValue = VariantUtil.getLong(value, pos);
    if (longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
      throw new IllegalStateException("Value out of range for int: " + longValue);
    }
    return (int) longValue;
  }

  /**
   * @return the long value
   */
  public long getLong() {
    return VariantUtil.getLong(value, pos);
  }

  /**
   * @return the double value
   */
  public double getDouble() {
    return VariantUtil.getDouble(value, pos);
  }

  /**
   * @return the decimal value
   */
  public BigDecimal getDecimal() {
    return VariantUtil.getDecimal(value, pos);
  }

  /**
   * @return the float value
   */
  public float getFloat() {
    return VariantUtil.getFloat(value, pos);
  }

  /**
   * @return the binary value
   */
  public byte[] getBinary() {
    return VariantUtil.getBinary(value, pos);
  }

  /**
   * @return the UUID value
   */
  public UUID getUUID() {
    return VariantUtil.getUUID(value, pos);
  }

  /**
   * @return the string value
   */
  public String getString() {
    return VariantUtil.getString(value, pos);
  }

  /**
   * @return the primitive type id from a variant value
   */
  public int getPrimitiveTypeId() {
    return VariantUtil.getPrimitiveTypeId(value, pos);
  }

  /**
   * @return the type of the variant value
   */
  public VariantUtil.Type getType() {
    return VariantUtil.getType(value, pos);
  }

  /**
   * @return the number of object fields in the variant. `getType()` must be `Type.OBJECT`.
   */
  public int numObjectElements() {
    return VariantUtil.handleObject(value, pos, (info) -> info.numElements);
  }

  /**
   * Returns the object field Variant value whose key is equal to `key`.
   * Return null if the key is not found. `getType()` must be `Type.OBJECT`.
   * @param key the key to look up
   * @return the field value whose key is equal to `key`, or null if key is not found
   */
  public Variant getFieldByKey(String key) {
    return VariantUtil.handleObject(value, pos, (info) -> {
      // Use linear search for a short list. Switch to binary search when the length reaches
      // `BINARY_SEARCH_THRESHOLD`.
      if (info.numElements < BINARY_SEARCH_THRESHOLD) {
        for (int i = 0; i < info.numElements; ++i) {
          ObjectField field = getFieldAtIndex(
              i,
              value,
              metadata,
              info.idSize,
              info.offsetSize,
              info.idStart,
              info.offsetStart,
              info.dataStart);
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
              info.idSize,
              info.offsetSize,
              info.idStart,
              info.offsetStart,
              info.dataStart);
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
    });
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
    return VariantUtil.handleObject(value, pos, (info) -> {
      if (index < 0 || index >= info.numElements) {
        return null;
      }
      return getFieldAtIndex(
          index,
          value,
          metadata,
          info.idSize,
          info.offsetSize,
          info.idStart,
          info.offsetStart,
          info.dataStart);
    });
  }

  private static ObjectField getFieldAtIndex(
      int index,
      byte[] value,
      byte[] metadata,
      int idSize,
      int offsetSize,
      int idStart,
      int offsetStart,
      int dataStart) {
    int id = VariantUtil.readUnsigned(value, idStart + idSize * index, idSize);
    int offset = VariantUtil.readUnsigned(value, offsetStart + offsetSize * index, offsetSize);
    String key = VariantUtil.getMetadataKey(metadata, id);
    Variant v = new Variant(value, metadata, dataStart + offset);
    return new ObjectField(key, v);
  }

  /**
   * @return the number of array elements. `getType()` must be `Type.ARRAY`.
   */
  public int numArrayElements() {
    return VariantUtil.handleArray(value, pos, (info) -> info.numElements);
  }

  /**
   * Returns the array element Variant value at the `index` slot. Returns null if `index` is
   * out of the bound of `[0, arraySize())`. `getType()` must be `Type.ARRAY`.
   * @param index the index of the array element to get
   * @return the array element Variant at the `index` slot, or null if `index` is out of bounds
   */
  public Variant getElementAtIndex(int index) {
    return VariantUtil.handleArray(value, pos, (info) -> {
      if (index < 0 || index >= info.numElements) {
        return null;
      }
      return getElementAtIndex(index, value, metadata, info.offsetSize, info.offsetStart, info.dataStart);
    });
  }

  private static Variant getElementAtIndex(
      int index, byte[] value, byte[] metadata, int offsetSize, int offsetStart, int dataStart) {
    int offset = VariantUtil.readUnsigned(value, offsetStart + offsetSize * index, offsetSize);
    return new Variant(value, metadata, dataStart + offset);
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
      toJsonImpl(value, metadata, pos, gen, zoneId, truncateTrailingZeros);
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
      byte[] value, byte[] metadata, int pos, JsonGenerator gen, ZoneId zoneId, boolean truncateTrailingZeros)
      throws IOException {
    switch (VariantUtil.getType(value, pos)) {
      case OBJECT:
        VariantUtil.handleObjectException(value, pos, (info) -> {
          gen.writeStartObject();
          for (int i = 0; i < info.numElements; ++i) {
            ObjectField field = getFieldAtIndex(
                i,
                value,
                metadata,
                info.idSize,
                info.offsetSize,
                info.idStart,
                info.offsetStart,
                info.dataStart);
            gen.writeFieldName(field.key);
            toJsonImpl(
                field.value.value,
                field.value.metadata,
                field.value.pos,
                gen,
                zoneId,
                truncateTrailingZeros);
          }
          gen.writeEndObject();
          return null;
        });
        break;
      case ARRAY:
        VariantUtil.handleArrayException(value, pos, (info) -> {
          gen.writeStartArray();
          for (int i = 0; i < info.numElements; ++i) {
            Variant v = getElementAtIndex(
                i, value, metadata, info.offsetSize, info.offsetStart, info.dataStart);
            toJsonImpl(v.value, v.metadata, v.pos, gen, zoneId, truncateTrailingZeros);
          }
          gen.writeEndArray();
          return null;
        });
        break;
      case NULL:
        gen.writeNull();
        break;
      case BOOLEAN:
        gen.writeBoolean(VariantUtil.getBoolean(value, pos));
        break;
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        gen.writeNumber(VariantUtil.getLong(value, pos));
        break;
      case STRING:
        gen.writeString(VariantUtil.getString(value, pos));
        break;
      case DOUBLE:
        gen.writeNumber(VariantUtil.getDouble(value, pos));
        break;
      case DECIMAL:
        if (truncateTrailingZeros) {
          gen.writeNumber(VariantUtil.getDecimal(value, pos)
              .stripTrailingZeros()
              .toPlainString());
        } else {
          gen.writeNumber(VariantUtil.getDecimal(value, pos).toPlainString());
        }
        break;
      case DATE:
        gen.writeString(LocalDate.ofEpochDay((int) VariantUtil.getLong(value, pos))
            .toString());
        break;
      case TIMESTAMP:
        if (truncateTrailingZeros) {
          gen.writeString(TIMESTAMP_TRUNC_FORMATTER.format(
              microsToInstant(VariantUtil.getLong(value, pos)).atZone(zoneId)));
        } else {
          gen.writeString(TIMESTAMP_FORMATTER.format(
              microsToInstant(VariantUtil.getLong(value, pos)).atZone(zoneId)));
        }
        break;
      case TIMESTAMP_NTZ:
        if (truncateTrailingZeros) {
          gen.writeString(TIMESTAMP_NTZ_TRUNC_FORMATTER.format(
              microsToInstant(VariantUtil.getLong(value, pos)).atZone(ZoneOffset.UTC)));
        } else {
          gen.writeString(TIMESTAMP_NTZ_FORMATTER.format(
              microsToInstant(VariantUtil.getLong(value, pos)).atZone(ZoneOffset.UTC)));
        }
        break;
      case FLOAT:
        gen.writeNumber(VariantUtil.getFloat(value, pos));
        break;
      case BINARY:
        gen.writeString(Base64.getEncoder().encodeToString(VariantUtil.getBinary(value, pos)));
        break;
      case TIME:
        if (truncateTrailingZeros) {
          gen.writeString(TIME_TRUNC_FORMATTER.format(
              LocalTime.ofNanoOfDay(VariantUtil.getLong(value, pos) * 1_000)));
        } else {
          gen.writeString(
              TIME_FORMATTER.format(LocalTime.ofNanoOfDay(VariantUtil.getLong(value, pos) * 1_000)));
        }
        break;
      case TIMESTAMP_NANOS:
        if (truncateTrailingZeros) {
          gen.writeString(TIMESTAMP_NANOS_TRUNC_FORMATTER.format(
              nanosToInstant(VariantUtil.getLong(value, pos)).atZone(zoneId)));
        } else {
          gen.writeString(TIMESTAMP_NANOS_FORMATTER.format(
              nanosToInstant(VariantUtil.getLong(value, pos)).atZone(zoneId)));
        }
        break;
      case TIMESTAMP_NANOS_NTZ:
        if (truncateTrailingZeros) {
          gen.writeString(TIMESTAMP_NANOS_NTZ_TRUNC_FORMATTER.format(
              nanosToInstant(VariantUtil.getLong(value, pos)).atZone(ZoneOffset.UTC)));
        } else {
          gen.writeString(TIMESTAMP_NANOS_NTZ_FORMATTER.format(
              nanosToInstant(VariantUtil.getLong(value, pos)).atZone(ZoneOffset.UTC)));
        }
        break;
      case UUID:
        gen.writeString(VariantUtil.getUUID(value, pos).toString());
        break;
      default:
        throw new IllegalArgumentException("Unsupported type: " + VariantUtil.getType(value, pos));
    }
  }
}
