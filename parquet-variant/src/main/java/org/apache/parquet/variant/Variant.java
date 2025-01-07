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
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.Locale;

import static java.time.temporal.ChronoField.*;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static org.apache.parquet.variant.VariantUtil.*;

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

  public Variant(byte[] value, byte[] metadata) {
    this(value, metadata, 0);
  }

  Variant(byte[] value, byte[] metadata, int pos) {
    this.value = value;
    this.metadata = metadata;
    this.pos = pos;
    // There is currently only one allowed version.
    if (metadata.length < 1 || (metadata[0] & VERSION_MASK) != VERSION) {
      throw malformedVariant();
    }
  }

  public byte[] getValue() {
    if (pos == 0) return value;
    int size = valueSize(value, pos);
    checkIndex(pos + size - 1, value.length);
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
   * @return the string value
   */
  public String getString() {
    return VariantUtil.getString(value, pos);
  }

  /**
   * @return the type info bits from a variant value
   */
  public int getTypeInfo() {
    return VariantUtil.getTypeInfo(value, pos);
  }

  /**
   * @return the type of the variant value
   */
  public Type getType() {
    return VariantUtil.getType(value, pos);
  }

  /**
   * @return the number of object fields in the variant. `getType()` must be `Type.OBJECT`.
   */
  public int objectSize() {
    return handleObject(value, pos,
        (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> size);
  }

  // Find the field value whose key is equal to `key`. Return null if the key is not found.
  // It is only legal to call it when `getType()` is `Type.OBJECT`.

  /**
   * Returns the object field Variant value whose key is equal to `key`.
   * Return null if the key is not found. `getType()` must be `Type.OBJECT`.
   * @param key the key to look up
   * @return the field value whose key is equal to `key`, or null if key is not found
   */
  public Variant getFieldByKey(String key) {
    return handleObject(value, pos, (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
      // Use linear search for a short list. Switch to binary search when the length reaches
      // `BINARY_SEARCH_THRESHOLD`.
      final int BINARY_SEARCH_THRESHOLD = 32;
      if (size < BINARY_SEARCH_THRESHOLD) {
        for (int i = 0; i < size; ++i) {
          int id = readUnsigned(value, idStart + idSize * i, idSize);
          if (key.equals(getMetadataKey(metadata, id))) {
            int offset = readUnsigned(value, offsetStart + offsetSize * i, offsetSize);
            return new Variant(value, metadata, dataStart + offset);
          }
        }
      } else {
        int low = 0;
        int high = size - 1;
        while (low <= high) {
          // Use unsigned right shift to compute the middle of `low` and `high`. This is not only a
          // performance optimization, because it can properly handle the case where `low + high`
          // overflows int.
          int mid = (low + high) >>> 1;
          int id = readUnsigned(value, idStart + idSize * mid, idSize);
          int cmp = getMetadataKey(metadata, id).compareTo(key);
          if (cmp < 0) {
            low = mid + 1;
          } else if (cmp > 0) {
            high = mid - 1;
          } else {
            int offset = readUnsigned(value, offsetStart + offsetSize * mid, offsetSize);
            return new Variant(value, metadata, dataStart + offset);
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

  // Get the object field at the `index` slot. Return null if `index` is out of the bound of
  // `[0, objectSize())`.
  // It is only legal to call it when `getType()` is `Type.OBJECT`.
  /**
   * Returns the object field at the `index` slot. Return null if `index` is out of the bound of
   * `[0, objectSize())`. `getType()` must be `Type.OBJECT`.
   * @param index the index of the object field to get
   * @return the Objectfield at the `index` slot, or null if `index` is out of bounds
   */
  public ObjectField getFieldAtIndex(int index) {
    return handleObject(value, pos, (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
      if (index < 0 || index >= size) return null;
      int id = readUnsigned(value, idStart + idSize * index, idSize);
      int offset = readUnsigned(value, offsetStart + offsetSize * index, offsetSize);
      String key = getMetadataKey(metadata, id);
      Variant v = new Variant(value, metadata, dataStart + offset);
      return new ObjectField(key, v);
    });
  }

  /**
   * Returns the dictionary ID for the object field at the `index` slot.
   * `getType()` must be `Type.OBJECT`.
   * @param index the index of the object field to get the dictionary ID for
   * @return the dictionary ID for the object field at the `index` slot
   * @throws MalformedVariantException if `index` is out of bounds
   */
  public int getDictionaryIdAtIndex(int index) {
    return handleObject(value, pos, (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
      if (index < 0 || index >= size) {
        throw malformedVariant();
      }
      return readUnsigned(value, idStart + idSize * index, idSize);
    });
  }

  /**
   * @return the number of array elements. `getType()` must be `Type.ARRAY`.
   */
  public int arraySize() {
    return handleArray(value, pos, (size, offsetSize, offsetStart, dataStart) -> size);
  }

  /**
   * Returns the array element Variant value at the `index` slot. Returns null if `index` is
   * out of the bound of `[0, arraySize())`. `getType()` must be `Type.ARRAY`.
   * @param index the index of the array element to get
   * @return the array element Variant at the `index` slot, or null if `index` is out of bounds
   */
  public Variant getElementAtIndex(int index) {
    return handleArray(value, pos, (size, offsetSize, offsetStart, dataStart) -> {
      if (index < 0 || index >= size) return null;
      int offset = readUnsigned(value, offsetStart + offsetSize * index, offsetSize);
      return new Variant(value, metadata, dataStart + offset);
    });
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
    StringBuilder sb = new StringBuilder();
    toJsonImpl(value, metadata, pos, sb, zoneId, truncateTrailingZeros);
    return sb.toString();
  }

  /**
   * Escapes a string so that it can be pasted into a JSON structure. For example, if `str`
   * only contains a new-line character, then the result is "\n" (4 characters)
   * @param str the string to escape
   * @return the escaped string
   */
  private static String escapeJson(String str) {
    try (CharArrayWriter writer = new CharArrayWriter();
         JsonGenerator gen = new JsonFactory().createGenerator(writer)) {
      gen.writeString(str);
      gen.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // A simplified and more performant version of `sb.append(escapeJson(str))`. It is used when we
  // know `str` doesn't contain any special character that needs escaping.
  /**
   * Appends a quoted string to a StringBuilder. It is used when we know `str` doesn't contain any
   * special characters that needs escaping. This is more performant than
   * `sb.append(escapeJson(str))`.
   * @param sb the StringBuilder to append to
   * @param str the string to append
   */
  private static void appendQuoted(StringBuilder sb, String str) {
    sb.append('"');
    sb.append(str);
    sb.append('"');
  }

  /** The format for a timestamp without time zone. */
  private static final DateTimeFormatter TIMESTAMP_NTZ_FORMATTER = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral('T')
      .appendPattern("HH:mm:ss")
      .appendFraction(MICRO_OF_SECOND, 6, 6, true)
      .toFormatter(Locale.US);

  /** The format for a timestamp with time zone. */
  private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
      .append(TIMESTAMP_NTZ_FORMATTER)
      .appendOffset("+HH:MM", "+00:00")
      .toFormatter(Locale.US);

  /** The format for a timestamp without time zone, truncating trailing microsecond zeros. */
  private static final DateTimeFormatter TIMESTAMP_NTZ_TRUNC_FORMATTER =
      new DateTimeFormatterBuilder()
          .append(DateTimeFormatter.ISO_LOCAL_DATE)
          .appendLiteral('T')
          .appendPattern("HH:mm:ss")
          .optionalStart()
          .appendFraction(MICRO_OF_SECOND, 0, 6, true)
          .optionalEnd()
          .toFormatter(Locale.US);

  /** The format for a timestamp with time zone, truncating trailing microsecond zeros. */
  private static final DateTimeFormatter TIMESTAMP_TRUNC_FORMATTER = new DateTimeFormatterBuilder()
      .append(TIMESTAMP_NTZ_TRUNC_FORMATTER)
      .appendOffset("+HH:MM", "+00:00")
      .toFormatter(Locale.US);

  private static Instant microsToInstant(long microsSinceEpoch) {
    return Instant.EPOCH.plus(microsSinceEpoch, ChronoUnit.MICROS);
  }

  private static void toJsonImpl(byte[] value, byte[] metadata, int pos, StringBuilder sb,
                                 ZoneId zoneId, boolean truncateTrailingZeros) {
    switch (VariantUtil.getType(value, pos)) {
      case OBJECT:
        handleObject(value, pos, (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
          sb.append('{');
          for (int i = 0; i < size; ++i) {
            int id = readUnsigned(value, idStart + idSize * i, idSize);
            int offset = readUnsigned(value, offsetStart + offsetSize * i, offsetSize);
            int elementPos = dataStart + offset;
            if (i != 0) sb.append(',');
            sb.append(escapeJson(getMetadataKey(metadata, id)));
            sb.append(':');
            toJsonImpl(value, metadata, elementPos, sb, zoneId, truncateTrailingZeros);
          }
          sb.append('}');
          return null;
        });
        break;
      case ARRAY:
        handleArray(value, pos, (size, offsetSize, offsetStart, dataStart) -> {
          sb.append('[');
          for (int i = 0; i < size; ++i) {
            int offset = readUnsigned(value, offsetStart + offsetSize * i, offsetSize);
            int elementPos = dataStart + offset;
            if (i != 0) sb.append(',');
            toJsonImpl(value, metadata, elementPos, sb, zoneId, truncateTrailingZeros);
          }
          sb.append(']');
          return null;
        });
        break;
      case NULL:
        sb.append("null");
        break;
      case BOOLEAN:
        sb.append(VariantUtil.getBoolean(value, pos));
        break;
      case LONG:
        sb.append(VariantUtil.getLong(value, pos));
        break;
      case STRING:
        sb.append(escapeJson(VariantUtil.getString(value, pos)));
        break;
      case DOUBLE:
        sb.append(VariantUtil.getDouble(value, pos));
        break;
      case DECIMAL:
        if (truncateTrailingZeros) {
          sb.append(VariantUtil.getDecimal(value, pos).stripTrailingZeros().toPlainString());
        } else {
          sb.append(VariantUtil.getDecimal(value, pos).toPlainString());
        }
        break;
      case DATE:
        appendQuoted(sb, LocalDate.ofEpochDay((int) VariantUtil.getLong(value, pos)).toString());
        break;
      case TIMESTAMP:
        if (truncateTrailingZeros) {
          appendQuoted(sb, TIMESTAMP_TRUNC_FORMATTER.format(
              microsToInstant(VariantUtil.getLong(value, pos)).atZone(zoneId)));
        } else {
          appendQuoted(sb, TIMESTAMP_FORMATTER.format(
              microsToInstant(VariantUtil.getLong(value, pos)).atZone(zoneId)));
        }
        break;
      case TIMESTAMP_NTZ:
        if (truncateTrailingZeros) {
          appendQuoted(sb, TIMESTAMP_NTZ_TRUNC_FORMATTER.format(
              microsToInstant(VariantUtil.getLong(value, pos)).atZone(ZoneOffset.UTC)));
        } else {
          appendQuoted(sb, TIMESTAMP_NTZ_FORMATTER.format(
              microsToInstant(VariantUtil.getLong(value, pos)).atZone(ZoneOffset.UTC)));
        }
        break;
      case FLOAT:
        sb.append(VariantUtil.getFloat(value, pos));
        break;
      case BINARY:
        appendQuoted(sb, Base64.getEncoder().encodeToString(VariantUtil.getBinary(value, pos)));
        break;
    }
  }
}
