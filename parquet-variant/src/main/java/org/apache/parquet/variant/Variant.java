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
   * An interface to write Variant scalar values to a JSON generator.
   */
  public interface ScalarToJson {
    void writeNull(JsonGenerator gen) throws IOException;

    void writeBoolean(JsonGenerator gen, boolean value) throws IOException;

    void writeByte(JsonGenerator gen, byte value) throws IOException;

    void writeShort(JsonGenerator gen, short value) throws IOException;

    void writeInt(JsonGenerator gen, int value) throws IOException;

    void writeLong(JsonGenerator gen, long value) throws IOException;

    void writeFloat(JsonGenerator gen, float value) throws IOException;

    void writeDouble(JsonGenerator gen, double value) throws IOException;

    void writeString(JsonGenerator gen, String value) throws IOException;

    void writeBinary(JsonGenerator gen, byte[] value) throws IOException;

    void writeDecimal(JsonGenerator gen, BigDecimal value) throws IOException;

    void writeUUID(JsonGenerator gen, UUID value) throws IOException;

    void writeDate(JsonGenerator gen, int value) throws IOException;

    void writeTime(JsonGenerator gen, long microsSinceMidnight) throws IOException;

    void writeTimestamp(JsonGenerator gen, long microsSinceEpoch) throws IOException;

    void writeTimestampNtz(JsonGenerator gen, long microsSinceEpoch) throws IOException;

    void writeTimestampNanos(JsonGenerator gen, long nanosSinceEpoch) throws IOException;

    void writeTimestampNanosNtz(JsonGenerator gen, long nanosSinceEpoch) throws IOException;
  }

  /**
   * @return the JSON representation of the variant
   * @throws MalformedVariantException if the variant is malformed
   */
  public String toJson() {
    return toJson(new DefaultScalarToJson());
  }

  /**
   * @param scalarWriter the writer to use for writing scalar values
   * @return the JSON representation of the variant
   * @throws MalformedVariantException if the variant is malformed
   */
  public String toJson(ScalarToJson scalarWriter) {
    try (CharArrayWriter writer = new CharArrayWriter();
        JsonGenerator gen = new JsonFactory().createGenerator(writer)) {
      toJsonImpl(value, valuePos, metadata, metadataPos, gen, scalarWriter);
      gen.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeIOException("Failed to convert variant to json", e);
    }
  }

  private static void toJsonImpl(
      byte[] value, int valuePos, byte[] metadata, int metadataPos, JsonGenerator gen, ScalarToJson scalarWriter)
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
              scalarWriter);
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
          toJsonImpl(v.value, v.valuePos, v.metadata, metadataPos, gen, scalarWriter);
        }
        gen.writeEndArray();
        break;
      }
      case NULL:
        scalarWriter.writeNull(gen);
        break;
      case BOOLEAN:
        scalarWriter.writeBoolean(gen, VariantUtil.getBoolean(value, valuePos));
        break;
      case BYTE:
        scalarWriter.writeByte(gen, (byte) VariantUtil.getLong(value, valuePos));
        break;
      case SHORT:
        scalarWriter.writeShort(gen, (short) VariantUtil.getLong(value, valuePos));
        break;
      case INT:
        scalarWriter.writeInt(gen, (int) VariantUtil.getLong(value, valuePos));
        break;
      case LONG:
        scalarWriter.writeLong(gen, VariantUtil.getLong(value, valuePos));
        break;
      case STRING:
        scalarWriter.writeString(gen, VariantUtil.getString(value, valuePos));
        break;
      case BINARY:
        scalarWriter.writeBinary(gen, VariantUtil.getBinary(value, valuePos));
        break;
      case FLOAT:
        scalarWriter.writeFloat(gen, VariantUtil.getFloat(value, valuePos));
        break;
      case DOUBLE:
        scalarWriter.writeDouble(gen, VariantUtil.getDouble(value, valuePos));
        break;
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        scalarWriter.writeDecimal(gen, VariantUtil.getDecimal(value, valuePos));
        break;
      case DATE:
        scalarWriter.writeDate(gen, (int) VariantUtil.getLong(value, valuePos));
        break;
      case TIMESTAMP:
        scalarWriter.writeTimestamp(gen, VariantUtil.getLong(value, valuePos));
        break;
      case TIMESTAMP_NTZ:
        scalarWriter.writeTimestampNtz(gen, VariantUtil.getLong(value, valuePos));
        break;
      case TIME:
        scalarWriter.writeTime(gen, VariantUtil.getLong(value, valuePos));
        break;
      case TIMESTAMP_NANOS:
        scalarWriter.writeTimestampNanos(gen, VariantUtil.getLong(value, valuePos));
        break;
      case TIMESTAMP_NANOS_NTZ:
        scalarWriter.writeTimestampNanosNtz(gen, VariantUtil.getLong(value, valuePos));
        break;
      case UUID:
        scalarWriter.writeUUID(gen, VariantUtil.getUUID(value, valuePos));
        break;
      default:
        throw new IllegalArgumentException("Unsupported type: " + VariantUtil.getType(value, valuePos));
    }
  }
}
