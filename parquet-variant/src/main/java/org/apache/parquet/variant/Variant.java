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
import java.util.UUID;

/**
 * This Variant class holds the Variant-encoded value and metadata binary values.
 */
public final class Variant {
  /**
   * The buffer that contains the Variant value.
   */
  final ByteBuffer value;

  /**
   * The buffer that contains the Variant metadata.
   */
  final ByteBuffer metadata;

  /**
   * The threshold to switch from linear search to binary search when looking up a field by key in
   * an object. This is a performance optimization to avoid the overhead of binary search for a
   * short list.
   */
  static final int BINARY_SEARCH_THRESHOLD = 32;

  public Variant(byte[] value, byte[] metadata) {
    this(value, 0, value.length, metadata, 0, metadata.length);
  }

  public Variant(byte[] value, int valuePos, int valueLength, byte[] metadata, int metadataPos, int metadataLength) {
    this(ByteBuffer.wrap(value, valuePos, valueLength), ByteBuffer.wrap(metadata, metadataPos, metadataLength));
  }

  Variant(ByteBuffer value, Metadata metadata) {
    this(value, metadata.getEncodedBuffer());
  }

  public Variant(ByteBuffer value, ByteBuffer metadata) {
    // The buffers are read a single-byte at a time, so the endianness of the input buffers
    // is not important.
    this.value = value.asReadOnlyBuffer();
    this.metadata = metadata.asReadOnlyBuffer();

    // There is currently only one allowed version.
    if ((metadata.get(metadata.position()) & VariantUtil.VERSION_MASK) != VariantUtil.VERSION) {
      throw new UnsupportedOperationException(String.format(
          "Unsupported variant metadata version: %d",
          metadata.get(metadata.position()) & VariantUtil.VERSION_MASK));
    }
  }

  public ByteBuffer getValueBuffer() {
    return value;
  }

  public ByteBuffer getMetadataBuffer() {
    return metadata;
  }

  /**
   * @return the boolean value
   */
  public boolean getBoolean() {
    return VariantUtil.getBoolean(value);
  }

  /**
   * @return the byte value
   */
  public byte getByte() {
    return VariantUtil.getByte(value);
  }

  /**
   * @return the short value
   */
  public short getShort() {
    return VariantUtil.getShort(value);
  }

  /**
   * @return the int value
   */
  public int getInt() {
    return VariantUtil.getInt(value);
  }

  /**
   * @return the long value
   */
  public long getLong() {
    return VariantUtil.getLong(value);
  }

  /**
   * @return the double value
   */
  public double getDouble() {
    return VariantUtil.getDouble(value);
  }

  /**
   * @return the decimal value
   */
  public BigDecimal getDecimal() {
    return VariantUtil.getDecimal(value);
  }

  /**
   * @return the float value
   */
  public float getFloat() {
    return VariantUtil.getFloat(value);
  }

  /**
   * @return the binary value
   */
  public ByteBuffer getBinary() {
    return VariantUtil.getBinary(value);
  }

  /**
   * @return the UUID value
   */
  public UUID getUUID() {
    return VariantUtil.getUUID(value);
  }

  /**
   * @return the string value
   */
  public String getString() {
    return VariantUtil.getString(value);
  }

  /**
   * The value type of Variant value. It is determined by the header byte.
   */
  public enum Type {
    OBJECT,
    ARRAY,
    NULL,
    BOOLEAN,
    BYTE,
    SHORT,
    INT,
    LONG,
    STRING,
    DOUBLE,
    DECIMAL4,
    DECIMAL8,
    DECIMAL16,
    DATE,
    TIMESTAMP_TZ,
    TIMESTAMP_NTZ,
    FLOAT,
    BINARY,
    TIME,
    TIMESTAMP_NANOS_TZ,
    TIMESTAMP_NANOS_NTZ,
    UUID
  }

  /**
   * @return the type of the variant value
   */
  public Type getType() {
    return VariantUtil.getType(value);
  }

  /**
   * @return the number of object fields in the variant
   * @throws IllegalArgumentException if `getType()` does not return `Type.OBJECT`
   */
  public int numObjectElements() {
    return VariantUtil.getObjectInfo(value).numElements;
  }

  /**
   * Returns the object field Variant value whose key is equal to `key`.
   * Returns null if the key is not found.
   *
   * @param key the key to look up
   * @return the field value whose key is equal to `key`, or null if key is not found
   * @throws IllegalArgumentException if `getType()` does not return `Type.OBJECT`
   */
  public Variant getFieldByKey(String key) {
    VariantUtil.ObjectInfo info = VariantUtil.getObjectInfo(value);
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
            value.position() + info.idStartOffset,
            value.position() + info.offsetStartOffset,
            value.position() + info.dataStartOffset);
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
            value.position() + info.idStartOffset,
            value.position() + info.offsetStartOffset,
            value.position() + info.dataStartOffset);
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
   * Returns the field at index idx, lexicographically ordered.
   *
   * @param idx the index to look up
   * @return the field value whose key is equal to `key`, or null if key is not found
   * @throws IllegalArgumentException if `getType()` does not return `Type.OBJECT`
   */
  public ObjectField getFieldAtIndex(int idx) {
    VariantUtil.ObjectInfo info = VariantUtil.getObjectInfo(value);
    // Use linear search for a short list. Switch to binary search when the length reaches
    // `BINARY_SEARCH_THRESHOLD`.
    ObjectField field = getFieldAtIndex(
        idx,
        value,
        metadata,
        info.idSize,
        info.offsetSize,
        value.position() + info.idStartOffset,
        value.position() + info.offsetStartOffset,
        value.position() + info.dataStartOffset);
    return field;
  }

  static ObjectField getFieldAtIndex(
      int index,
      ByteBuffer value,
      ByteBuffer metadata,
      int idSize,
      int offsetSize,
      int idStart,
      int offsetStart,
      int dataStart) {
    // idStart, offsetStart, and dataStart are absolute positions in the `value` buffer.
    int id = VariantUtil.readUnsigned(value, idStart + idSize * index, idSize);
    int offset = VariantUtil.readUnsigned(value, offsetStart + offsetSize * index, offsetSize);
    String key = VariantUtil.getMetadataKey(metadata, id);
    Variant v = new Variant(VariantUtil.slice(value, dataStart + offset), metadata);
    return new ObjectField(key, v);
  }

  /**
   * @return the number of array elements
   * @throws IllegalArgumentException if `getType()` does not return `Type.ARRAY`
   */
  public int numArrayElements() {
    return VariantUtil.getArrayInfo(value).numElements;
  }

  /**
   * Returns the array element Variant value at the `index` slot. Returns null if `index` is
   * out of the bound of `[0, arraySize())`.
   *
   * @param index the index of the array element to get
   * @return the array element Variant at the `index` slot, or null if `index` is out of bounds
   * @throws IllegalArgumentException if `getType()` does not return `Type.ARRAY`
   */
  public Variant getElementAtIndex(int index) {
    VariantUtil.ArrayInfo info = VariantUtil.getArrayInfo(value);
    if (index < 0 || index >= info.numElements) {
      return null;
    }
    return getElementAtIndex(
        index,
        value,
        metadata,
        info.offsetSize,
        value.position() + info.offsetStartOffset,
        value.position() + info.dataStartOffset);
  }

  private static Variant getElementAtIndex(
      int index, ByteBuffer value, ByteBuffer metadata, int offsetSize, int offsetStart, int dataStart) {
    // offsetStart and dataStart are absolute positions in the `value` buffer.
    int offset = VariantUtil.readUnsigned(value, offsetStart + offsetSize * index, offsetSize);
    return new Variant(VariantUtil.slice(value, dataStart + offset), metadata);
  }
}
