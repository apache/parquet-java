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
  /** The buffer that contains the Variant value. */
  final ByteBuffer value;

  /** The buffer that contains the Variant metadata. */
  final ByteBuffer metadata;

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
    this(
        ByteBuffer.wrap(value, valuePos, value.length - valuePos),
        ByteBuffer.wrap(metadata, metadataPos, metadata.length - metadataPos));
  }

  Variant(ByteBuffer value, ByteBuffer metadata) {
    this.value = value.asReadOnlyBuffer();
    this.value.mark();

    this.metadata = metadata.asReadOnlyBuffer();
    this.metadata.mark();

    // There is currently only one allowed version.
    if ((metadata.get(metadata.position()) & VariantUtil.VERSION_MASK) != VariantUtil.VERSION) {
      throw new UnsupportedOperationException(String.format(
          "Unsupported variant metadata version: %02X",
          metadata.get(metadata.position()) & VariantUtil.VERSION_MASK));
    }
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
    long longValue = VariantUtil.getLong(value);
    if (longValue < Byte.MIN_VALUE || longValue > Byte.MAX_VALUE) {
      throw new IllegalStateException("Value out of range for byte: " + longValue);
    }
    return (byte) longValue;
  }

  /**
   * @return the short value
   */
  public short getShort() {
    long longValue = VariantUtil.getLong(value);
    if (longValue < Short.MIN_VALUE || longValue > Short.MAX_VALUE) {
      throw new IllegalStateException("Value out of range for short: " + longValue);
    }
    return (short) longValue;
  }

  /**
   * @return the int value
   */
  public int getInt() {
    long longValue = VariantUtil.getLong(value);
    if (longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
      throw new IllegalStateException("Value out of range for int: " + longValue);
    }
    return (int) longValue;
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
  public byte[] getBinary() {
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
   * @return the type of the variant value
   */
  public VariantUtil.Type getType() {
    return VariantUtil.getType(value);
  }

  /**
   * @return the number of object fields in the variant. `getType()` must be `Type.OBJECT`.
   */
  public int numObjectElements() {
    return VariantUtil.getObjectInfo(value).numElements;
  }

  /**
   * Returns the object field Variant value whose key is equal to `key`.
   * Return null if the key is not found. `getType()` must be `Type.OBJECT`.
   * @param key the key to look up
   * @return the field value whose key is equal to `key`, or null if key is not found
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

  private static ObjectField getFieldAtIndex(
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
   * @return the number of array elements. `getType()` must be `Type.ARRAY`.
   */
  public int numArrayElements() {
    return VariantUtil.getArrayInfo(value).numElements;
  }

  /**
   * Returns the array element Variant value at the `index` slot. Returns null if `index` is
   * out of the bound of `[0, arraySize())`. `getType()` must be `Type.ARRAY`.
   * @param index the index of the array element to get
   * @return the array element Variant at the `index` slot, or null if `index` is out of bounds
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
