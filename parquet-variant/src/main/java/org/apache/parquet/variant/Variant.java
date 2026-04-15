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
   * Pre-computed metadata dictionary size
   */
  private final int dictSize;

  /**
   * Lazy cache for metadata dictionary strings.
   */
  private volatile String[] metadataCache;

  /**
   * Lazy cache for the parsed object header.
   */
  private volatile VariantUtil.ObjectInfo cachedObjectInfo;

  /**
   * Lazy cache for the parsed array header.
   */
  private volatile VariantUtil.ArrayInfo cachedArrayInfo;

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

    // Pre-compute dictionary size for lazy metadata cache allocation.
    int pos = this.metadata.position();
    int metaOffsetSize = ((this.metadata.get(pos) >> 6) & 0x3) + 1;
    if (this.metadata.remaining() > 1) {
      this.dictSize = VariantUtil.readUnsigned(this.metadata, pos + 1, metaOffsetSize);
    } else {
      this.dictSize = 0;
    }
    this.metadataCache = null;
  }

  /**
   * Package-private constructor that shares pre-parsed metadata state from a parent Variant.
   */
  Variant(ByteBuffer value, ByteBuffer metadata, String[] metadataCache, int dictSize) {
    this.value = value.asReadOnlyBuffer();
    this.metadata = metadata.asReadOnlyBuffer();
    this.metadataCache = metadataCache;
    this.dictSize = dictSize;
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
    return objectInfo().numElements;
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
    VariantUtil.ObjectInfo info = objectInfo();
    int idStart = value.position() + info.idStartOffset;
    int offsetStart = value.position() + info.offsetStartOffset;
    int dataStart = value.position() + info.dataStartOffset;

    if (info.numElements < BINARY_SEARCH_THRESHOLD) {
      for (int i = 0; i < info.numElements; ++i) {
        int id = VariantUtil.readUnsigned(value, idStart + info.idSize * i, info.idSize);
        String fieldKey = getMetadataKeyCached(id);
        if (fieldKey.equals(key)) {
          int offset = VariantUtil.readUnsigned(value, offsetStart + info.offsetSize * i, info.offsetSize);
          return childVariant(VariantUtil.slice(value, dataStart + offset));
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
        int midId = VariantUtil.readUnsigned(value, idStart + info.idSize * mid, info.idSize);
        String midKey = getMetadataKeyCached(midId);
        int cmp = midKey.compareTo(key);
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          int offset = VariantUtil.readUnsigned(value, offsetStart + info.offsetSize * mid, info.offsetSize);
          return childVariant(VariantUtil.slice(value, dataStart + offset));
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
    VariantUtil.ObjectInfo info = objectInfo();
    int idStart = value.position() + info.idStartOffset;
    int offsetStart = value.position() + info.offsetStartOffset;
    int dataStart = value.position() + info.dataStartOffset;
    int id = VariantUtil.readUnsigned(value, idStart + info.idSize * idx, info.idSize);
    int offset = VariantUtil.readUnsigned(value, offsetStart + info.offsetSize * idx, info.offsetSize);
    String key = getMetadataKeyCached(id);
    Variant v = childVariant(VariantUtil.slice(value, dataStart + offset));
    return new ObjectField(key, v);
  }

  /**
   * @return the number of array elements
   * @throws IllegalArgumentException if `getType()` does not return `Type.ARRAY`
   */
  public int numArrayElements() {
    return arrayInfo().numElements;
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
    VariantUtil.ArrayInfo info = arrayInfo();
    if (index < 0 || index >= info.numElements) {
      return null;
    }
    int offsetStart = value.position() + info.offsetStartOffset;
    int dataStart = value.position() + info.dataStartOffset;
    int offset = VariantUtil.readUnsigned(value, offsetStart + info.offsetSize * index, info.offsetSize);
    return childVariant(VariantUtil.slice(value, dataStart + offset));
  }

  /**
   * Creates a child Variant that shares this instance's metadata cache.
   */
  private Variant childVariant(ByteBuffer childValue) {
    return new Variant(childValue, metadata, metadataCache, dictSize);
  }

  /**
   * Returns the metadata dictionary string for the given ID, caching the result.
   */
  String getMetadataKeyCached(int id) {
    // Fall back to uncached lookup for out-of-range IDs
    if (id < 0 || id >= dictSize) {
      return VariantUtil.getMetadataKey(metadata, id);
    }
    // Demand-create shared dictionary cache
    String[] cache = metadataCache;
    if (cache == null) {
      cache = new String[dictSize];
      metadataCache = cache;
    }
    if (cache[id] == null) {
      cache[id] = VariantUtil.getMetadataKey(metadata, id);
    }
    return cache[id];
  }

  /**
   * Returns the cached object header, parsing it on first access.
   */
  private VariantUtil.ObjectInfo objectInfo() {
    VariantUtil.ObjectInfo info = cachedObjectInfo;
    if (info == null) {
      info = VariantUtil.getObjectInfo(value);
      cachedObjectInfo = info;
    }
    return info;
  }

  /**
   * Returns the cached array header, parsing it on first access.
   */
  private VariantUtil.ArrayInfo arrayInfo() {
    VariantUtil.ArrayInfo info = cachedArrayInfo;
    if (info == null) {
      info = VariantUtil.getArrayInfo(value);
      cachedArrayInfo = info;
    }
    return info;
  }
}
