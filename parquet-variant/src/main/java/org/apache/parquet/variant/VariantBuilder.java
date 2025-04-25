/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.variant;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

/**
 * Builder for creating Variant value and metadata.
 */
public class VariantBuilder {
  /**
   * Creates a VariantBuilder.
   * @param allowDuplicateKeys if true, only the last occurrence of a duplicate key will be kept.
   *                           Otherwise, an exception will be thrown.
   */
  public VariantBuilder(boolean allowDuplicateKeys) {
    this.allowDuplicateKeys = allowDuplicateKeys;
  }

  /**
   * @return the Variant value
   */
  public Variant build() {
    int numKeys = dictionaryKeys.size();
    // Use long to avoid overflow in accumulating lengths.
    long dictionaryStringSize = 0;
    for (byte[] key : dictionaryKeys) {
      dictionaryStringSize += key.length;
    }
    // Determine the number of bytes required per offset entry.
    // The largest offset is the one-past-the-end value, which is total string size. It's very
    // unlikely that the number of keys could be larger, but incorporate that into the calculation
    // in case of pathological data.
    long maxSize = Math.max(dictionaryStringSize, numKeys);
    int offsetSize = getMinIntegerSize((int) maxSize);

    int offsetStart = 1 + offsetSize;
    int stringStart = offsetStart + (numKeys + 1) * offsetSize;
    long metadataSize = stringStart + dictionaryStringSize;

    byte[] metadata = new byte[(int) metadataSize];
    int headerByte = VariantUtil.VERSION | ((offsetSize - 1) << 6);
    VariantUtil.writeLong(metadata, 0, headerByte, 1);
    VariantUtil.writeLong(metadata, 1, numKeys, offsetSize);
    int currentOffset = 0;
    for (int i = 0; i < numKeys; ++i) {
      VariantUtil.writeLong(metadata, offsetStart + i * offsetSize, currentOffset, offsetSize);
      byte[] key = dictionaryKeys.get(i);
      System.arraycopy(key, 0, metadata, stringStart + currentOffset, key.length);
      currentOffset += key.length;
    }
    VariantUtil.writeLong(metadata, offsetStart + numKeys * offsetSize, currentOffset, offsetSize);
    return new Variant(Arrays.copyOfRange(writeBuffer, 0, writePos), metadata);
  }

  /**
   * Appends a string value to the Variant builder.
   * @param str the string value to append
   * @return this builder
   */
  public VariantBuilder appendString(String str) {
    byte[] text = str.getBytes(StandardCharsets.UTF_8);
    boolean longStr = text.length > VariantUtil.MAX_SHORT_STR_SIZE;
    checkCapacity((longStr ? 1 + VariantUtil.U32_SIZE : 1) + text.length);
    if (longStr) {
      writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.LONG_STR);
      VariantUtil.writeLong(writeBuffer, writePos, text.length, VariantUtil.U32_SIZE);
      writePos += VariantUtil.U32_SIZE;
    } else {
      writeBuffer[writePos++] = VariantUtil.shortStrHeader(text.length);
    }
    System.arraycopy(text, 0, writeBuffer, writePos, text.length);
    writePos += text.length;
    return this;
  }

  /**
   * Appends a null value to the Variant builder.
   * @return this builder
   */
  public VariantBuilder appendNull() {
    checkCapacity(1);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.NULL);
    return this;
  }

  /**
   * Appends a boolean value to the Variant builder.
   * @param b the boolean value to append
   * @return this builder
   */
  public VariantBuilder appendBoolean(boolean b) {
    checkCapacity(1);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(b ? VariantUtil.TRUE : VariantUtil.FALSE);
    return this;
  }

  /**
   * Appends a long value to the variant builder. The actual encoded integer type depends on the
   * value range of the long value.
   * @param l the long value to append
   * @return this builder
   */
  public VariantBuilder appendLong(long l) {
    if (l == (byte) l) {
      checkCapacity(1 + 1);
      writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.INT8);
      VariantUtil.writeLong(writeBuffer, writePos, l, 1);
      writePos += 1;
    } else if (l == (short) l) {
      checkCapacity(1 + 2);
      writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.INT16);
      VariantUtil.writeLong(writeBuffer, writePos, l, 2);
      writePos += 2;
    } else if (l == (int) l) {
      checkCapacity(1 + 4);
      writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.INT32);
      VariantUtil.writeLong(writeBuffer, writePos, l, 4);
      writePos += 4;
    } else {
      checkCapacity(1 + 8);
      writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.INT64);
      VariantUtil.writeLong(writeBuffer, writePos, l, 8);
      writePos += 8;
    }
    return this;
  }

  /**
   * Appends a double value to the variant builder.
   * @param d the double to append
   * @return this builder
   */
  public VariantBuilder appendDouble(double d) {
    checkCapacity(1 + 8);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.DOUBLE);
    VariantUtil.writeLong(writeBuffer, writePos, Double.doubleToLongBits(d), 8);
    writePos += 8;
    return this;
  }

  /**
   * Appends a decimal value to the variant builder. The actual encoded decimal type depends on the
   * precision and scale of the decimal value.
   * @param d the decimal value to append
   * @return this builder
   */
  public VariantBuilder appendDecimal(BigDecimal d) {
    BigInteger unscaled = d.unscaledValue();
    if (d.scale() <= VariantUtil.MAX_DECIMAL4_PRECISION && d.precision() <= VariantUtil.MAX_DECIMAL4_PRECISION) {
      checkCapacity(2 + 4);
      writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.DECIMAL4);
      writeBuffer[writePos++] = (byte) d.scale();
      VariantUtil.writeLong(writeBuffer, writePos, unscaled.intValueExact(), 4);
      writePos += 4;
    } else if (d.scale() <= VariantUtil.MAX_DECIMAL8_PRECISION
        && d.precision() <= VariantUtil.MAX_DECIMAL8_PRECISION) {
      checkCapacity(2 + 8);
      writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.DECIMAL8);
      writeBuffer[writePos++] = (byte) d.scale();
      VariantUtil.writeLong(writeBuffer, writePos, unscaled.longValueExact(), 8);
      writePos += 8;
    } else {
      assert d.scale() <= VariantUtil.MAX_DECIMAL16_PRECISION
          && d.precision() <= VariantUtil.MAX_DECIMAL16_PRECISION;
      checkCapacity(2 + 16);
      writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.DECIMAL16);
      writeBuffer[writePos++] = (byte) d.scale();
      // `toByteArray` returns a big-endian representation. We need to copy it reversely and sign
      // extend it to 16 bytes.
      byte[] bytes = unscaled.toByteArray();
      for (int i = 0; i < bytes.length; ++i) {
        writeBuffer[writePos + i] = bytes[bytes.length - 1 - i];
      }
      byte sign = (byte) (bytes[0] < 0 ? -1 : 0);
      for (int i = bytes.length; i < 16; ++i) {
        writeBuffer[writePos + i] = sign;
      }
      writePos += 16;
    }
    return this;
  }

  /**
   * Appends a date value to the variant builder. The date is represented as the number of days
   * since the epoch.
   * @param daysSinceEpoch the number of days since the epoch
   * @return this builder
   */
  public VariantBuilder appendDate(int daysSinceEpoch) {
    checkCapacity(1 + 4);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.DATE);
    VariantUtil.writeLong(writeBuffer, writePos, daysSinceEpoch, 4);
    writePos += 4;
    return this;
  }

  /**
   * Appends a TimestampTz value to the variant builder. The timestamp is represented as the number
   * of microseconds since the epoch.
   * @param microsSinceEpoch the number of microseconds since the epoch
   * @return this builder
   */
  public VariantBuilder appendTimestampTz(long microsSinceEpoch) {
    checkCapacity(1 + 8);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.TIMESTAMP_TZ);
    VariantUtil.writeLong(writeBuffer, writePos, microsSinceEpoch, 8);
    writePos += 8;
    return this;
  }

  /**
   * Appends a TimestampNtz value to the variant builder. The timestamp is represented as the number
   * of microseconds since the epoch.
   * @param microsSinceEpoch the number of microseconds since the epoch
   * @return this builder
   */
  public VariantBuilder appendTimestampNtz(long microsSinceEpoch) {
    checkCapacity(1 + 8);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.TIMESTAMP_NTZ);
    VariantUtil.writeLong(writeBuffer, writePos, microsSinceEpoch, 8);
    writePos += 8;
    return this;
  }

  /**
   * Appends a Time value to the variant builder. The time is represented as the number of
   * microseconds since midnight.
   * @param microsSinceMidnight the number of microseconds since midnight
   * @return this builder
   */
  public VariantBuilder appendTime(long microsSinceMidnight) {
    checkCapacity(1 + 8);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.TIME);
    VariantUtil.writeLong(writeBuffer, writePos, microsSinceMidnight, 8);
    writePos += 8;
    return this;
  }

  /**
   * Appends a TimestampNanosTz value to the variant builder. The timestamp is represented as the
   * number of nanoseconds since the epoch.
   * @param nanosSinceEpoch the number of nanoseconds since the epoch
   * @return this builder
   */
  public VariantBuilder appendTimestampNanosTz(long nanosSinceEpoch) {
    checkCapacity(1 + 8);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.TIMESTAMP_NANOS_TZ);
    VariantUtil.writeLong(writeBuffer, writePos, nanosSinceEpoch, 8);
    writePos += 8;
    return this;
  }

  /**
   * Appends a TimestampNanosNtz value to the variant builder. The timestamp is represented as the
   * number of nanoseconds since the epoch.
   * @param nanosSinceEpoch the number of nanoseconds since the epoch
   * @return this builder
   */
  public VariantBuilder appendTimestampNanosNtz(long nanosSinceEpoch) {
    checkCapacity(1 + 8);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.TIMESTAMP_NANOS_NTZ);
    VariantUtil.writeLong(writeBuffer, writePos, nanosSinceEpoch, 8);
    writePos += 8;
    return this;
  }

  /**
   * Appends a float value to the variant builder.
   * @param f the float to append
   * @return this builder
   */
  public VariantBuilder appendFloat(float f) {
    checkCapacity(1 + 4);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.FLOAT);
    VariantUtil.writeLong(writeBuffer, writePos, Float.floatToIntBits(f), 8);
    writePos += 4;
    return this;
  }

  /**
   * Appends a byte array to the variant builder.
   * @param binary the byte array to append
   * @return this builder
   */
  public VariantBuilder appendBinary(byte[] binary) {
    checkCapacity(1 + VariantUtil.U32_SIZE + binary.length);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.BINARY);
    VariantUtil.writeLong(writeBuffer, writePos, binary.length, VariantUtil.U32_SIZE);
    writePos += VariantUtil.U32_SIZE;
    System.arraycopy(binary, 0, writeBuffer, writePos, binary.length);
    writePos += binary.length;
    return this;
  }

  /**
   * Appends a UUID value to the variant builder.
   * @param uuid the UUID to append
   * @return this builder
   */
  public VariantBuilder appendUUID(java.util.UUID uuid) {
    checkCapacity(1 + VariantUtil.UUID_SIZE);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.UUID);

    ByteBuffer bb =
        ByteBuffer.wrap(writeBuffer, writePos, VariantUtil.UUID_SIZE).order(ByteOrder.BIG_ENDIAN);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    writePos += VariantUtil.UUID_SIZE;
    return this;
  }

  /**
   * Starts appending an object to this variant builder. The returned VariantObjectBuilder must
   * be used for future calls to addObjectKey() and endObject(). To add a (key, value) to the
   * Variant object, call appendObjectKey() to add the key name, and then append the value.
   * endObject() must be called to finish writing the Variant object.
   *
   * Example usage:
   * VariantBuilder builder = new VariantBuilder();
   * VariantObjectBuilder objBuilder = builder.startObject();
   * builder.appendObjectKey(objBuilder, "key1");
   * builder.appendString("value1");
   * builder.endObject(objBuilder);
   *
   * @return a VariantObjectBuilder to use for appendObjectKey() and endObject().
   */
  public VariantObjectBuilder startObject() {
    return new VariantObjectBuilder(this);
  }

  /**
   * Appends a key to the Variant object. This method must be called before appending the
   * corresponding value.
   * @param objBuilder the VariantObjectBuilder to use
   */
  public void appendObjectKey(VariantObjectBuilder objBuilder, String key) {
    objBuilder.appendKey(key);
  }

  /**
   * Ends appending an object to this variant builder. This method must be called after all keys and
   * values have been added to the object.
   * @param objBuilder the VariantObjectBuilder to use
   * @return this builder
   */
  public VariantBuilder endObject(VariantObjectBuilder objBuilder) {
    ArrayList<FieldEntry> fields = objBuilder.fields();
    int size = fields.size();
    Collections.sort(fields);
    int maxId = size == 0 ? 0 : fields.get(0).id;
    if (allowDuplicateKeys) {
      int distinctPos = 0;
      // Maintain a list of distinct keys in-place.
      for (int i = 1; i < size; ++i) {
        maxId = Math.max(maxId, fields.get(i).id);
        if (fields.get(i).id == fields.get(i - 1).id) {
          // Found a duplicate key. Keep the field with the greater offset.
          if (fields.get(distinctPos).offset < fields.get(i).offset) {
            fields.set(distinctPos, fields.get(distinctPos).withNewOffset(fields.get(i).offset));
          }
        } else {
          // Found a distinct key. Add the field to the list.
          ++distinctPos;
          fields.set(distinctPos, fields.get(i));
        }
      }
      if (distinctPos + 1 < fields.size()) {
        size = distinctPos + 1;
        // Resize `fields` to `size`.
        fields.subList(size, fields.size()).clear();
        // Sort the fields by offsets so that we can move the value data of each field to the new
        // offset without overwriting the fields after it.
        fields.sort(Comparator.comparingInt(f -> f.offset));
        int currentOffset = 0;
        for (int i = 0; i < size; ++i) {
          int oldOffset = fields.get(i).offset;
          int fieldSize =
              VariantUtil.valueSize(ByteBuffer.wrap(writeBuffer), objBuilder.startPos() + oldOffset);
          System.arraycopy(
              writeBuffer,
              objBuilder.startPos() + oldOffset,
              writeBuffer,
              objBuilder.startPos() + currentOffset,
              fieldSize);
          fields.set(i, fields.get(i).withNewOffset(currentOffset));
          currentOffset += fieldSize;
        }
        writePos = objBuilder.startPos() + currentOffset;
        // Change back to the sort order by field keys, required by the Variant specification.
        Collections.sort(fields);
      }
    } else {
      for (int i = 1; i < size; ++i) {
        maxId = Math.max(maxId, fields.get(i).id);
        String key = fields.get(i).key;
        if (key.equals(fields.get(i - 1).key)) {
          throw new IllegalStateException("Failed to build Variant because of duplicate object key: " + key);
        }
      }
    }
    int dataSize = writePos - objBuilder.startPos();
    boolean largeSize = size > VariantUtil.U8_MAX;
    int sizeBytes = largeSize ? VariantUtil.U32_SIZE : 1;
    int idSize = getMinIntegerSize(maxId);
    int offsetSize = getMinIntegerSize(dataSize);
    // The space for header byte, object size, id list, and offset list.
    int headerSize = 1 + sizeBytes + size * idSize + (size + 1) * offsetSize;
    checkCapacity(headerSize);
    // Shift the just-written field data to make room for the object header section.
    System.arraycopy(writeBuffer, objBuilder.startPos(), writeBuffer, objBuilder.startPos() + headerSize, dataSize);
    writePos += headerSize;
    writeBuffer[objBuilder.startPos()] = VariantUtil.objectHeader(largeSize, idSize, offsetSize);
    VariantUtil.writeLong(writeBuffer, objBuilder.startPos() + 1, size, sizeBytes);
    int idStart = objBuilder.startPos() + 1 + sizeBytes;
    int offsetStart = idStart + size * idSize;
    for (int i = 0; i < size; ++i) {
      VariantUtil.writeLong(writeBuffer, idStart + i * idSize, fields.get(i).id, idSize);
      VariantUtil.writeLong(writeBuffer, offsetStart + i * offsetSize, fields.get(i).offset, offsetSize);
    }
    VariantUtil.writeLong(writeBuffer, offsetStart + size * offsetSize, dataSize, offsetSize);
    return this;
  }

  /**
   * Starts appending an array to this variant builder. The returned VariantArrayBuilder must be
   * used for future calls to startArrayElement() and endArray(). To add an element to the array,
   * call startArrayElement() and then append the value. endArray() must be called to finish writing
   * the Variant array.
   *
   * Example usage:
   * VariantBuilder builder = new VariantBuilder();
   * VariantArrayBuilder arrayBuilder = builder.startArray();
   * arrayBuilder.startArrayElement(arrayBuilder);
   * builder.appendString("value1");
   * arrayBuilder.endArray(arrayBuilder);
   *
   * @return a VariantArrayBuilder to use for startArrayElement() and endArray().
   */
  public VariantArrayBuilder startArray() {
    return new VariantArrayBuilder(this);
  }

  /**
   * Starts appending an element to the array. This method must be called before appending the
   * corresponding value.
   * @param arrayBuilder the VariantArrayBuilder to use
   */
  public void startArrayElement(VariantArrayBuilder arrayBuilder) {
    arrayBuilder.startElement();
  }

  /**
   * Ends appending an array to this variant builder. This method must be called after all elements
   * have been added to the array.
   * @param arrayBuilder the VariantArrayBuilder to use
   * @return this builder
   */
  public VariantBuilder endArray(VariantArrayBuilder arrayBuilder) {
    int start = arrayBuilder.startPos();
    int dataSize = writePos - start;
    int size = arrayBuilder.offsets().size();
    boolean largeSize = size > VariantUtil.U8_MAX;
    int sizeBytes = largeSize ? VariantUtil.U32_SIZE : 1;
    int offsetSize = getMinIntegerSize(dataSize);
    // The space for header byte, object size, and offset list.
    int headerSize = 1 + sizeBytes + (size + 1) * offsetSize;
    checkCapacity(headerSize);
    // Shift the just-written field data to make room for the header section.
    System.arraycopy(writeBuffer, start, writeBuffer, start + headerSize, dataSize);
    writePos += headerSize;
    writeBuffer[start] = VariantUtil.arrayHeader(largeSize, offsetSize);
    VariantUtil.writeLong(writeBuffer, start + 1, size, sizeBytes);
    int offsetStart = start + 1 + sizeBytes;
    for (int i = 0; i < size; ++i) {
      VariantUtil.writeLong(
          writeBuffer,
          offsetStart + i * offsetSize,
          arrayBuilder.offsets().get(i),
          offsetSize);
    }
    VariantUtil.writeLong(writeBuffer, offsetStart + size * offsetSize, dataSize, offsetSize);
    return this;
  }

  /**
   * Adds a key to the Variant dictionary. If the key already exists, the dictionary is unmodified.
   * @param key the key to add
   * @return the id of the key
   */
  int addKey(String key) {
    return dictionary.computeIfAbsent(key, newKey -> {
      int id = dictionaryKeys.size();
      dictionaryKeys.add(newKey.getBytes(StandardCharsets.UTF_8));
      return id;
    });
  }

  /**
   * @return the current write position of the variant builder
   */
  int writePos() {
    return writePos;
  }

  /**
   * Class to store the information of a Variant object field. We need to collect all fields of
   * an object, sort them by their keys, and build the Variant object in sorted order.
   */
  static final class FieldEntry implements Comparable<FieldEntry> {
    final String key;
    final int id;
    final int offset;

    FieldEntry(String key, int id, int offset) {
      this.key = key;
      this.id = id;
      this.offset = offset;
    }

    FieldEntry withNewOffset(int newOffset) {
      return new FieldEntry(key, id, newOffset);
    }

    @Override
    public int compareTo(FieldEntry other) {
      return key.compareTo(other.key);
    }
  }

  private void checkCapacity(int additionalBytes) {
    int requiredBytes = writePos + additionalBytes;
    if (requiredBytes > writeBuffer.length) {
      // Allocate a new buffer with a capacity of the next power of 2 of `requiredBytes`.
      int newCapacity = Integer.highestOneBit(requiredBytes);
      newCapacity = newCapacity < requiredBytes ? newCapacity * 2 : newCapacity;
      byte[] newValue = new byte[newCapacity];
      System.arraycopy(writeBuffer, 0, newValue, 0, writePos);
      writeBuffer = newValue;
    }
  }

  private int getMinIntegerSize(int value) {
    assert value >= 0;
    if (value <= VariantUtil.U8_MAX) {
      return VariantUtil.U8_SIZE;
    }
    if (value <= VariantUtil.U16_MAX) {
      return VariantUtil.U16_SIZE;
    }
    if (value <= VariantUtil.U24_MAX) {
      return VariantUtil.U24_SIZE;
    }
    return VariantUtil.U32_SIZE;
  }

  /** The buffer for building the Variant value. The first `writePos` bytes have been written. */
  private byte[] writeBuffer = new byte[128];

  private int writePos = 0;
  /** The dictionary for mapping keys to monotonically increasing ids. */
  private final HashMap<String, Integer> dictionary = new HashMap<>();
  /** The keys in the dictionary, in id order. */
  private final ArrayList<byte[]> dictionaryKeys = new ArrayList<>();

  private final boolean allowDuplicateKeys;
}
