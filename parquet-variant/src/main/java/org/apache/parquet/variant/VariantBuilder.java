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
import java.util.HashMap;

/**
 * Builder for creating Variant value and metadata.
 */
public class VariantBuilder {
  /** The buffer for building the Variant value. The first `writePos` bytes have been written. */
  protected byte[] writeBuffer = new byte[1024];

  protected int writePos = 0;
  /** The dictionary for mapping keys to monotonically increasing ids. */
  private final HashMap<String, Integer> dictionary = new HashMap<>();
  /** The keys in the dictionary, in id order. */
  private final ArrayList<byte[]> dictionaryKeys = new ArrayList<>();

  /** The number of values appended to this builder. Must be updated after each append(). */
  protected long numValues = 0;

  protected VariantObjectBuilder objectBuilder = null;

  /**
   * Creates a VariantBuilder.
   */
  public VariantBuilder() {}

  /**
   * @return the Variant value
   */
  public Variant build() {
    if (objectBuilder != null) {
      throw new IllegalStateException(
          "Cannot call build() while an object is being built. Must call endObject() first.");
    }
    int numKeys = dictionaryKeys.size();
    // Use long to avoid overflow in accumulating lengths.
    long dictionaryTotalDataSize = 0;
    for (byte[] key : dictionaryKeys) {
      dictionaryTotalDataSize += key.length;
    }
    // Determine the number of bytes required per offset entry.
    // The largest offset is the one-past-the-end value, which is total data size. It's very
    // unlikely that the number of keys could be larger, but incorporate that into the calculation
    // in case of pathological data.
    long maxSize = Math.max(dictionaryTotalDataSize, numKeys);
    int offsetSize = getMinIntegerSize((int) maxSize);

    int offsetListOffset = 1 + offsetSize;
    int dataOffset = offsetListOffset + (numKeys + 1) * offsetSize;
    long metadataSize = dataOffset + dictionaryTotalDataSize;

    byte[] metadata = new byte[(int) metadataSize];
    // Only unsorted dictionary keys are supported.
    // TODO: Support sorted dictionary keys.
    int headerByte = VariantUtil.VERSION | ((offsetSize - 1) << 6);
    VariantUtil.writeLong(metadata, 0, headerByte, 1);
    VariantUtil.writeLong(metadata, 1, numKeys, offsetSize);
    int currentOffset = 0;
    for (int i = 0; i < numKeys; ++i) {
      VariantUtil.writeLong(metadata, offsetListOffset + i * offsetSize, currentOffset, offsetSize);
      byte[] key = dictionaryKeys.get(i);
      System.arraycopy(key, 0, metadata, dataOffset + currentOffset, key.length);
      currentOffset += key.length;
    }
    VariantUtil.writeLong(metadata, offsetListOffset + numKeys * offsetSize, currentOffset, offsetSize);
    // Copying the data to a new buffer, to retain only the required data length, not the capacity.
    return new Variant(Arrays.copyOfRange(writeBuffer, 0, writePos), metadata);
  }

  /**
   * Appends a string value to the Variant builder.
   * @param str the string value to append
   * @return this builder
   */
  public VariantBuilder appendString(String str) {
    checkAppendState();
    byte[] data = str.getBytes(StandardCharsets.UTF_8);
    boolean longStr = data.length > VariantUtil.MAX_SHORT_STR_SIZE;
    checkCapacity((longStr ? 1 + VariantUtil.U32_SIZE : 1) + data.length);
    if (longStr) {
      writeBuffer[writePos] = VariantUtil.HEADER_LONG_STRING;
      writePos += 1;
      VariantUtil.writeLong(writeBuffer, writePos, data.length, VariantUtil.U32_SIZE);
      writePos += VariantUtil.U32_SIZE;
    } else {
      writeBuffer[writePos] = VariantUtil.shortStrHeader(data.length);
      writePos += 1;
    }
    System.arraycopy(data, 0, writeBuffer, writePos, data.length);
    writePos += data.length;
    numValues++;
    return this;
  }

  /**
   * Appends a null value to the Variant builder.
   * @return this builder
   */
  public VariantBuilder appendNull() {
    checkAppendState();
    checkCapacity(1);
    writeBuffer[writePos] = VariantUtil.HEADER_NULL;
    writePos += 1;
    numValues++;
    return this;
  }

  /**
   * Appends a boolean value to the Variant builder.
   * @param b the boolean value to append
   * @return this builder
   */
  public VariantBuilder appendBoolean(boolean b) {
    checkAppendState();
    checkCapacity(1);
    writeBuffer[writePos] = b ? VariantUtil.HEADER_TRUE : VariantUtil.HEADER_FALSE;
    writePos += 1;
    numValues++;
    return this;
  }

  /**
   * Appends a long value to the variant builder.
   * @param l the long value to append
   * @return this builder
   */
  public VariantBuilder appendLong(long l) {
    checkAppendState();
    checkCapacity(1 /* header size */ + 8);
    writeBuffer[writePos] = VariantUtil.HEADER_INT64;
    VariantUtil.writeLong(writeBuffer, writePos + 1, l, 8);
    writePos += 9;
    numValues++;
    return this;
  }

  /**
   * Appends an int value to the variant builder.
   * @param i the int to append
   * @return this builder
   */
  public VariantBuilder appendInt(int i) {
    checkAppendState();
    checkCapacity(1 /* header size */ + 4);
    writeBuffer[writePos] = VariantUtil.HEADER_INT32;
    VariantUtil.writeLong(writeBuffer, writePos + 1, i, 4);
    writePos += 5;
    numValues++;
    return this;
  }

  /**
   * Appends a short value to the variant builder.
   * @param s the short to append
   * @return this builder
   */
  public VariantBuilder appendShort(short s) {
    checkAppendState();
    checkCapacity(1 /* header size */ + 2);
    writeBuffer[writePos] = VariantUtil.HEADER_INT16;
    VariantUtil.writeLong(writeBuffer, writePos + 1, s, 2);
    writePos += 3;
    numValues++;
    return this;
  }

  /**
   * Appends a byte value to the variant builder.
   * @param b the byte to append
   * @return this builder
   */
  public VariantBuilder appendByte(byte b) {
    checkAppendState();
    checkCapacity(1 /* header size */ + 1);
    writeBuffer[writePos] = VariantUtil.HEADER_INT8;
    VariantUtil.writeLong(writeBuffer, writePos + 1, b, 1);
    writePos += 2;
    numValues++;
    return this;
  }

  /**
   * Appends a double value to the variant builder.
   * @param d the double to append
   * @return this builder
   */
  public VariantBuilder appendDouble(double d) {
    checkAppendState();
    checkCapacity(1 /* header size */ + 8);
    writeBuffer[writePos] = VariantUtil.HEADER_DOUBLE;
    VariantUtil.writeLong(writeBuffer, writePos + 1, Double.doubleToLongBits(d), 8);
    writePos += 9;
    numValues++;
    return this;
  }

  /**
   * Appends a decimal value to the variant builder. The actual encoded decimal type depends on the
   * precision and scale of the decimal value.
   * @param d the decimal value to append
   * @return this builder
   */
  public VariantBuilder appendDecimal(BigDecimal d) {
    checkAppendState();
    BigInteger unscaled = d.unscaledValue();
    if (d.scale() <= VariantUtil.MAX_DECIMAL4_PRECISION && d.precision() <= VariantUtil.MAX_DECIMAL4_PRECISION) {
      checkCapacity(2 /* header and scale size */ + 4);
      writeBuffer[writePos] = VariantUtil.HEADER_DECIMAL4;
      writeBuffer[writePos + 1] = (byte) d.scale();
      VariantUtil.writeLong(writeBuffer, writePos + 2, unscaled.intValueExact(), 4);
      writePos += 6;
    } else if (d.scale() <= VariantUtil.MAX_DECIMAL8_PRECISION
        && d.precision() <= VariantUtil.MAX_DECIMAL8_PRECISION) {
      checkCapacity(2 /* header and scale size */ + 8);
      writeBuffer[writePos] = VariantUtil.HEADER_DECIMAL8;
      writeBuffer[writePos + 1] = (byte) d.scale();
      VariantUtil.writeLong(writeBuffer, writePos + 2, unscaled.longValueExact(), 8);
      writePos += 10;
    } else {
      assert d.scale() <= VariantUtil.MAX_DECIMAL16_PRECISION
          && d.precision() <= VariantUtil.MAX_DECIMAL16_PRECISION;
      checkCapacity(2 /* header and scale size */ + 16);
      writeBuffer[writePos] = VariantUtil.HEADER_DECIMAL16;
      writeBuffer[writePos + 1] = (byte) d.scale();
      writePos += 2;
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
    numValues++;
    return this;
  }

  /**
   * Appends a date value to the variant builder. The date is represented as the number of days
   * since the epoch.
   * @param daysSinceEpoch the number of days since the epoch
   * @return this builder
   */
  public VariantBuilder appendDate(int daysSinceEpoch) {
    checkAppendState();
    checkCapacity(1 /* header size */ + 4);
    writeBuffer[writePos] = VariantUtil.HEADER_DATE;
    VariantUtil.writeLong(writeBuffer, writePos + 1, daysSinceEpoch, 4);
    writePos += 5;
    numValues++;
    return this;
  }

  /**
   * Appends a TimestampTz value to the variant builder. The timestamp is represented as the number
   * of microseconds since the epoch.
   * @param microsSinceEpoch the number of microseconds since the epoch
   * @return this builder
   */
  public VariantBuilder appendTimestampTz(long microsSinceEpoch) {
    checkAppendState();
    checkCapacity(1 /* header size */ + 8);
    writeBuffer[writePos] = VariantUtil.HEADER_TIMESTAMP_TZ;
    VariantUtil.writeLong(writeBuffer, writePos + 1, microsSinceEpoch, 8);
    writePos += 9;
    numValues++;
    return this;
  }

  /**
   * Appends a TimestampNtz value to the variant builder. The timestamp is represented as the number
   * of microseconds since the epoch.
   * @param microsSinceEpoch the number of microseconds since the epoch
   * @return this builder
   */
  public VariantBuilder appendTimestampNtz(long microsSinceEpoch) {
    checkAppendState();
    checkCapacity(1 /* header size */ + 8);
    writeBuffer[writePos] = VariantUtil.HEADER_TIMESTAMP_NTZ;
    VariantUtil.writeLong(writeBuffer, writePos + 1, microsSinceEpoch, 8);
    writePos += 9;
    numValues++;
    return this;
  }

  /**
   * Appends a Time value to the variant builder. The time is represented as the number of
   * microseconds since midnight.
   * @param microsSinceMidnight the number of microseconds since midnight
   * @return this builder
   */
  public VariantBuilder appendTime(long microsSinceMidnight) {
    if (microsSinceMidnight < 0) {
      throw new IllegalArgumentException(
          String.format("Time value (%d) cannot be negative.", microsSinceMidnight));
    }
    checkAppendState();
    checkCapacity(1 /* header size */ + 8);
    writeBuffer[writePos] = VariantUtil.HEADER_TIME;
    VariantUtil.writeLong(writeBuffer, writePos + 1, microsSinceMidnight, 8);
    writePos += 9;
    numValues++;
    return this;
  }

  /**
   * Appends a TimestampNanosTz value to the variant builder. The timestamp is represented as the
   * number of nanoseconds since the epoch.
   * @param nanosSinceEpoch the number of nanoseconds since the epoch
   * @return this builder
   */
  public VariantBuilder appendTimestampNanosTz(long nanosSinceEpoch) {
    checkAppendState();
    checkCapacity(1 /* header size */ + 8);
    writeBuffer[writePos] = VariantUtil.HEADER_TIMESTAMP_NANOS_TZ;
    VariantUtil.writeLong(writeBuffer, writePos + 1, nanosSinceEpoch, 8);
    writePos += 9;
    numValues++;
    return this;
  }

  /**
   * Appends a TimestampNanosNtz value to the variant builder. The timestamp is represented as the
   * number of nanoseconds since the epoch.
   * @param nanosSinceEpoch the number of nanoseconds since the epoch
   * @return this builder
   */
  public VariantBuilder appendTimestampNanosNtz(long nanosSinceEpoch) {
    checkAppendState();
    checkCapacity(1 /* header size */ + 8);
    writeBuffer[writePos] = VariantUtil.HEADER_TIMESTAMP_NANOS_NTZ;
    VariantUtil.writeLong(writeBuffer, writePos + 1, nanosSinceEpoch, 8);
    writePos += 9;
    numValues++;
    return this;
  }

  /**
   * Appends a float value to the variant builder.
   * @param f the float to append
   * @return this builder
   */
  public VariantBuilder appendFloat(float f) {
    checkAppendState();
    checkCapacity(1 /* header size */ + 4);
    writeBuffer[writePos] = VariantUtil.HEADER_FLOAT;
    VariantUtil.writeLong(writeBuffer, writePos + 1, Float.floatToIntBits(f), 8);
    writePos += 5;
    numValues++;
    return this;
  }

  /**
   * Appends binary data to the variant builder.
   * @param binary the binary data to append
   * @return this builder
   */
  public VariantBuilder appendBinary(ByteBuffer binary) {
    checkAppendState();
    int binarySize = binary.remaining();
    checkCapacity(1 /* header size */ + VariantUtil.U32_SIZE + binarySize);
    writeBuffer[writePos] = VariantUtil.HEADER_BINARY;
    writePos += 1;
    VariantUtil.writeLong(writeBuffer, writePos, binarySize, VariantUtil.U32_SIZE);
    writePos += VariantUtil.U32_SIZE;
    ByteBuffer.wrap(writeBuffer, writePos, binarySize).put(binary);
    writePos += binarySize;
    numValues++;
    return this;
  }

  /**
   * Appends a UUID value to the variant builder.
   * @param uuid the UUID to append
   * @return this builder
   */
  public VariantBuilder appendUUID(java.util.UUID uuid) {
    checkAppendState();
    checkCapacity(1 /* header size */ + VariantUtil.UUID_SIZE);
    writeBuffer[writePos] = VariantUtil.HEADER_UUID;
    writePos += 1;

    ByteBuffer bb =
        ByteBuffer.wrap(writeBuffer, writePos, VariantUtil.UUID_SIZE).order(ByteOrder.BIG_ENDIAN);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    writePos += VariantUtil.UUID_SIZE;
    numValues++;
    return this;
  }

  protected void checkAppendState() {
    if (objectBuilder != null) {
      throw new IllegalStateException(
          "Cannot call append() methods while an object is being built. Must call endObject() first.");
    }
    if (numValues > 0) {
      throw new IllegalStateException("Cannot call multiple append() methods.");
    }
  }

  /**
   * Starts appending an object to this variant builder. The returned VariantObjectBuilder is used
   * to append object keys and values. startObject() must be called before endObject().
   * No append*() methods can be called in between startObject() and endObject().
   *
   * Example usage:
   * VariantBuilder builder = new VariantBuilder();
   * VariantObjectBuilder objBuilder = builder.startObject();
   * objBuilder.appendKey("key1");
   * objBuilder.appendString("value1");
   * builder.endObject();
   *
   * @return a VariantObjectBuilder to build an object
   */
  public VariantObjectBuilder startObject() {
    if (objectBuilder != null) {
      throw new IllegalStateException("Cannot call startObject() without calling endObject() first.");
    }
    objectBuilder = new VariantObjectBuilder(this);
    return objectBuilder;
  }

  /**
   * Finishes appending the object to this builder. This method must be called after startObject(),
   * before other append*() methods can be called on this builder.
   * @return this builder
   */
  protected VariantBuilder endObject() {
    if (objectBuilder == null) {
      throw new IllegalStateException("Cannot call endObject() without calling startObject() first.");
    }
    ArrayList<FieldEntry> fields = objectBuilder.validateAndGetFields();
    int numFields = fields.size();
    Collections.sort(fields);
    int maxId = numFields == 0 ? 0 : fields.get(0).id;
    int dataSize = numFields == 0 ? 0 : fields.get(0).valueSize;

    int distinctPos = 0;
    // Maintain a list of distinct keys in-place.
    for (int i = 1; i < numFields; ++i) {
      maxId = Math.max(maxId, fields.get(i).id);
      if (fields.get(i).id == fields.get(i - 1).id) {
        // Found a duplicate key. Keep the field with the greater offset.
        if (fields.get(distinctPos).offset < fields.get(i).offset) {
          fields.set(distinctPos, fields.get(i));
        }
      } else {
        // Found a distinct key. Add the field to the list.
        distinctPos++;
        fields.set(distinctPos, fields.get(i));
        dataSize += fields.get(i).valueSize;
      }
    }

    if (distinctPos + 1 < fields.size()) {
      numFields = distinctPos + 1;
      // Resize `fields` to `size`.
      fields.subList(numFields, fields.size()).clear();
    }

    boolean largeSize = numFields > VariantUtil.U8_MAX;
    int sizeBytes = largeSize ? VariantUtil.U32_SIZE : 1;
    int idSize = getMinIntegerSize(maxId);
    int offsetSize = getMinIntegerSize(dataSize);
    // The space for header byte, object size, id list, and offset list.
    int headerSize = 1 + sizeBytes + numFields * idSize + (numFields + 1) * offsetSize;
    checkCapacity(headerSize + dataSize);

    // Write the header byte and size entry.
    writeBuffer[writePos] = VariantUtil.objectHeader(largeSize, idSize, offsetSize);
    VariantUtil.writeLong(writeBuffer, writePos + 1, numFields, sizeBytes);

    int idStart = writePos + 1 + sizeBytes;
    int offsetStart = idStart + numFields * idSize;
    int fieldDataStart = offsetStart + (numFields + 1) * offsetSize;
    int currOffset = 0;

    // Loop over all fields and write the key id, offset, and data to the appropriate offsets
    for (int i = 0; i < numFields; ++i) {
      FieldEntry field = fields.get(i);
      VariantUtil.writeLong(writeBuffer, idStart + i * idSize, field.id, idSize);
      VariantUtil.writeLong(writeBuffer, offsetStart + i * offsetSize, currOffset, offsetSize);
      System.arraycopy(
          objectBuilder.writeBuffer, field.offset, writeBuffer, fieldDataStart + currOffset, field.valueSize);
      currOffset += field.valueSize;
    }
    VariantUtil.writeLong(writeBuffer, offsetStart + numFields * offsetSize, dataSize, offsetSize);
    writePos += headerSize + dataSize;
    numValues++;
    objectBuilder = null;
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
    numValues++;
    return this;
  }

  /**
   * Adds a key to the Variant dictionary. If the key already exists, the dictionary is unmodified.
   * @param key the key to add
   * @return the id of the key
   */
  int addDictionaryKey(String key) {
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
    int valueSize = 0;

    FieldEntry(String key, int id, int offset) {
      this.key = key;
      this.id = id;
      this.offset = offset;
    }

    void updateValueSize(int size) {
      valueSize = size;
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

  protected int getMinIntegerSize(int value) {
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
}
