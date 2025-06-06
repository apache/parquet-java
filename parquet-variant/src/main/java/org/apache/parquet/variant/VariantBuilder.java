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
import java.util.Collections;
import java.util.Set;

/**
 * Builder for creating Variant value and metadata.
 */
public class VariantBuilder {

  /**
   * The buffer for building the Variant value. The first `writePos` bytes have been written.
   */
  protected byte[] writeBuffer = new byte[1024];

  protected int writePos = 0;

  /**
   * Object and array builders share the same Metadata object as the main builder.
   */
  protected Metadata metadata;

  /**
   * These are used to build nested objects and arrays, via startObject() and startArray().
   * Only one of these can be non-null at a time. If one of these is non-null, then no append()
   * methods can be called on this builder, until endObject() or endArray() is called.
   */
  protected VariantObjectBuilder objectBuilder = null;

  protected VariantArrayBuilder arrayBuilder = null;

  /**
   * Creates a VariantBuilder.
   */
  public VariantBuilder() {
    this.metadata = new MetadataBuilder();
  }

  /**
   * Creates a VariantBuilder with a non-default metadata object.
   */
  public VariantBuilder(Metadata metadata) {
    this.metadata = metadata;
  }

  /**
   * @return the Variant value
   */
  public Variant build() {
    if (objectBuilder != null) {
      throw new IllegalStateException(
          "Cannot call build() while an object is being built. Must call endObject() first.");
    }
    if (arrayBuilder != null) {
      throw new IllegalStateException(
          "Cannot call build() while an array is being built. Must call endArray() first.");
    }
    ByteBuffer metadataBuffer = metadata.getEncodedBuffer();
    // Copying the data to a new buffer, to retain only the required data length, not the capacity.
    // TODO: Reduce the copying, and look into builder reuse.
    return new Variant(ByteBuffer.wrap(writeBuffer, 0, writePos), metadataBuffer);
  }

  /**
   * @return the constructed Variant value binary, without metadata.
   */
  public ByteBuffer encodedValue() {
    return ByteBuffer.wrap(writeBuffer, 0, writePos);
  }

  /**
   * Directly append a Variant value. Its keys must already be in the metadata
   * dictionary.
   */
  public void appendEncodedValue(ByteBuffer value) {
    onAppend();
    int size = VariantUtil.valueSize(value);
    checkCapacity(size);
    value.duplicate().get(writeBuffer, writePos, size);
    writePos += size;
  }

  /**
   * Appends a string value to the Variant builder.
   *
   * @param str the string value to append
   */
  public void appendString(String str) {
    onAppend();
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
  }

  /**
   * Appends a null value to the Variant builder.
   */
  public void appendNull() {
    onAppend();
    checkCapacity(1);
    writeBuffer[writePos] = VariantUtil.HEADER_NULL;
    writePos += 1;
  }

  public void appendNullIfEmpty() {
    if (writePos == 0) {
      appendNull();
    }
  }

  /**
   * Appends a boolean value to the Variant builder.
   *
   * @param b the boolean value to append
   */
  public void appendBoolean(boolean b) {
    onAppend();
    checkCapacity(1);
    writeBuffer[writePos] = b ? VariantUtil.HEADER_TRUE : VariantUtil.HEADER_FALSE;
    writePos += 1;
  }

  /**
   * Appends a long value to the variant builder.
   *
   * @param l the long value to append
   */
  public void appendLong(long l) {
    onAppend();
    checkCapacity(1 /* header size */ + 8);
    writeBuffer[writePos] = VariantUtil.HEADER_INT64;
    VariantUtil.writeLong(writeBuffer, writePos + 1, l, 8);
    writePos += 9;
  }

  /**
   * Appends an int value to the variant builder.
   *
   * @param i the int to append
   */
  public void appendInt(int i) {
    onAppend();
    checkCapacity(1 /* header size */ + 4);
    writeBuffer[writePos] = VariantUtil.HEADER_INT32;
    VariantUtil.writeLong(writeBuffer, writePos + 1, i, 4);
    writePos += 5;
  }

  /**
   * Appends a short value to the variant builder.
   *
   * @param s the short to append
   */
  public void appendShort(short s) {
    onAppend();
    checkCapacity(1 /* header size */ + 2);
    writeBuffer[writePos] = VariantUtil.HEADER_INT16;
    VariantUtil.writeLong(writeBuffer, writePos + 1, s, 2);
    writePos += 3;
  }

  /**
   * Appends a byte value to the variant builder.
   *
   * @param b the byte to append
   */
  public void appendByte(byte b) {
    onAppend();
    checkCapacity(1 /* header size */ + 1);
    writeBuffer[writePos] = VariantUtil.HEADER_INT8;
    VariantUtil.writeLong(writeBuffer, writePos + 1, b, 1);
    writePos += 2;
  }

  /**
   * Appends a double value to the variant builder.
   *
   * @param d the double to append
   */
  public void appendDouble(double d) {
    onAppend();
    checkCapacity(1 /* header size */ + 8);
    writeBuffer[writePos] = VariantUtil.HEADER_DOUBLE;
    VariantUtil.writeLong(writeBuffer, writePos + 1, Double.doubleToLongBits(d), 8);
    writePos += 9;
  }

  /**
   * Appends a decimal value to the variant builder. The actual encoded decimal type depends on the
   * precision and scale of the decimal value.
   *
   * @param d the decimal value to append
   */
  public void appendDecimal(BigDecimal d) {
    onAppend();
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
  }

  /**
   * Appends a date value to the variant builder. The date is represented as the number of days
   * since the epoch.
   *
   * @param daysSinceEpoch the number of days since the epoch
   */
  public void appendDate(int daysSinceEpoch) {
    onAppend();
    checkCapacity(1 /* header size */ + 4);
    writeBuffer[writePos] = VariantUtil.HEADER_DATE;
    VariantUtil.writeLong(writeBuffer, writePos + 1, daysSinceEpoch, 4);
    writePos += 5;
  }

  /**
   * Appends a TimestampTz value to the variant builder. The timestamp is represented as the number
   * of microseconds since the epoch.
   *
   * @param microsSinceEpoch the number of microseconds since the epoch
   */
  public void appendTimestampTz(long microsSinceEpoch) {
    onAppend();
    checkCapacity(1 /* header size */ + 8);
    writeBuffer[writePos] = VariantUtil.HEADER_TIMESTAMP_TZ;
    VariantUtil.writeLong(writeBuffer, writePos + 1, microsSinceEpoch, 8);
    writePos += 9;
  }

  /**
   * Appends a TimestampNtz value to the variant builder. The timestamp is represented as the number
   * of microseconds since the epoch.
   *
   * @param microsSinceEpoch the number of microseconds since the epoch
   */
  public void appendTimestampNtz(long microsSinceEpoch) {
    onAppend();
    checkCapacity(1 /* header size */ + 8);
    writeBuffer[writePos] = VariantUtil.HEADER_TIMESTAMP_NTZ;
    VariantUtil.writeLong(writeBuffer, writePos + 1, microsSinceEpoch, 8);
    writePos += 9;
  }

  /**
   * Appends a Time value to the variant builder. The time is represented as the number of
   * microseconds since midnight.
   *
   * @param microsSinceMidnight the number of microseconds since midnight
   */
  public void appendTime(long microsSinceMidnight) {
    if (microsSinceMidnight < 0) {
      throw new IllegalArgumentException(
          String.format("Time value (%d) cannot be negative.", microsSinceMidnight));
    }
    onAppend();
    checkCapacity(1 /* header size */ + 8);
    writeBuffer[writePos] = VariantUtil.HEADER_TIME;
    VariantUtil.writeLong(writeBuffer, writePos + 1, microsSinceMidnight, 8);
    writePos += 9;
  }

  /**
   * Appends a TimestampNanosTz value to the variant builder. The timestamp is represented as the
   * number of nanoseconds since the epoch.
   *
   * @param nanosSinceEpoch the number of nanoseconds since the epoch
   */
  public void appendTimestampNanosTz(long nanosSinceEpoch) {
    onAppend();
    checkCapacity(1 /* header size */ + 8);
    writeBuffer[writePos] = VariantUtil.HEADER_TIMESTAMP_NANOS_TZ;
    VariantUtil.writeLong(writeBuffer, writePos + 1, nanosSinceEpoch, 8);
    writePos += 9;
  }

  /**
   * Appends a TimestampNanosNtz value to the variant builder. The timestamp is represented as the
   * number of nanoseconds since the epoch.
   *
   * @param nanosSinceEpoch the number of nanoseconds since the epoch
   */
  public void appendTimestampNanosNtz(long nanosSinceEpoch) {
    onAppend();
    checkCapacity(1 /* header size */ + 8);
    writeBuffer[writePos] = VariantUtil.HEADER_TIMESTAMP_NANOS_NTZ;
    VariantUtil.writeLong(writeBuffer, writePos + 1, nanosSinceEpoch, 8);
    writePos += 9;
  }

  /**
   * Appends a float value to the variant builder.
   *
   * @param f the float to append
   */
  public void appendFloat(float f) {
    onAppend();
    checkCapacity(1 /* header size */ + 4);
    writeBuffer[writePos] = VariantUtil.HEADER_FLOAT;
    VariantUtil.writeLong(writeBuffer, writePos + 1, Float.floatToIntBits(f), 8);
    writePos += 5;
  }

  /**
   * Appends binary data to the variant builder.
   *
   * @param binary the binary data to append
   */
  public void appendBinary(ByteBuffer binary) {
    onAppend();
    int binarySize = binary.remaining();
    checkCapacity(1 /* header size */ + VariantUtil.U32_SIZE + binarySize);
    writeBuffer[writePos] = VariantUtil.HEADER_BINARY;
    writePos += 1;
    VariantUtil.writeLong(writeBuffer, writePos, binarySize, VariantUtil.U32_SIZE);
    writePos += VariantUtil.U32_SIZE;
    ByteBuffer.wrap(writeBuffer, writePos, binarySize).put(binary);
    writePos += binarySize;
  }

  /**
   * Appends a UUID value to the variant builder.
   *
   * @param uuid the UUID to append
   */
  public void appendUUID(java.util.UUID uuid) {
    onAppend();
    checkCapacity(1 /* header size */ + VariantUtil.UUID_SIZE);
    writeBuffer[writePos] = VariantUtil.HEADER_UUID;
    writePos += 1;

    ByteBuffer bb =
        ByteBuffer.wrap(writeBuffer, writePos, VariantUtil.UUID_SIZE).order(ByteOrder.BIG_ENDIAN);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    writePos += VariantUtil.UUID_SIZE;
  }

  /**
   * Append raw bytes in the form stored in Variant.
   *
   * @param bytes a 16-byte value.
   */
  void appendUUIDBytes(ByteBuffer bytes) {
    checkCapacity(1 + VariantUtil.UUID_SIZE);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.UUID);
    if (bytes.remaining() < VariantUtil.UUID_SIZE) {
      throw new IllegalArgumentException("UUID must be exactly 16 bytes");
    }
    bytes.duplicate().get(writeBuffer, writePos, VariantUtil.UUID_SIZE);
    writePos += VariantUtil.UUID_SIZE;
  }

  /**
   * Starts appending an object to this variant builder. The returned VariantObjectBuilder is used
   * to append object keys and values. startObject() must be called before endObject().
   * No append*() methods can be called in between startObject() and endObject().
   * <p>
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
    onStartNested();
    if (objectBuilder != null) {
      throw new IllegalStateException("Cannot call startObject() without calling endObject() first.");
    }
    if (arrayBuilder != null) {
      throw new IllegalStateException("Cannot call startObject() without calling endArray() first.");
    }
    this.objectBuilder = new VariantObjectBuilder(this.metadata);
    return objectBuilder;
  }

  public VariantObjectBuilder startOrContinueObject() {
    if (objectBuilder != null) {
      return objectBuilder;
    }

    return startObject();
  }

  public VariantObjectBuilder startOrContinuePartialObject(ByteBuffer value, Set<String> suppressedKeys) {
    VariantObjectBuilder objectBuilder = startOrContinueObject();

    // copy values to a new builder
    Variant variant = new Variant(value, metadata);
    for (int index = 0; index < variant.numObjectElements(); index += 1) {
      Variant.ObjectField field = variant.getFieldAtIndex(index);
      if (!suppressedKeys.contains(field.key)) {
        objectBuilder.appendKey(field.key);
        objectBuilder.appendEncodedValue(field.value.getValueBuffer());
      }
    }

    return objectBuilder;
  }

  public void endObjectIfExists() {
    if (objectBuilder != null) {
      endObject();
    }
  }

  /**
   * Finishes appending the object to this builder. This method must be called after startObject(),
   * before other append*() methods can be called on this builder.
   */
  public void endObject() {
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
        // Found a duplicate key. Keep the field with the greater offset, because it was written last.
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
    // The data starts after: the header byte, object size, id list, and offset list.
    int dataOffset = 1 + sizeBytes + numFields * idSize + (numFields + 1) * offsetSize;
    checkCapacity(dataOffset + dataSize);

    // Write the header byte and size entry.
    writeBuffer[writePos] = VariantUtil.objectHeader(largeSize, idSize, offsetSize);
    VariantUtil.writeLong(writeBuffer, writePos + 1, numFields, sizeBytes);

    int idStart = writePos + 1 + sizeBytes;
    int offsetStart = idStart + numFields * idSize;
    int currOffset = 0;

    // Loop over all fields and write the key id, offset, and data to the appropriate offsets
    for (int i = 0; i < numFields; ++i) {
      FieldEntry field = fields.get(i);
      VariantUtil.writeLong(writeBuffer, idStart + i * idSize, field.id, idSize);
      VariantUtil.writeLong(writeBuffer, offsetStart + i * offsetSize, currOffset, offsetSize);
      System.arraycopy(
          objectBuilder.writeBuffer,
          field.offset,
          writeBuffer,
          writePos + dataOffset + currOffset,
          field.valueSize);
      currOffset += field.valueSize;
    }
    VariantUtil.writeLong(writeBuffer, offsetStart + numFields * offsetSize, dataSize, offsetSize);
    writePos += dataOffset + dataSize;
    this.objectBuilder = null;
  }

  /**
   * Starts appending an array to this variant builder. The returned VariantArrayBuilder is used to
   * append values ot the array. startArray() must be called before endArray(). No append*() methods
   * can be called in between startArray() and endArray().
   * <p>
   * Example usage:
   * VariantBuilder builder = new VariantBuilder();
   * VariantArrayBuilder arrayBuilder = builder.startArray();
   * arrayBuilder.appendString("value1");
   * arrayBuilder.appendString("value2");
   * builder.endArray();
   *
   * @return a VariantArrayBuilder to use for startArrayElement() and endArray().
   */
  public VariantArrayBuilder startArray() {
    onStartNested();
    if (objectBuilder != null) {
      throw new IllegalStateException("Cannot call startArray() without calling endObject() first.");
    }
    if (arrayBuilder != null) {
      throw new IllegalStateException("Cannot call startArray() without calling endArray() first.");
    }
    this.arrayBuilder = new VariantArrayBuilder(this.metadata);
    return arrayBuilder;
  }

  /**
   * Ends appending an array to this variant builder. This method must be called after all elements
   * have been added to the array.
   */
  public void endArray() {
    if (arrayBuilder == null) {
      throw new IllegalStateException("Cannot call endArray() without calling startArray() first.");
    }
    ArrayList<Integer> offsets = arrayBuilder.validateAndGetOffsets();
    int numElements = offsets.size();
    int dataSize = arrayBuilder.writePos;
    boolean largeSize = numElements > VariantUtil.U8_MAX;
    int sizeBytes = largeSize ? VariantUtil.U32_SIZE : 1;
    int offsetSize = getMinIntegerSize(dataSize);
    // The data starts after: the header byte, object size, and offset list.
    int dataOffset = 1 + sizeBytes + (numElements + 1) * offsetSize;
    checkCapacity(dataOffset + dataSize);

    // Copy all the element data to the write buffer.
    System.arraycopy(arrayBuilder.writeBuffer, 0, writeBuffer, writePos + dataOffset, dataSize);

    writeBuffer[writePos] = VariantUtil.arrayHeader(largeSize, offsetSize);
    VariantUtil.writeLong(writeBuffer, writePos + 1, numElements, sizeBytes);

    int offsetStart = writePos + 1 + sizeBytes;
    for (int i = 0; i < numElements; ++i) {
      VariantUtil.writeLong(writeBuffer, offsetStart + i * offsetSize, offsets.get(i), offsetSize);
    }
    VariantUtil.writeLong(writeBuffer, offsetStart + numElements * offsetSize, dataSize, offsetSize);
    writePos += dataOffset + dataSize;
    this.arrayBuilder = null;
  }

  protected void onAppend() {
    checkAppendWhileNested();
    if (writePos > 0) {
      throw new IllegalStateException("Cannot call multiple append() methods.");
    }
  }

  protected void onStartNested() {
    checkMultipleNested("Cannot call startObject()/startArray() without calling endObject()/endArray() first.");
    if (writePos > 0) {
      throw new IllegalStateException("Cannot call startObject()/startArray() after appending a value.");
    }
  }

  protected void checkMultipleNested(String message) {
    if (objectBuilder != null || arrayBuilder != null) {
      throw new IllegalStateException(message);
    }
  }

  protected void checkAppendWhileNested() {
    if (objectBuilder != null) {
      throw new IllegalStateException(
          "Cannot call append() methods while an object is being built. Must call endObject() first.");
    }
    if (arrayBuilder != null) {
      throw new IllegalStateException(
          "Cannot call append() methods while an array is being built. Must call endArray() first.");
    }
  }

  /**
   * Adds a key to the Variant dictionary. If the key already exists, the dictionary is unmodified.
   *
   * @param key the key to add
   * @return the id of the key
   */
  int addDictionaryKey(String key) {
    return metadata.getOrInsert(key);
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
      this.writeBuffer = newValue;
    }
  }

  public static int getMinIntegerSize(int value) {
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
