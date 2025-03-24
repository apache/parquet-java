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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.exc.InputCoercionException;
import java.io.IOException;
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
    this(allowDuplicateKeys, VariantUtil.DEFAULT_SIZE_LIMIT);
  }

  /**
   * Creates a VariantBuilder.
   * @param allowDuplicateKeys if true, only the last occurrence of a duplicate key will be kept.
   *                           Otherwise, an exception will be thrown.
   * @param sizeLimitBytes the maximum size (in bytes) of the resulting Variant value or metadata
   */
  public VariantBuilder(boolean allowDuplicateKeys, int sizeLimitBytes) {
    this.allowDuplicateKeys = allowDuplicateKeys;
    this.sizeLimitBytes = sizeLimitBytes;
  }

  /**
   * Parse a JSON string as a Variant value.
   * @param json the JSON string to parse
   * @return the Variant value
   * @throws IOException if any JSON parsing error happens
   * the size limit
   */
  public static Variant parseJson(String json) throws IOException {
    return parseJson(json, new VariantBuilder(false));
  }

  /**
   * Parse a JSON string as a Variant value.
   * @param json the JSON string to parse
   * @param builder the VariantBuilder to use for building the Variant
   * @return the Variant value
   * @throws IOException if any JSON parsing error happens
   */
  public static Variant parseJson(String json, VariantBuilder builder) throws IOException {
    try (JsonParser parser = new JsonFactory().createParser(json)) {
      parser.nextToken();
      return parseJson(parser, builder);
    }
  }

  /**
   * Parse a JSON parser as a Variant value.
   * @param parser the JSON parser to use
   * @param builder the VariantBuilder to use for building the Variant
   * @return the Variant value
   * @throws IOException if any JSON parsing error happens
   */
  public static Variant parseJson(JsonParser parser, VariantBuilder builder) throws IOException {
    builder.buildFromJsonParser(parser);
    return builder.result();
  }

  /**
   * @return the Variant value
   */
  public Variant result() {
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

  public void appendString(String str) {
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
  }

  public void appendNull() {
    checkCapacity(1);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.NULL);
  }

  public void appendBoolean(boolean b) {
    checkCapacity(1);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(b ? VariantUtil.TRUE : VariantUtil.FALSE);
  }

  /**
   * Appends a long value to the variant builder. The actual encoded integer type depends on the
   * value range of the long value.
   * @param l the long value to append
   */
  public void appendLong(long l) {
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
  }

  public void appendDouble(double d) {
    checkCapacity(1 + 8);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.DOUBLE);
    VariantUtil.writeLong(writeBuffer, writePos, Double.doubleToLongBits(d), 8);
    writePos += 8;
  }

  /**
   * Appends a decimal value to the variant builder. The actual encoded decimal type depends on the
   * precision and scale of the decimal value.
   * @param d the decimal value to append
   */
  public void appendDecimal(BigDecimal d) {
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
  }

  public void appendDate(int daysSinceEpoch) {
    checkCapacity(1 + 4);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.DATE);
    VariantUtil.writeLong(writeBuffer, writePos, daysSinceEpoch, 4);
    writePos += 4;
  }

  public void appendTimestamp(long microsSinceEpoch) {
    checkCapacity(1 + 8);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.TIMESTAMP);
    VariantUtil.writeLong(writeBuffer, writePos, microsSinceEpoch, 8);
    writePos += 8;
  }

  public void appendTimestampNtz(long microsSinceEpoch) {
    checkCapacity(1 + 8);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.TIMESTAMP_NTZ);
    VariantUtil.writeLong(writeBuffer, writePos, microsSinceEpoch, 8);
    writePos += 8;
  }

  public void appendTime(long microsSinceMidnight) {
    checkCapacity(1 + 8);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.TIME);
    VariantUtil.writeLong(writeBuffer, writePos, microsSinceMidnight, 8);
    writePos += 8;
  }

  public void appendTimestampNanos(long nanosSinceEpoch) {
    checkCapacity(1 + 8);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.TIMESTAMP_NANOS);
    VariantUtil.writeLong(writeBuffer, writePos, nanosSinceEpoch, 8);
    writePos += 8;
  }

  public void appendTimestampNanosNtz(long nanosSinceEpoch) {
    checkCapacity(1 + 8);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.TIMESTAMP_NANOS_NTZ);
    VariantUtil.writeLong(writeBuffer, writePos, nanosSinceEpoch, 8);
    writePos += 8;
  }

  public void appendFloat(float f) {
    checkCapacity(1 + 4);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.FLOAT);
    VariantUtil.writeLong(writeBuffer, writePos, Float.floatToIntBits(f), 8);
    writePos += 4;
  }

  public void appendBinary(byte[] binary) {
    checkCapacity(1 + VariantUtil.U32_SIZE + binary.length);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.BINARY);
    VariantUtil.writeLong(writeBuffer, writePos, binary.length, VariantUtil.U32_SIZE);
    writePos += VariantUtil.U32_SIZE;
    System.arraycopy(binary, 0, writeBuffer, writePos, binary.length);
    writePos += binary.length;
  }

  public void appendUUID(java.util.UUID uuid) {
    checkCapacity(1 + VariantUtil.UUID_SIZE);
    writeBuffer[writePos++] = VariantUtil.primitiveHeader(VariantUtil.UUID);

    ByteBuffer bb =
        ByteBuffer.wrap(writeBuffer, writePos, VariantUtil.UUID_SIZE).order(ByteOrder.BIG_ENDIAN);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    writePos += VariantUtil.UUID_SIZE;
  }

  /**
   * Adds a key to the Variant dictionary. If the key already exists, the dictionary is unmodified.
   * @param key the key to add
   * @return the id of the key
   */
  public int addKey(String key) {
    return dictionary.computeIfAbsent(key, newKey -> {
      int id = dictionaryKeys.size();
      dictionaryKeys.add(newKey.getBytes(StandardCharsets.UTF_8));
      return id;
    });
  }

  /**
   * @return the current write position of the variant builder
   */
  public int getWritePos() {
    return writePos;
  }

  /**
   * Finish writing a Variant object after all of its fields have already been written. The process
   * is as follows:
   * 1. The caller calls `getWritePos()` before writing any fields to obtain the `start` parameter.
   * 2. The caller appends all the object fields to the builder. In the meantime, it should maintain
   * the `fields` parameter. Before appending each field, it should append an entry to `fields` to
   * record the offset of the field. The offset is computed as `getWritePos() - start`.
   * 3. The caller calls `finishWritingObject` to finish writing the Variant object.
   *
   * This method will sort the fields by key. If there are duplicate field keys:
   * - when `allowDuplicateKeys` is true, the field with the greatest offset value (the last
   * appended one) is kept.
   * - otherwise, throw an exception.
   * @param start the start position of the object in the write buffer
   * @param fields the list of `FieldEntry` in the object
   * @throws VariantDuplicateKeyException if there are duplicate keys and `allowDuplicateKeys` is
   * false
   */
  public void finishWritingObject(int start, ArrayList<FieldEntry> fields) {
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
          int fieldSize = VariantUtil.valueSize(writeBuffer, start + oldOffset);
          System.arraycopy(writeBuffer, start + oldOffset, writeBuffer, start + currentOffset, fieldSize);
          fields.set(i, fields.get(i).withNewOffset(currentOffset));
          currentOffset += fieldSize;
        }
        writePos = start + currentOffset;
        // Change back to the sort order by field keys, required by the Variant specification.
        Collections.sort(fields);
      }
    } else {
      for (int i = 1; i < size; ++i) {
        maxId = Math.max(maxId, fields.get(i).id);
        String key = fields.get(i).key;
        if (key.equals(fields.get(i - 1).key)) {
          throw new VariantDuplicateKeyException(key);
        }
      }
    }
    int dataSize = writePos - start;
    boolean largeSize = size > VariantUtil.U8_MAX;
    int sizeBytes = largeSize ? VariantUtil.U32_SIZE : 1;
    int idSize = getMinIntegerSize(maxId);
    int offsetSize = getMinIntegerSize(dataSize);
    // The space for header byte, object size, id list, and offset list.
    int headerSize = 1 + sizeBytes + size * idSize + (size + 1) * offsetSize;
    checkCapacity(headerSize);
    // Shift the just-written field data to make room for the object header section.
    System.arraycopy(writeBuffer, start, writeBuffer, start + headerSize, dataSize);
    writePos += headerSize;
    writeBuffer[start] = VariantUtil.objectHeader(largeSize, idSize, offsetSize);
    VariantUtil.writeLong(writeBuffer, start + 1, size, sizeBytes);
    int idStart = start + 1 + sizeBytes;
    int offsetStart = idStart + size * idSize;
    for (int i = 0; i < size; ++i) {
      VariantUtil.writeLong(writeBuffer, idStart + i * idSize, fields.get(i).id, idSize);
      VariantUtil.writeLong(writeBuffer, offsetStart + i * offsetSize, fields.get(i).offset, offsetSize);
    }
    VariantUtil.writeLong(writeBuffer, offsetStart + size * offsetSize, dataSize, offsetSize);
  }

  /**
   * Finish writing a Variant array after all of its elements have already been written. The process
   * is similar to that of `finishWritingObject`.
   * @param start the start position of the array in the write buffer
   * @param offsets the list of offsets of the array elements
   */
  public void finishWritingArray(int start, ArrayList<Integer> offsets) {
    int dataSize = writePos - start;
    int size = offsets.size();
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
      VariantUtil.writeLong(writeBuffer, offsetStart + i * offsetSize, offsets.get(i), offsetSize);
    }
    VariantUtil.writeLong(writeBuffer, offsetStart + size * offsetSize, dataSize, offsetSize);
  }

  /**
   * Appends a Variant value to the Variant builder. The input Variant keys must be inserted into
   * the builder dictionary and rebuilt with new field ids. For scalar values in the input
   * Variant, we can directly copy the binary slice.
   * @param v the Variant value to append
   */
  public void appendVariant(Variant v) {
    appendVariantImpl(v.value, v.metadata, v.pos);
  }

  private void appendVariantImpl(byte[] value, byte[] metadata, int pos) {
    VariantUtil.checkIndex(pos, value.length);
    int basicType = value[pos] & VariantUtil.BASIC_TYPE_MASK;
    switch (basicType) {
      case VariantUtil.OBJECT: {
        VariantUtil.ObjectInfo info = VariantUtil.getObjectInfo(value, pos);
        ArrayList<FieldEntry> fields = new ArrayList<>(info.numElements);
        int start = writePos;
        for (int i = 0; i < info.numElements; ++i) {
          int id = VariantUtil.readUnsigned(value, pos + info.idStartOffset + info.idSize * i, info.idSize);
          int offset = VariantUtil.readUnsigned(
              value, pos + info.offsetStartOffset + info.offsetSize * i, info.offsetSize);
          int elementPos = pos + info.dataStartOffset + offset;
          String key = VariantUtil.getMetadataKey(metadata, id);
          int newId = addKey(key);
          fields.add(new FieldEntry(key, newId, writePos - start));
          appendVariantImpl(value, metadata, elementPos);
        }
        finishWritingObject(start, fields);
        break;
      }
      case VariantUtil.ARRAY: {
        VariantUtil.ArrayInfo info = VariantUtil.getArrayInfo(value, pos);
        ArrayList<Integer> offsets = new ArrayList<>(info.numElements);
        int start = writePos;
        for (int i = 0; i < info.numElements; ++i) {
          int offset = VariantUtil.readUnsigned(
              value, pos + info.offsetStartOffset + info.offsetSize * i, info.offsetSize);
          int elementPos = pos + info.dataStartOffset + offset;
          offsets.add(writePos - start);
          appendVariantImpl(value, metadata, elementPos);
        }
        finishWritingArray(start, offsets);
        break;
      }
      default:
        shallowAppendVariantImpl(value, pos);
        break;
    }
  }

  private void shallowAppendVariantImpl(byte[] value, int pos) {
    int size = VariantUtil.valueSize(value, pos);
    VariantUtil.checkIndex(pos + size - 1, value.length);
    checkCapacity(size);
    System.arraycopy(value, pos, writeBuffer, writePos, size);
    writePos += size;
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

  /**
   * Class to store the information of a Variant object field. We need to collect all fields of
   * an object, sort them by their keys, and build the Variant object in sorted order.
   */
  public static final class FieldEntry implements Comparable<FieldEntry> {
    final String key;
    final int id;
    final int offset;

    public FieldEntry(String key, int id, int offset) {
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

  private void buildFromJsonParser(JsonParser parser) throws IOException {
    JsonToken token = parser.currentToken();
    if (token == null) {
      throw new JsonParseException(parser, "Unexpected null token");
    }
    switch (token) {
      case START_OBJECT: {
        ArrayList<FieldEntry> fields = new ArrayList<>();
        int start = writePos;
        while (parser.nextToken() != JsonToken.END_OBJECT) {
          String key = parser.currentName();
          parser.nextToken();
          int id = addKey(key);
          fields.add(new FieldEntry(key, id, writePos - start));
          buildFromJsonParser(parser);
        }
        finishWritingObject(start, fields);
        break;
      }
      case START_ARRAY: {
        ArrayList<Integer> offsets = new ArrayList<>();
        int start = writePos;
        while (parser.nextToken() != JsonToken.END_ARRAY) {
          offsets.add(writePos - start);
          buildFromJsonParser(parser);
        }
        finishWritingArray(start, offsets);
        break;
      }
      case VALUE_STRING:
        appendString(parser.getText());
        break;
      case VALUE_NUMBER_INT:
        try {
          appendLong(parser.getLongValue());
        } catch (InputCoercionException ignored) {
          // If the value doesn't fit any integer type, try to parse it as decimal instead.
          if (!tryParseDecimal(parser.getText())) {
            throw new JsonParseException(parser, "Cannot parse token as int/decimal. token: " + token);
          }
        }
        break;
      case VALUE_NUMBER_FLOAT:
        parseAndAppendFloatingPoint(parser);
        break;
      case VALUE_TRUE:
        appendBoolean(true);
        break;
      case VALUE_FALSE:
        appendBoolean(false);
        break;
      case VALUE_NULL:
        appendNull();
        break;
      default:
        throw new JsonParseException(parser, "Unexpected token " + token);
    }
  }

  /**
   * Returns the size (number of bytes) of the smallest unsigned integer type that can store
   * `value`. It must be within `[0, U24_MAX]`.
   * @param value the value to get the size for
   * @return the size (number of bytes) of the smallest unsigned integer type that can store `value`
   */
  private int getMinIntegerSize(int value) {
    assert value >= 0 && value <= VariantUtil.U24_MAX;
    if (value <= VariantUtil.U8_MAX) {
      return VariantUtil.U8_SIZE;
    }
    if (value <= VariantUtil.U16_MAX) {
      return VariantUtil.U16_SIZE;
    }
    return VariantUtil.U24_SIZE;
  }

  /**
   * Parse a JSON number as a floating point value. If the number can be parsed as a decimal, it
   * will be appended as a decimal value. Otherwise, it will be appended as a double value.
   * @param parser the JSON parser to use
   */
  private void parseAndAppendFloatingPoint(JsonParser parser) throws IOException {
    if (!tryParseDecimal(parser.getText())) {
      appendDouble(parser.getDoubleValue());
    }
  }

  /**
   * Try to parse a JSON number as a decimal. The input must only use the decimal format
   * (an integer value with an optional '.' in it) and must not use scientific notation. It also
   * must fit into the precision limitation of decimal types.
   * @param input the input string to parse as decimal
   * @return whether the parsing succeeds
   */
  private boolean tryParseDecimal(String input) {
    for (int i = 0; i < input.length(); ++i) {
      char ch = input.charAt(i);
      if (ch != '-' && ch != '.' && !(ch >= '0' && ch <= '9')) {
        return false;
      }
    }
    BigDecimal d = new BigDecimal(input);
    if (d.scale() <= VariantUtil.MAX_DECIMAL16_PRECISION && d.precision() <= VariantUtil.MAX_DECIMAL16_PRECISION) {
      appendDecimal(d);
      return true;
    }
    return false;
  }

  /** The buffer for building the Variant value. The first `writePos` bytes have been written. */
  private byte[] writeBuffer = new byte[128];

  private int writePos = 0;
  /** The dictionary for mapping keys to monotonically increasing ids. */
  private final HashMap<String, Integer> dictionary = new HashMap<>();
  /** The keys in the dictionary, in id order. */
  private final ArrayList<byte[]> dictionaryKeys = new ArrayList<>();

  private final boolean allowDuplicateKeys;
  private final int sizeLimitBytes;
}
