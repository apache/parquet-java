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
import java.util.Arrays;

/**
 * This class defines constants related to the Variant format and provides functions for
 * manipulating Variant binaries.
 *
 * A Variant is made up of 2 binaries: value and metadata. A Variant value consists of a one-byte
 * header and a number of content bytes (can be zero). The header byte is divided into upper 6 bits
 * (called "type info") and lower 2 bits (called "basic type"). The content format is explained in
 * the below constants for all possible basic type and type info values.
 *
 * The Variant metadata includes a version id and a dictionary of distinct strings (case-sensitive).
 * Its binary format is:
 * - Version: 1-byte unsigned integer. The only acceptable value is 1 currently.
 * - Dictionary size: 4-byte little-endian unsigned integer. The number of keys in the
 *                    dictionary.
 * - Offsets: (size + 1) * 4-byte little-endian unsigned integers. `offsets[i]` represents the
 * starting position of string i, counting starting from the address of `offsets[0]`. Strings
 * must be stored contiguously, so we don’t need to store the string size, instead, we compute it
 * with `offset[i + 1] - offset[i]`.
 * - UTF-8 string data.
 */
class VariantUtil {
  static final int BASIC_TYPE_BITS = 2;
  static final int BASIC_TYPE_MASK = 0b00000011;
  static final int PRIMITIVE_TYPE_MASK = 0b00111111;
  /** The inclusive maximum value of the type info value. It is the size limit of `SHORT_STR`. */
  static final int MAX_SHORT_STR_SIZE = 0b00111111;

  // The basic types

  /**
   * Primitive value.
   * The type info value must be one of the values in the "Primitive" section below.
   */
  static final int PRIMITIVE = 0;
  /**
   * Short string value.
   * The type info value is the string size, which must be in `[0, MAX_SHORT_STR_SIZE]`.
   * The string content bytes directly follow the header byte.
   */
  static final int SHORT_STR = 1;
  /**
   * Object value.
   * The content contains a size, a list of field ids, a list of field offsets, and
   * the actual field values. The list of field ids has `size` ids, while the list of field offsets
   * has `size + 1` offsets, where the last offset represents the total size of the field values
   * data. The list of fields ids must be sorted by the field name in alphabetical order.
   * Duplicate field names within one object are not allowed.
   * 5 bits in the type info are used to specify the integer type of the object header. It is
   * 0_b4_b3b2_b1b0 (most significant bit is 0), where:
   *   - b4: the integer type of size. When it is 0/1, `size` is a little-endian 1/4-byte
   *         unsigned integer.
   *   - b3b2: the integer type of ids. When the 2 bits are 0/1/2, the id list contains
   *           1/2/3-byte little-endian unsigned integers.
   *   - b1b0: the integer type of offset. When the 2 bits are 0/1/2, the offset list contains
   *           1/2/3-byte little-endian unsigned integers.
   */
  static final int OBJECT = 2;
  /**
   * Array value.
   * The content contains a size, a list of field offsets, and the actual element values.
   * It is similar to an object without the id list. The length of the offset list
   * is `size + 1`, where the last offset represent the total size of the element data.
   * Its type info is: 000_b2_b1b0:
   *   - b2: the type of size.
   *   - b1b0: the integer type of offset.
   */
  static final int ARRAY = 3;

  // The primitive types

  /** JSON Null value. Empty content. */
  static final int NULL = 0;
  /** True value. Empty content. */
  static final int TRUE = 1;
  /** False value. Empty content. */
  static final int FALSE = 2;
  /** 1-byte little-endian signed integer. */
  static final int INT8 = 3;
  /** 2-byte little-endian signed integer. */
  static final int INT16 = 4;
  /** 4-byte little-endian signed integer. */
  static final int INT32 = 5;
  /** 4-byte little-endian signed integer. */
  static final int INT64 = 6;
  /** 8-byte IEEE double. */
  static final int DOUBLE = 7;
  /** 4-byte decimal. Content is 1-byte scale + 4-byte little-endian signed integer. */
  static final int DECIMAL4 = 8;
  /** 8-byte decimal. Content is 1-byte scale + 8-byte little-endian signed integer. */
  static final int DECIMAL8 = 9;
  /** 16-byte decimal. Content is 1-byte scale + 16-byte little-endian signed integer. */
  static final int DECIMAL16 = 10;
  /**
   * Date value. Content is 4-byte little-endian signed integer that represents the
   * number of days from the Unix epoch.
   */
  static final int DATE = 11;
  /**
   * Timestamp value. Content is 8-byte little-endian signed integer that represents the number of
   * microseconds elapsed since the Unix epoch, 1970-01-01 00:00:00 UTC. It is displayed to users in
   * their local time zones and may be displayed differently depending on the execution environment.
   */
  static final int TIMESTAMP_TZ = 12;
  /**
   * Timestamp_ntz value. It has the same content as `TIMESTAMP` but should always be interpreted
   * as if the local time zone is UTC.
   */
  static final int TIMESTAMP_NTZ = 13;
  /** 4-byte IEEE float. */
  static final int FLOAT = 14;
  /**
   * Binary value. The content is (4-byte little-endian unsigned integer representing the binary
   * size) + (size bytes of binary content).
   */
  static final int BINARY = 15;
  /**
   * Long string value. The content is (4-byte little-endian unsigned integer representing the
   * string size) + (size bytes of string content).
   */
  static final int LONG_STR = 16;
  /**
   * Time value. Values can be from 00:00:00 to 23:59:59.999999.
   * Content is 8-byte little-endian unsigned integer that represents the number of microseconds
   * since midnight.
   */
  static final int TIME = 17;
  /**
   * Timestamp nanos value. Similar to `TIMESTAMP_TZ`, but represents the number of nanoseconds
   * elapsed since the Unix epoch, 1970-01-01 00:00:00 UTC.
   */
  static final int TIMESTAMP_NANOS_TZ = 18;
  /**
   * Timestamp nanos (without timestamp) value. It has the same content as `TIMESTAMP_NANOS_TZ` but
   * should always be interpreted as if the local time zone is UTC.
   */
  static final int TIMESTAMP_NANOS_NTZ = 19;
  /**
   * UUID value. The content is a 16-byte binary, encoded using big-endian.
   * For example, UUID 00112233-4455-6677-8899-aabbccddeeff is encoded as the bytes
   * 00 11 22 33 44 55 66 77 88 99 aa bb cc dd ee ff.
   */
  static final int UUID = 20;

  // The metadata version.
  static final byte VERSION = 1;
  // The lower 4 bits of the first metadata byte contain the version.
  static final byte VERSION_MASK = 0x0F;

  // Constants for various unsigned integer sizes.
  static final int U8_MAX = 0xFF;
  static final int U16_MAX = 0xFFFF;
  static final int U24_MAX = 0xFFFFFF;
  static final int U8_SIZE = 1;
  static final int U16_SIZE = 2;
  static final int U24_SIZE = 3;
  static final int U32_SIZE = 4;

  // Max decimal precision for each decimal type.
  static final int MAX_DECIMAL4_PRECISION = 9;
  static final int MAX_DECIMAL8_PRECISION = 18;
  static final int MAX_DECIMAL16_PRECISION = 38;

  // The size (in bytes) of a UUID.
  static final int UUID_SIZE = 16;

  // header bytes
  static final byte HDR_NULL = primitiveHeader(NULL);
  static final byte HDR_LONG_STRING = primitiveHeader(LONG_STR);
  static final byte HDR_TRUE = primitiveHeader(TRUE);
  static final byte HDR_FALSE = primitiveHeader(FALSE);
  static final byte HDR_INT8 = primitiveHeader(INT8);
  static final byte HDR_INT16 = primitiveHeader(INT16);
  static final byte HDR_INT32 = primitiveHeader(INT32);
  static final byte HDR_INT64 = primitiveHeader(INT64);
  static final byte HDR_DOUBLE = primitiveHeader(DOUBLE);
  static final byte HDR_DECIMAL4 = primitiveHeader(DECIMAL4);
  static final byte HDR_DECIMAL8 = primitiveHeader(DECIMAL8);
  static final byte HDR_DECIMAL16 = primitiveHeader(DECIMAL16);
  static final byte HDR_DATE = primitiveHeader(DATE);
  static final byte HDR_TIMESTAMP_TZ = primitiveHeader(TIMESTAMP_TZ);
  static final byte HDR_TIMESTAMP_NTZ = primitiveHeader(TIMESTAMP_NTZ);
  static final byte HDR_TIME = primitiveHeader(TIME);
  static final byte HDR_TIMESTAMP_NANOS_TZ = primitiveHeader(TIMESTAMP_NANOS_TZ);
  static final byte HDR_TIMESTAMP_NANOS_NTZ = primitiveHeader(TIMESTAMP_NANOS_NTZ);
  static final byte HDR_FLOAT = primitiveHeader(FLOAT);
  static final byte HDR_BINARY = primitiveHeader(BINARY);
  static final byte HDR_UUID = primitiveHeader(UUID);

  static byte primitiveHeader(int type) {
    return (byte) (type << 2 | PRIMITIVE);
  }

  static byte shortStrHeader(int size) {
    return (byte) (size << 2 | SHORT_STR);
  }

  static byte objectHeader(boolean largeSize, int idSize, int offsetSize) {
    return (byte) (((largeSize ? 1 : 0) << (BASIC_TYPE_BITS + 4))
        | ((idSize - 1) << (BASIC_TYPE_BITS + 2))
        | ((offsetSize - 1) << BASIC_TYPE_BITS)
        | OBJECT);
  }

  static byte arrayHeader(boolean largeSize, int offsetSize) {
    return (byte) (((largeSize ? 1 : 0) << (BASIC_TYPE_BITS + 2)) | ((offsetSize - 1) << BASIC_TYPE_BITS) | ARRAY);
  }

  /**
   * Check the validity of an array index `pos`.
   * @param pos The index to check
   * @param length The length of the array
   * @throws IllegalArgumentException if the index is out of bound
   */
  static void checkIndex(int pos, int length) {
    if (pos < 0 || pos >= length) {
      throw new IllegalArgumentException(
          String.format("Invalid byte-array offset (%d). length: %d", pos, length));
    }
  }

  /**
   * Write the least significant `numBytes` bytes in `value` into `bytes[pos, pos + numBytes)` in
   * little endian.
   * @param bytes The byte array to write into
   * @param pos The starting index of the byte array to write into
   * @param value The value to write
   * @param numBytes The number of bytes to write
   */
  static void writeLong(byte[] bytes, int pos, long value, int numBytes) {
    for (int i = 0; i < numBytes; ++i) {
      bytes[pos + i] = (byte) ((value >>> (8 * i)) & 0xFF);
    }
  }

  /**
   * Reads a little-endian signed long value from `buffer[pos, pos + numBytes)`.
   * @param buffer The ByteBuffer to read from
   * @param pos The starting index of the buffer to read from
   * @param numBytes The number of bytes to read
   * @return The long value
   */
  static long readLong(ByteBuffer buffer, int pos, int numBytes) {
    checkIndex(pos, buffer.limit());
    checkIndex(pos + numBytes - 1, buffer.limit());
    long result = 0;
    // All bytes except the most significant byte should be unsigned-extended and shifted
    // (so we need & 0xFF`). The most significant byte should be sign-extended and is handled
    // after the loop.
    for (int i = 0; i < numBytes - 1; ++i) {
      long unsignedByteValue = buffer.get(pos + i) & 0xFF;
      result |= unsignedByteValue << (8 * i);
    }
    long signedByteValue = buffer.get(pos + numBytes - 1);
    result |= signedByteValue << (8 * (numBytes - 1));
    return result;
  }

  /**
   * Read a little-endian unsigned int value from `bytes[pos, pos + numBytes)`. The value must fit
   * into a non-negative int (`[0, Integer.MAX_VALUE]`).
   */
  static int readUnsigned(ByteBuffer bytes, int pos, int numBytes) {
    checkIndex(pos, bytes.limit());
    checkIndex(pos + numBytes - 1, bytes.limit());
    int result = 0;
    // Similar to the `readLong` loop, but all bytes should be unsigned-extended.
    for (int i = 0; i < numBytes; ++i) {
      int unsignedByteValue = bytes.get(pos + i) & 0xFF;
      result |= unsignedByteValue << (8 * i);
    }
    if (result < 0) {
      throw new IllegalArgumentException(String.format("Failed to read unsigned int. numBytes: %d", numBytes));
    }
    return result;
  }

  /**
   * Returns the value type of Variant value `value[pos...]`. It is only legal to call `get*` if
   * `getType` returns the corresponding type. For example, it is only legal to call
   * `getLong` if this method returns `Type.Long`.
   * @param value The Variant value to get the type from
   * @return The type of the Variant value
   */
  static Variant.Type getType(ByteBuffer value) {
    checkIndex(value.position(), value.limit());
    int basicType = value.get(value.position()) & BASIC_TYPE_MASK;
    int typeInfo = (value.get(value.position()) >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    switch (basicType) {
      case SHORT_STR:
        return Variant.Type.STRING;
      case OBJECT:
        return Variant.Type.OBJECT;
      case ARRAY:
        return Variant.Type.ARRAY;
      default:
        switch (typeInfo) {
          case NULL:
            return Variant.Type.NULL;
          case TRUE:
          case FALSE:
            return Variant.Type.BOOLEAN;
          case INT8:
            return Variant.Type.BYTE;
          case INT16:
            return Variant.Type.SHORT;
          case INT32:
            return Variant.Type.INT;
          case INT64:
            return Variant.Type.LONG;
          case DOUBLE:
            return Variant.Type.DOUBLE;
          case DECIMAL4:
            return Variant.Type.DECIMAL4;
          case DECIMAL8:
            return Variant.Type.DECIMAL8;
          case DECIMAL16:
            return Variant.Type.DECIMAL16;
          case DATE:
            return Variant.Type.DATE;
          case TIMESTAMP_TZ:
            return Variant.Type.TIMESTAMP_TZ;
          case TIMESTAMP_NTZ:
            return Variant.Type.TIMESTAMP_NTZ;
          case FLOAT:
            return Variant.Type.FLOAT;
          case BINARY:
            return Variant.Type.BINARY;
          case LONG_STR:
            return Variant.Type.STRING;
          case TIME:
            return Variant.Type.TIME;
          case TIMESTAMP_NANOS_TZ:
            return Variant.Type.TIMESTAMP_NANOS_TZ;
          case TIMESTAMP_NANOS_NTZ:
            return Variant.Type.TIMESTAMP_NANOS_NTZ;
          case UUID:
            return Variant.Type.UUID;
          default:
            throw new UnsupportedOperationException(
                String.format("Unknown type in Variant. primitive type: %d", typeInfo));
        }
    }
  }

  /**
   * Computes the actual size (in bytes) of the Variant value at `value[pos...]`
   * @param value The Variant value
   * @param pos The starting index of the Variant value
   * @return The size (in bytes) of the Variant value
   */
  public static int valueSize(ByteBuffer value, int pos) {
    checkIndex(pos, value.limit());
    int basicType = value.get(pos) & BASIC_TYPE_MASK;
    int typeInfo = (value.get(pos) >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    switch (basicType) {
      case SHORT_STR:
        return 1 + typeInfo;
      case OBJECT: {
        VariantUtil.ObjectInfo info = VariantUtil.getObjectInfo(slice(value, pos));
        return info.dataStartOffset
            + readUnsigned(
                value,
                pos + info.offsetStartOffset + info.numElements * info.offsetSize,
                info.offsetSize);
      }
      case ARRAY: {
        VariantUtil.ArrayInfo info = VariantUtil.getArrayInfo(slice(value, pos));
        return info.dataStartOffset
            + readUnsigned(
                value,
                pos + info.offsetStartOffset + info.numElements * info.offsetSize,
                info.offsetSize);
      }
      default:
        switch (typeInfo) {
          case NULL:
          case TRUE:
          case FALSE:
            return 1;
          case INT8:
            return 2;
          case INT16:
            return 3;
          case INT32:
          case DATE:
          case FLOAT:
            return 5;
          case INT64:
          case DOUBLE:
          case TIMESTAMP_TZ:
          case TIMESTAMP_NTZ:
          case TIME:
          case TIMESTAMP_NANOS_TZ:
          case TIMESTAMP_NANOS_NTZ:
            return 9;
          case DECIMAL4:
            return 6;
          case DECIMAL8:
            return 10;
          case DECIMAL16:
            return 18;
          case BINARY:
          case LONG_STR:
            return 1 + U32_SIZE + readUnsigned(value, pos + 1, U32_SIZE);
          case UUID:
            return 1 + UUID_SIZE;
          default:
            throw new UnsupportedOperationException(
                String.format("Unknown type in Variant. primitive type: %d", typeInfo));
        }
    }
  }

  /**
   * Returns the debug string representation of the type of the Variant value `value[pos...]`.
   * @param value The Variant value to get the type from
   * @return The String representation of the type of the Variant value
   */
  private static String getTypeDebugString(ByteBuffer value) {
    try {
      return getType(value).toString();
    } catch (Exception e) {
      int basicType = value.get(value.position()) & BASIC_TYPE_MASK;
      int valueHeader = (value.get(value.position()) >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
      return String.format("unknownType(basicType: %d, valueHeader: %d)", basicType, valueHeader);
    }
  }

  private static IllegalArgumentException unexpectedType(Variant.Type type, ByteBuffer actualValue) {
    String actualType = getTypeDebugString(actualValue);
    return new IllegalArgumentException(String.format("Cannot read %s value as %s", actualType, type));
  }

  private static IllegalArgumentException unexpectedType(Variant.Type[] types, ByteBuffer actualValue) {
    String actualType = getTypeDebugString(actualValue);
    return new IllegalArgumentException(
        String.format("Cannot read %s value as one of %s", actualType, Arrays.toString(types)));
  }

  static boolean getBoolean(ByteBuffer value) {
    checkIndex(value.position(), value.limit());
    int basicType = value.get(value.position()) & BASIC_TYPE_MASK;
    int typeInfo = (value.get(value.position()) >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != PRIMITIVE || (typeInfo != TRUE && typeInfo != FALSE)) {
      throw unexpectedType(Variant.Type.BOOLEAN, value);
    }
    return typeInfo == TRUE;
  }

  /**
   * Returns a long value from Variant value `value[pos...]`.
   * It is only legal to call it if `getType` returns one of Type.BYTE, SHORT, INT, LONG,
   * DATE, TIMESTAMP_TZ, TIMESTAMP_NTZ, TIME, TIMESTAMP_NANOS_TZ, TIMESTAMP_NANOS_NTZ.
   * If the type is `DATE`, the return value is guaranteed to fit into an int and
   * represents the number of days from the Unix epoch.
   * If the type is `TIMESTAMP_TZ/TIMESTAMP_NTZ`, the return value represents the number of
   * microseconds from the Unix epoch.
   * If the type is `TIME`, the return value represents the number of microseconds since midnight.
   * If the type is `TIMESTAMP_NANOS_TZ/TIMESTAMP_NANOS_NTZ`, the return value represents the number
   * of nanoseconds from the Unix epoch.
   * @param value The Variant value
   * @return The long value
   */
  static long getLong(ByteBuffer value) {
    checkIndex(value.position(), value.limit());
    int basicType = value.get(value.position()) & BASIC_TYPE_MASK;
    int typeInfo = (value.get(value.position()) >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != PRIMITIVE) {
      throw unexpectedType(
          new Variant.Type[] {
            Variant.Type.BYTE,
            Variant.Type.SHORT,
            Variant.Type.INT,
            Variant.Type.DATE,
            Variant.Type.LONG,
            Variant.Type.TIMESTAMP_TZ,
            Variant.Type.TIMESTAMP_NTZ,
            Variant.Type.TIME,
            Variant.Type.TIMESTAMP_NANOS_TZ,
            Variant.Type.TIMESTAMP_NANOS_NTZ
          },
          value);
    }
    switch (typeInfo) {
      case INT8:
        return readLong(value, value.position() + 1, 1);
      case INT16:
        return readLong(value, value.position() + 1, 2);
      case INT32:
      case DATE:
        return readLong(value, value.position() + 1, 4);
      case INT64:
      case TIMESTAMP_TZ:
      case TIMESTAMP_NTZ:
      case TIME:
      case TIMESTAMP_NANOS_TZ:
      case TIMESTAMP_NANOS_NTZ:
        return readLong(value, value.position() + 1, 8);
      default:
        throw unexpectedType(
            new Variant.Type[] {
              Variant.Type.BYTE,
              Variant.Type.SHORT,
              Variant.Type.INT,
              Variant.Type.DATE,
              Variant.Type.LONG,
              Variant.Type.TIMESTAMP_TZ,
              Variant.Type.TIMESTAMP_NTZ,
              Variant.Type.TIME,
              Variant.Type.TIMESTAMP_NANOS_TZ,
              Variant.Type.TIMESTAMP_NANOS_NTZ
            },
            value);
    }
  }

  /**
   * Similar to getLong(), but for the types: Type.BYTE, SHORT, INT, DATE.
   * @param value The Variant value
   * @return The int value
   */
  static int getInt(ByteBuffer value) {
    checkIndex(value.position(), value.limit());
    int basicType = value.get(value.position()) & BASIC_TYPE_MASK;
    int typeInfo = (value.get(value.position()) >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != PRIMITIVE) {
      throw unexpectedType(
          new Variant.Type[] {Variant.Type.BYTE, Variant.Type.SHORT, Variant.Type.INT, Variant.Type.DATE},
          value);
    }
    switch (typeInfo) {
      case INT8:
        return (int) readLong(value, value.position() + 1, 1);
      case INT16:
        return (int) readLong(value, value.position() + 1, 2);
      case INT32:
      case DATE:
        return (int) readLong(value, value.position() + 1, 4);
      default:
        throw unexpectedType(
            new Variant.Type[] {Variant.Type.BYTE, Variant.Type.SHORT, Variant.Type.INT, Variant.Type.DATE},
            value);
    }
  }

  /**
   * Similar to getLong(), but for the types: Type.BYTE, SHORT.
   * @param value The Variant value
   * @return The short value
   */
  static short getShort(ByteBuffer value) {
    checkIndex(value.position(), value.limit());
    int basicType = value.get(value.position()) & BASIC_TYPE_MASK;
    int typeInfo = (value.get(value.position()) >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != PRIMITIVE) {
      throw unexpectedType(new Variant.Type[] {Variant.Type.BYTE, Variant.Type.SHORT}, value);
    }
    switch (typeInfo) {
      case INT8:
        return (short) readLong(value, value.position() + 1, 1);
      case INT16:
        return (short) readLong(value, value.position() + 1, 2);
      default:
        throw unexpectedType(new Variant.Type[] {Variant.Type.BYTE, Variant.Type.SHORT}, value);
    }
  }

  /**
   * Similar to getLong(), but for the types: Type.BYTE, SHORT.
   * @param value The Variant value
   * @return The short value
   */
  static byte getByte(ByteBuffer value) {
    checkIndex(value.position(), value.limit());
    int basicType = value.get(value.position()) & BASIC_TYPE_MASK;
    int typeInfo = (value.get(value.position()) >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != PRIMITIVE) {
      throw unexpectedType(Variant.Type.BYTE, value);
    }
    switch (typeInfo) {
      case INT8:
        return (byte) readLong(value, value.position() + 1, 1);
      default:
        throw unexpectedType(Variant.Type.BYTE, value);
    }
  }

  static double getDouble(ByteBuffer value) {
    checkIndex(value.position(), value.limit());
    int basicType = value.get(value.position()) & BASIC_TYPE_MASK;
    int typeInfo = (value.get(value.position()) >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != PRIMITIVE || typeInfo != DOUBLE) {
      throw unexpectedType(Variant.Type.DOUBLE, value);
    }
    return Double.longBitsToDouble(readLong(value, value.position() + 1, 8));
  }

  static BigDecimal getDecimalWithOriginalScale(ByteBuffer value) {
    checkIndex(value.position(), value.limit());
    int basicType = value.get(value.position()) & BASIC_TYPE_MASK;
    int typeInfo = (value.get(value.position()) >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != PRIMITIVE) {
      throw unexpectedType(
          new Variant.Type[] {Variant.Type.DECIMAL4, Variant.Type.DECIMAL8, Variant.Type.DECIMAL16}, value);
    }
    // Interpret the scale byte as unsigned. If it is a negative byte, the unsigned value must be
    // greater than `MAX_DECIMAL16_PRECISION` and will trigger an error in `checkDecimal`.
    int scale = value.get(value.position() + 1) & 0xFF;
    BigDecimal result;
    switch (typeInfo) {
      case DECIMAL4:
        result = BigDecimal.valueOf(readLong(value, value.position() + 2, 4), scale);
        break;
      case DECIMAL8:
        result = BigDecimal.valueOf(readLong(value, value.position() + 2, 8), scale);
        break;
      case DECIMAL16:
        checkIndex(value.position() + 17, value.limit());
        byte[] bytes = new byte[16];
        // Copy the bytes reversely because the `BigInteger` constructor expects a big-endian
        // representation.
        for (int i = 0; i < 16; ++i) {
          bytes[i] = value.get(value.position() + 17 - i);
        }
        result = new BigDecimal(new BigInteger(bytes), scale);
        break;
      default:
        throw unexpectedType(
            new Variant.Type[] {Variant.Type.DECIMAL4, Variant.Type.DECIMAL8, Variant.Type.DECIMAL16},
            value);
    }
    return result;
  }

  static BigDecimal getDecimal(ByteBuffer value) {
    return getDecimalWithOriginalScale(value);
  }

  static float getFloat(ByteBuffer value) {
    checkIndex(value.position(), value.limit());
    int basicType = value.get(value.position()) & BASIC_TYPE_MASK;
    int typeInfo = (value.get(value.position()) >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != PRIMITIVE || typeInfo != FLOAT) {
      throw unexpectedType(Variant.Type.FLOAT, value);
    }
    return Float.intBitsToFloat((int) readLong(value, value.position() + 1, 4));
  }

  static ByteBuffer getBinary(ByteBuffer value) {
    checkIndex(value.position(), value.limit());
    int basicType = value.get(value.position()) & BASIC_TYPE_MASK;
    int typeInfo = (value.get(value.position()) >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != PRIMITIVE || typeInfo != BINARY) {
      throw unexpectedType(Variant.Type.BINARY, value);
    }
    int start = value.position() + 1 + U32_SIZE;
    int length = readUnsigned(value, value.position() + 1, U32_SIZE);
    checkIndex(start + length - 1, value.limit());
    return slice(value, start);
  }

  static String getString(ByteBuffer value) {
    checkIndex(value.position(), value.limit());
    int basicType = value.get(value.position()) & BASIC_TYPE_MASK;
    int typeInfo = (value.get(value.position()) >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType == SHORT_STR || (basicType == PRIMITIVE && typeInfo == LONG_STR)) {
      int start;
      int length;
      if (basicType == SHORT_STR) {
        start = value.position() + 1;
        length = typeInfo;
      } else {
        start = value.position() + 1 + U32_SIZE;
        length = readUnsigned(value, value.position() + 1, U32_SIZE);
      }
      checkIndex(start + length - 1, value.limit());
      if (value.hasArray()) {
        // If the buffer is backed by an array, we can use the array directly.
        return new String(value.array(), value.arrayOffset() + start, length);
      } else {
        // If the buffer is not backed by an array, we need to copy the bytes into a new array.
        byte[] valueArray = new byte[length];
        slice(value, start).get(valueArray);
        return new String(valueArray);
      }
    }
    throw unexpectedType(Variant.Type.STRING, value);
  }

  static java.util.UUID getUUID(ByteBuffer value) {
    checkIndex(value.position(), value.limit());
    int basicType = value.get(value.position()) & BASIC_TYPE_MASK;
    int typeInfo = (value.get(value.position()) >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != PRIMITIVE || typeInfo != UUID) {
      throw unexpectedType(Variant.Type.UUID, value);
    }
    int start = value.position() + 1;
    checkIndex(start + UUID_SIZE - 1, value.limit());
    ByteBuffer bb = slice(value, start).order(ByteOrder.BIG_ENDIAN);
    return new java.util.UUID(bb.getLong(), bb.getLong());
  }

  /**
   * Slices the `value` buffer starting from `start` index.
   * @param value The ByteBuffer to slice
   * @param start The starting index of the slice
   * @return The sliced ByteBuffer
   */
  static ByteBuffer slice(ByteBuffer value, int start) {
    ByteBuffer newSlice = value.duplicate();
    newSlice.position(start);
    return newSlice;
  }

  /**
   * A helper class representing the details of a Variant object, used for `ObjectHandler`.
   */
  static class ObjectInfo {
    /** Number of object fields. */
    public final int numElements;
    /** The integer size of the field id list. */
    public final int idSize;
    /** The integer size of the offset list. */
    public final int offsetSize;
    /** The byte offset (from the beginning of the Variant object) of the field id list. */
    public final int idStartOffset;
    /** The byte offset (from the beginning of the Variant object) of the offset list. */
    public final int offsetStartOffset;
    /** The byte offset (from the beginning of the Variant object) of the field data. */
    public final int dataStartOffset;

    public ObjectInfo(
        int numElements,
        int idSize,
        int offsetSize,
        int idStartOffset,
        int offsetStartOffset,
        int dataStartOffset) {
      this.numElements = numElements;
      this.idSize = idSize;
      this.offsetSize = offsetSize;
      this.idStartOffset = idStartOffset;
      this.offsetStartOffset = offsetStartOffset;
      this.dataStartOffset = dataStartOffset;
    }
  }

  /**
   * Parses the object at `value[pos...]`, and returns the object details.
   */
  static ObjectInfo getObjectInfo(ByteBuffer value) {
    checkIndex(value.position(), value.limit());
    int basicType = value.get(value.position()) & BASIC_TYPE_MASK;
    int typeInfo = (value.get(value.position()) >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != OBJECT) {
      throw unexpectedType(Variant.Type.OBJECT, value);
    }
    // Refer to the comment of the `OBJECT` constant for the details of the object header encoding.
    // Suppose `typeInfo` has a bit representation of 0_b4_b3b2_b1b0, the following line extracts
    // b4 to determine whether the object uses a 1/4-byte size.
    boolean largeSize = ((typeInfo >> 4) & 0x1) != 0;
    int sizeBytes = (largeSize ? U32_SIZE : 1);
    int numElements = readUnsigned(value, value.position() + 1, sizeBytes);
    // Extracts b3b2 to determine the integer size of the field id list.
    int idSize = ((typeInfo >> 2) & 0x3) + 1;
    // Extracts b1b0 to determine the integer size of the offset list.
    int offsetSize = (typeInfo & 0x3) + 1;
    int idStartOffset = 1 + sizeBytes;
    int offsetStartOffset = idStartOffset + numElements * idSize;
    int dataStartOffset = offsetStartOffset + (numElements + 1) * offsetSize;
    return new ObjectInfo(numElements, idSize, offsetSize, idStartOffset, offsetStartOffset, dataStartOffset);
  }

  /**
   * A helper class representing the details of a Variant array, used for `ArrayHandler`.
   */
  static class ArrayInfo {
    /** Number of object fields. */
    public final int numElements;
    /** The integer size of the offset list. */
    public final int offsetSize;
    /** The byte offset (from the beginning of the Variant array) of the offset list. */
    public final int offsetStartOffset;
    /** The byte offset (from the beginning of the Variant array) of the field data. */
    public final int dataStartOffset;

    public ArrayInfo(int numElements, int offsetSize, int offsetStartOffset, int dataStartOffset) {
      this.numElements = numElements;
      this.offsetSize = offsetSize;
      this.offsetStartOffset = offsetStartOffset;
      this.dataStartOffset = dataStartOffset;
    }
  }

  /**
   * Parses the array at `value[pos...]`, and returns the array details.
   */
  static ArrayInfo getArrayInfo(ByteBuffer value) {
    checkIndex(value.position(), value.limit());
    int basicType = value.get(value.position()) & BASIC_TYPE_MASK;
    int typeInfo = (value.get(value.position()) >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != ARRAY) {
      throw unexpectedType(Variant.Type.ARRAY, value);
    }
    // Refer to the comment of the `ARRAY` constant for the details of the object header encoding.
    // Suppose `typeInfo` has a bit representation of 000_b2_b1b0, the following line extracts
    // b2 to determine whether the object uses a 1/4-byte size.
    boolean largeSize = ((typeInfo >> 2) & 0x1) != 0;
    int sizeBytes = (largeSize ? U32_SIZE : 1);
    int numElements = readUnsigned(value, value.position() + 1, sizeBytes);
    // Extracts b1b0 to determine the integer size of the offset list.
    int offsetSize = (typeInfo & 0x3) + 1;
    int offsetStartOffset = 1 + sizeBytes;
    int dataStartOffset = offsetStartOffset + (numElements + 1) * offsetSize;
    return new ArrayInfo(numElements, offsetSize, offsetStartOffset, dataStartOffset);
  }

  /**
   * Returns a key at `id` in the Variant metadata.
   *
   * @param metadata The Variant metadata
   * @param id The key id
   * @return The key
   * @throws IllegalArgumentException if the id is out of bound
   * @throws IllegalStateException if the encoded metadata is malformed
   */
  static String getMetadataKey(ByteBuffer metadata, int id) {
    // Extracts the highest 2 bits in the metadata header to determine the integer size of the
    // offset list.
    int offsetSize = ((metadata.get(metadata.position()) >> 6) & 0x3) + 1;
    int dictSize = readUnsigned(metadata, metadata.position() + 1, offsetSize);
    if (id >= dictSize) {
      throw new IllegalArgumentException(
          String.format("Invalid dictionary id: %d. dictionary size: %d", id, dictSize));
    }
    // The offset list after the header byte, and a `dictSize` with `offsetSize` bytes.
    int offsetListPos = metadata.position() + 1 + offsetSize;
    // The data starts after the offset list, and `(dictSize + 1)` offset values.
    int dataPos = offsetListPos + (dictSize + 1) * offsetSize;
    int offset = readUnsigned(metadata, offsetListPos + (id) * offsetSize, offsetSize);
    int nextOffset = readUnsigned(metadata, offsetListPos + (id + 1) * offsetSize, offsetSize);
    if (offset > nextOffset) {
      throw new IllegalStateException(String.format("Invalid offset: %d. next offset: %d", offset, nextOffset));
    }
    checkIndex(dataPos + nextOffset - 1, metadata.limit());
    if (metadata.hasArray()) {
      return new String(metadata.array(), metadata.arrayOffset() + dataPos + offset, nextOffset - offset);
    } else {
      // ByteBuffer does not have an array, so we need to use the `get` method to read the bytes.
      byte[] metadataArray = new byte[nextOffset - offset];
      slice(metadata, dataPos + offset).get(metadataArray);
      return new String(metadataArray);
    }
  }
}
