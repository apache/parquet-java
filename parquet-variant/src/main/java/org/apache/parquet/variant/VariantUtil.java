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

import java.io.IOException;
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
public class VariantUtil {
  public static final int BASIC_TYPE_BITS = 2;
  public static final int BASIC_TYPE_MASK = 0b00000011;
  public static final int PRIMITIVE_TYPE_MASK = 0b00111111;
  /** The inclusive maximum value of the type info value. It is the size limit of `SHORT_STR`. */
  public static final int MAX_SHORT_STR_SIZE = 0b00111111;

  // The basic types

  /**
   * Primitive value.
   * The type info value must be one of the values in the "Primitive" section below.
   */
  public static final int PRIMITIVE = 0;
  /**
   * Short string value.
   * The type info value is the string size, which must be in `[0, MAX_SHORT_STR_SIZE]`.
   * The string content bytes directly follow the header byte.
   */
  public static final int SHORT_STR = 1;
  /**
   * Object value.
   * The content contains a size, a list of field ids, a list of field offsets, and
   * the actual field values. The list of field ids has `size` ids, while the list of field offsets
   * has `size + 1` offsets, where the last offset represents the total size of the field values
   * data. The list of fields ids must be sorted by the field name in alphabetical order.
   * Duplicate field names within one object are not allowed.
   * 5 bits in the type info are used to specify the integer type of the object header. It is
   * 0_b4_b3b2_b1b0 (MSB is 0), where:
   *   - b4: the integer type of size. When it is 0/1, `size` is a little-endian 1/4-byte
   *         unsigned integer.
   *   - b3b2: the integer type of ids. When the 2 bits are 0/1/2, the id list contains
   *           1/2/3-byte little-endian unsigned integers.
   *   - b1b0: the integer type of offset. When the 2 bits are 0/1/2, the offset list contains
   *           1/2/3-byte little-endian unsigned integers.
   */
  public static final int OBJECT = 2;
  /**
   * Array value.
   * The content contains a size, a list of field offsets, and the actual element values.
   * It is similar to an object without the id list. The length of the offset list
   * is `size + 1`, where the last offset represent the total size of the element data.
   * Its type info is: 000_b2_b1b0:
   *   - b2: the type of size.
   *   - b1b0: the integer type of offset.
   */
  public static final int ARRAY = 3;

  // The primitive types

  /** JSON Null value. Empty content. */
  public static final int NULL = 0;
  /** True value. Empty content. */
  public static final int TRUE = 1;
  /** False value. Empty content. */
  public static final int FALSE = 2;
  /** 1-byte little-endian signed integer. */
  public static final int INT8 = 3;
  /** 2-byte little-endian signed integer. */
  public static final int INT16 = 4;
  /** 4-byte little-endian signed integer. */
  public static final int INT32 = 5;
  /** 4-byte little-endian signed integer. */
  public static final int INT64 = 6;
  /** 8-byte IEEE double. */
  public static final int DOUBLE = 7;
  /** 4-byte decimal. Content is 1-byte scale + 4-byte little-endian signed integer. */
  public static final int DECIMAL4 = 8;
  /** 8-byte decimal. Content is 1-byte scale + 8-byte little-endian signed integer. */
  public static final int DECIMAL8 = 9;
  /** 16-byte decimal. Content is 1-byte scale + 16-byte little-endian signed integer. */
  public static final int DECIMAL16 = 10;
  /**
   * Date value. Content is 4-byte little-endian signed integer that represents the
   * number of days from the Unix epoch.
   */
  public static final int DATE = 11;
  /**
   * Timestamp value. Content is 8-byte little-endian signed integer that represents the number of
   * microseconds elapsed since the Unix epoch, 1970-01-01 00:00:00 UTC. It is displayed to users in
   * their local time zones and may be displayed differently depending on the execution environment.
   */
  public static final int TIMESTAMP = 12;
  /**
   * Timestamp_ntz value. It has the same content as `TIMESTAMP` but should always be interpreted
   * as if the local time zone is UTC.
   */
  public static final int TIMESTAMP_NTZ = 13;
  /** 4-byte IEEE float. */
  public static final int FLOAT = 14;
  /**
   * Binary value. The content is (4-byte little-endian unsigned integer representing the binary
   * size) + (size bytes of binary content).
   */
  public static final int BINARY = 15;
  /**
   * Long string value. The content is (4-byte little-endian unsigned integer representing the
   * string size) + (size bytes of string content).
   */
  public static final int LONG_STR = 16;
  /**
   * Time value. Values can be from 00:00:00 to 23:59:59.999999.
   * Content is 8-byte little-endian unsigned integer that represents the number of microseconds
   * since midnight.
   */
  public static final int TIME = 17;
  /**
   * Timestamp nanos value. Similar to `TIMESTAMP`, but represents the number of nanoseconds
   * elapsed since the Unix epoch, 1970-01-01 00:00:00 UTC.
   */
  public static final int TIMESTAMP_NANOS = 18;
  /**
   * Timestamp nanos (without timestamp) value. It has the same content as `TIMESTAMP_NANOS` but
   * should always be interpreted as if the local time zone is UTC.
   */
  public static final int TIMESTAMP_NANOS_NTZ = 19;
  /**
   * UUID value. The content is a 16-byte binary, encoded using big-endian.
   * For example, UUID 00112233-4455-6677-8899-aabbccddeeff is encoded as the bytes
   * 00 11 22 33 44 55 66 77 88 99 aa bb cc dd ee ff.
   */
  public static final int UUID = 20;

  // The metadata version.
  public static final byte VERSION = 1;
  // The lower 4 bits of the first metadata byte contain the version.
  public static final byte VERSION_MASK = 0x0F;

  // Constants for various unsigned integer sizes.
  public static final int U8_MAX = 0xFF;
  public static final int U16_MAX = 0xFFFF;
  public static final int U24_MAX = 0xFFFFFF;
  public static final int U8_SIZE = 1;
  public static final int U16_SIZE = 2;
  public static final int U24_SIZE = 3;
  public static final int U32_SIZE = 4;

  // Max decimal precision for each decimal type.
  public static final int MAX_DECIMAL4_PRECISION = 9;
  public static final int MAX_DECIMAL8_PRECISION = 18;
  public static final int MAX_DECIMAL16_PRECISION = 38;

  // The size (in bytes) of a UUID.
  public static final int UUID_SIZE = 16;

  // Default size limit for both variant value and variant metadata.
  public static final int DEFAULT_SIZE_LIMIT = U24_MAX + 1;

  /**
   * Write the least significant `numBytes` bytes in `value` into `bytes[pos, pos + numBytes)` in
   * little endian.
   * @param bytes The byte array to write into
   * @param pos The starting index of the byte array to write into
   * @param value The value to write
   * @param numBytes The number of bytes to write
   */
  public static void writeLong(byte[] bytes, int pos, long value, int numBytes) {
    for (int i = 0; i < numBytes; ++i) {
      bytes[pos + i] = (byte) ((value >>> (8 * i)) & 0xFF);
    }
  }

  public static byte primitiveHeader(int type) {
    return (byte) (type << 2 | PRIMITIVE);
  }

  public static byte shortStrHeader(int size) {
    return (byte) (size << 2 | SHORT_STR);
  }

  public static byte objectHeader(boolean largeSize, int idSize, int offsetSize) {
    return (byte) (((largeSize ? 1 : 0) << (BASIC_TYPE_BITS + 4))
        | ((idSize - 1) << (BASIC_TYPE_BITS + 2))
        | ((offsetSize - 1) << BASIC_TYPE_BITS)
        | OBJECT);
  }

  public static byte arrayHeader(boolean largeSize, int offsetSize) {
    return (byte) (((largeSize ? 1 : 0) << (BASIC_TYPE_BITS + 2)) | ((offsetSize - 1) << BASIC_TYPE_BITS) | ARRAY);
  }

  /**
   * Check the validity of an array index `pos`.
   * @param pos The index to check
   * @param length The length of the array
   * @throws MalformedVariantException if the index is out of bound
   */
  public static void checkIndex(int pos, int length) {
    if (pos < 0 || pos >= length) {
      throw new IllegalArgumentException(
          String.format("Invalid byte-array offset (%d). length: %d", pos, length));
    }
  }

  /**
   * Reads a little-endian signed long value from `bytes[pos, pos + numBytes)`.
   * @param bytes The byte array to read from
   * @param pos The starting index of the byte array to read from
   * @param numBytes The number of bytes to read
   * @return The long value
   */
  static long readLong(byte[] bytes, int pos, int numBytes) {
    checkIndex(pos, bytes.length);
    checkIndex(pos + numBytes - 1, bytes.length);
    long result = 0;
    // All bytes except the most significant byte should be unsigned-extended and shifted
    // (so we need & 0xFF`). The most significant byte should be sign-extended and is handled
    // after the loop.
    for (int i = 0; i < numBytes - 1; ++i) {
      long unsignedByteValue = bytes[pos + i] & 0xFF;
      result |= unsignedByteValue << (8 * i);
    }
    long signedByteValue = bytes[pos + numBytes - 1];
    result |= signedByteValue << (8 * (numBytes - 1));
    return result;
  }

  /**
   * Read a little-endian unsigned int value from `bytes[pos, pos + numBytes)`. The value must fit
   * into a non-negative int (`[0, Integer.MAX_VALUE]`).
   */
  static int readUnsigned(byte[] bytes, int pos, int numBytes) {
    checkIndex(pos, bytes.length);
    checkIndex(pos + numBytes - 1, bytes.length);
    int result = 0;
    // Similar to the `readLong` loop, but all bytes should be unsigned-extended.
    for (int i = 0; i < numBytes; ++i) {
      int unsignedByteValue = bytes[pos + i] & 0xFF;
      result |= unsignedByteValue << (8 * i);
    }
    if (result < 0) {
      throw new MalformedVariantException(String.format("Failed to read unsigned int. numBytes: %d", numBytes));
    }
    return result;
  }

  /**
   * The value type of Variant value. It is determined by the header byte but not a 1:1 mapping
   * (for example, INT1/2/4/8 all maps to `Type.LONG`).
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
    DECIMAL,
    DATE,
    TIMESTAMP,
    TIMESTAMP_NTZ,
    FLOAT,
    BINARY,
    TIME,
    TIMESTAMP_NANOS,
    TIMESTAMP_NANOS_NTZ,
    UUID
  }

  public static int getPrimitiveTypeId(byte[] value, int pos) {
    checkIndex(pos, value.length);
    return (value[pos] >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
  }

  /**
   * Returns the value type of Variant value `value[pos...]`. It is only legal to call `get*` if
   * `getType` returns the corresponding type. For example, it is only legal to call
   * `getLong` if this method returns `Type.Long`.
   * @param value The Variant value to get the type from
   * @param pos The starting index of the Variant value
   * @return The type of the Variant value
   */
  public static Type getType(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    switch (basicType) {
      case SHORT_STR:
        return Type.STRING;
      case OBJECT:
        return Type.OBJECT;
      case ARRAY:
        return Type.ARRAY;
      default:
        switch (typeInfo) {
          case NULL:
            return Type.NULL;
          case TRUE:
          case FALSE:
            return Type.BOOLEAN;
          case INT8:
            return Type.BYTE;
          case INT16:
            return Type.SHORT;
          case INT32:
            return Type.INT;
          case INT64:
            return Type.LONG;
          case DOUBLE:
            return Type.DOUBLE;
          case DECIMAL4:
          case DECIMAL8:
          case DECIMAL16:
            return Type.DECIMAL;
          case DATE:
            return Type.DATE;
          case TIMESTAMP:
            return Type.TIMESTAMP;
          case TIMESTAMP_NTZ:
            return Type.TIMESTAMP_NTZ;
          case FLOAT:
            return Type.FLOAT;
          case BINARY:
            return Type.BINARY;
          case LONG_STR:
            return Type.STRING;
          case TIME:
            return Type.TIME;
          case TIMESTAMP_NANOS:
            return Type.TIMESTAMP_NANOS;
          case TIMESTAMP_NANOS_NTZ:
            return Type.TIMESTAMP_NANOS_NTZ;
          case UUID:
            return Type.UUID;
          default:
            throw new UnknownVariantTypeException(typeInfo);
        }
    }
  }

  /**
   * Computes the actual size (in bytes) of the Variant value at `value[pos...]`.
   * `value.length - pos` is an upper bound of the size, but the actual size may be smaller.
   * @param value The Variant value
   * @param pos The starting index of the Variant value
   * @return The actual size of the Variant value
   * @throws MalformedVariantException if the Variant is malformed
   */
  public static int valueSize(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    switch (basicType) {
      case SHORT_STR:
        return 1 + typeInfo;
      case OBJECT:
        return handleObject(
            value,
            pos,
            (info) -> info.dataStart
                - pos
                + readUnsigned(
                    value, info.offsetStart + info.numElements * info.offsetSize, info.offsetSize));
      case ARRAY:
        return handleArray(
            value,
            pos,
            (info) -> info.dataStart
                - pos
                + readUnsigned(
                    value, info.offsetStart + info.numElements * info.offsetSize, info.offsetSize));
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
          case TIMESTAMP:
          case TIMESTAMP_NTZ:
          case TIME:
          case TIMESTAMP_NANOS:
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
            throw new UnknownVariantTypeException(typeInfo);
        }
    }
  }

  private static MalformedVariantException unexpectedType(Type type) {
    return new MalformedVariantException("Expected type to be " + type);
  }

  public static boolean getBoolean(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != PRIMITIVE || (typeInfo != TRUE && typeInfo != FALSE)) {
      throw unexpectedType(Type.BOOLEAN);
    }
    return typeInfo == TRUE;
  }

  /**
   * Returns a long value from Variant value `value[pos...]`.
   * It is only legal to call it if `getType` returns one of Type.BYTE, SHORT, INT, LONG,
   * DATE, TIMESTAMP, TIMESTAMP_NTZ, TIME, TIMESTAMP_NANOS, TIMESTAMP_NANOS_NTZ.
   * If the type is `DATE`, the return value is guaranteed to fit into an int and
   * represents the number of days from the Unix epoch.
   * If the type is `TIMESTAMP/TIMESTAMP_NTZ`, the return value represents the number of
   * microseconds from the Unix epoch.
   * If the type is `TIME`, the return value represents the number of microseconds since midnight.
   * If the type is `TIMESTAMP_NANOS/TIMESTAMP_NANOS_NTZ`, the return value represents the number of
   * nanoseconds from the Unix epoch.
   * @param value The Variant value
   * @param pos The starting index of the Variant value
   * @return The long value
   */
  public static long getLong(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    String exceptionMessage =
        "Expect type to be one of: BYTE, SHORT, INT, LONG, TIMESTAMP, TIMESTAMP_NTZ, TIME, TIMESTAMP_NANOS, TIMESTAMP_NANOS_NTZ";
    if (basicType != PRIMITIVE) {
      throw new IllegalStateException(exceptionMessage);
    }
    switch (typeInfo) {
      case INT8:
        return readLong(value, pos + 1, 1);
      case INT16:
        return readLong(value, pos + 1, 2);
      case INT32:
      case DATE:
        return readLong(value, pos + 1, 4);
      case INT64:
      case TIMESTAMP:
      case TIMESTAMP_NTZ:
      case TIME:
      case TIMESTAMP_NANOS:
      case TIMESTAMP_NANOS_NTZ:
        return readLong(value, pos + 1, 8);
      default:
        throw new IllegalStateException(exceptionMessage);
    }
  }

  public static double getDouble(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != PRIMITIVE || typeInfo != DOUBLE) {
      throw unexpectedType(Type.DOUBLE);
    }
    return Double.longBitsToDouble(readLong(value, pos + 1, 8));
  }

  /**
   * Checks whether the precision and scale of the decimal are within the limit.
   * @param d The decimal value to check
   * @param maxPrecision The maximum precision allowed
   * @throws MalformedVariantException if the decimal is malformed
   */
  private static void checkDecimal(BigDecimal d, int maxPrecision) {
    if (d.precision() > maxPrecision || d.scale() > maxPrecision) {
      throw new MalformedVariantException(String.format(
          "Decimal (precision: %d, scale: %d) exceeds max precision %d",
          d.precision(), d.scale(), maxPrecision));
    }
  }

  public static BigDecimal getDecimalWithOriginalScale(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != PRIMITIVE) {
      throw unexpectedType(Type.DECIMAL);
    }
    // Interpret the scale byte as unsigned. If it is a negative byte, the unsigned value must be
    // greater than `MAX_DECIMAL16_PRECISION` and will trigger an error in `checkDecimal`.
    int scale = value[pos + 1] & 0xFF;
    BigDecimal result;
    switch (typeInfo) {
      case DECIMAL4:
        result = BigDecimal.valueOf(readLong(value, pos + 2, 4), scale);
        checkDecimal(result, MAX_DECIMAL4_PRECISION);
        break;
      case DECIMAL8:
        result = BigDecimal.valueOf(readLong(value, pos + 2, 8), scale);
        checkDecimal(result, MAX_DECIMAL8_PRECISION);
        break;
      case DECIMAL16:
        checkIndex(pos + 17, value.length);
        byte[] bytes = new byte[16];
        // Copy the bytes reversely because the `BigInteger` constructor expects a big-endian
        // representation.
        for (int i = 0; i < 16; ++i) {
          bytes[i] = value[pos + 17 - i];
        }
        result = new BigDecimal(new BigInteger(bytes), scale);
        checkDecimal(result, MAX_DECIMAL16_PRECISION);
        break;
      default:
        throw unexpectedType(Type.DECIMAL);
    }
    return result;
  }

  public static BigDecimal getDecimal(byte[] value, int pos) {
    return getDecimalWithOriginalScale(value, pos);
  }

  public static float getFloat(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != PRIMITIVE || typeInfo != FLOAT) {
      throw unexpectedType(Type.FLOAT);
    }
    return Float.intBitsToFloat((int) readLong(value, pos + 1, 4));
  }

  public static byte[] getBinary(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != PRIMITIVE || typeInfo != BINARY) {
      throw unexpectedType(Type.BINARY);
    }
    int start = pos + 1 + U32_SIZE;
    int length = readUnsigned(value, pos + 1, U32_SIZE);
    checkIndex(start + length - 1, value.length);
    return Arrays.copyOfRange(value, start, start + length);
  }

  public static String getString(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType == SHORT_STR || (basicType == PRIMITIVE && typeInfo == LONG_STR)) {
      int start;
      int length;
      if (basicType == SHORT_STR) {
        start = pos + 1;
        length = typeInfo;
      } else {
        start = pos + 1 + U32_SIZE;
        length = readUnsigned(value, pos + 1, U32_SIZE);
      }
      checkIndex(start + length - 1, value.length);
      return new String(value, start, length);
    }
    throw unexpectedType(Type.STRING);
  }

  public static java.util.UUID getUUID(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != PRIMITIVE || typeInfo != UUID) {
      throw unexpectedType(Type.UUID);
    }
    int start = pos + 1;
    checkIndex(start + UUID_SIZE - 1, value.length);
    ByteBuffer bb = ByteBuffer.wrap(value, start, UUID_SIZE).order(ByteOrder.BIG_ENDIAN);
    return new java.util.UUID(bb.getLong(), bb.getLong());
  }

  /**
   * A helper class representing the details of a Variant object, used for `ObjectHandler`.
   */
  public static class ObjectInfo {
    /** Number of object fields. */
    public final int numElements;
    /** The integer size of the field id list. */
    public final int idSize;
    /** The integer size of the offset list. */
    public final int offsetSize;
    /** The starting index of the field id list in the variant value array. */
    public final int idStart;
    /** The starting index of the offset list in the variant value array. */
    public final int offsetStart;
    /** The starting index of field data in the variant value array. */
    public final int dataStart;

    public ObjectInfo(int numElements, int idSize, int offsetSize, int idStart, int offsetStart, int dataStart) {
      this.numElements = numElements;
      this.idSize = idSize;
      this.offsetSize = offsetSize;
      this.idStart = idStart;
      this.offsetStart = offsetStart;
      this.dataStart = dataStart;
    }
  }

  /**
   * An interface for the Variant object handler.
   * @param <T> The return type of the handler
   */
  public interface ObjectHandler<T> {
    /**
     * @param objectInfo The details of the Variant object
     */
    T apply(ObjectInfo objectInfo);
  }

  /**
   * An interface for the Variant object handler.
   * @param <T> The return type of the handler
   */
  public interface ObjectHandlerException<T> {
    /**
     * @param objectInfo The details of the Variant object
     */
    T apply(ObjectInfo objectInfo) throws IOException;
  }

  /**
   * A helper function to access a Variant object, at `value[pos...]`.
   * @param value The Variant value
   * @param pos The starting index of the Variant value
   * @param handler The handler to process the object
   * @return The result of the handler
   * @param <T> The return type of the handler
   */
  public static <T> T handleObject(byte[] value, int pos, ObjectHandler<T> handler) {
    ObjectInfo info = parseObject(value, pos);
    return handler.apply(info);
  }

  /**
   * Same as `handleObject` but handler can throw IOException.
   */
  public static <T> T handleObjectException(byte[] value, int pos, ObjectHandlerException<T> handler)
      throws IOException {
    ObjectInfo info = parseObject(value, pos);
    return handler.apply(info);
  }

  /**
   * Parses the object at `value[pos...]`, and returns the object details.
   */
  private static ObjectInfo parseObject(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != OBJECT) {
      throw unexpectedType(Type.OBJECT);
    }
    // Refer to the comment of the `OBJECT` constant for the details of the object header encoding.
    // Suppose `typeInfo` has a bit representation of 0_b4_b3b2_b1b0, the following line extracts
    // b4 to determine whether the object uses a 1/4-byte size.
    boolean largeSize = ((typeInfo >> 4) & 0x1) != 0;
    int sizeBytes = (largeSize ? U32_SIZE : 1);
    int numElements = readUnsigned(value, pos + 1, sizeBytes);
    // Extracts b3b2 to determine the integer size of the field id list.
    int idSize = ((typeInfo >> 2) & 0x3) + 1;
    // Extracts b1b0 to determine the integer size of the offset list.
    int offsetSize = (typeInfo & 0x3) + 1;
    int idStart = pos + 1 + sizeBytes;
    int offsetStart = idStart + numElements * idSize;
    int dataStart = offsetStart + (numElements + 1) * offsetSize;
    return new ObjectInfo(numElements, idSize, offsetSize, idStart, offsetStart, dataStart);
  }

  /**
   * A helper class representing the details of a Variant array, used for `ArrayHandler`.
   */
  public static class ArrayInfo {
    /** Number of object fields. */
    public final int numElements;
    /** The integer size of the offset list. */
    public final int offsetSize;
    /** The starting index of the offset list in the variant value array. */
    public final int offsetStart;
    /** The starting index of field data in the variant value array. */
    public final int dataStart;

    public ArrayInfo(int numElements, int offsetSize, int offsetStart, int dataStart) {
      this.numElements = numElements;
      this.offsetSize = offsetSize;
      this.offsetStart = offsetStart;
      this.dataStart = dataStart;
    }
  }

  /**
   * An interface for the Variant array handler.
   * @param <T> The return type of the handler
   */
  public interface ArrayHandler<T> {
    /**
     * @param arrayInfo The details of the Variant array
     */
    T apply(ArrayInfo arrayInfo);
  }

  /**
   * An interface for the Variant array handler.
   * @param <T> The return type of the handler
   */
  public interface ArrayHandlerException<T> {
    /**
     * @param arrayInfo The details of the Variant array
     */
    T apply(ArrayInfo arrayInfo) throws IOException;
  }

  /**
   * A helper function to access a Variant array, at `value[pos...]`.
   * @param value The Variant value
   * @param pos The starting index of the Variant value
   * @param handler The handler to process the array
   * @return The result of the handler
   * @param <T> The return type of the handler
   */
  public static <T> T handleArray(byte[] value, int pos, ArrayHandler<T> handler) {
    ArrayInfo info = parseArray(value, pos);
    return handler.apply(info);
  }

  /**
   * Same as `handleArray` but handler can throw IOException.
   */
  public static <T> T handleArrayException(byte[] value, int pos, ArrayHandlerException<T> handler)
      throws IOException {
    ArrayInfo info = parseArray(value, pos);
    return handler.apply(info);
  }

  /**
   * Parses the array at `value[pos...]`, and returns the array details.
   */
  private static ArrayInfo parseArray(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & PRIMITIVE_TYPE_MASK;
    if (basicType != ARRAY) {
      throw unexpectedType(Type.ARRAY);
    }
    // Refer to the comment of the `ARRAY` constant for the details of the object header encoding.
    // Suppose `typeInfo` has a bit representation of 000_b2_b1b0, the following line extracts
    // b2 to determine whether the object uses a 1/4-byte size.
    boolean largeSize = ((typeInfo >> 2) & 0x1) != 0;
    int sizeBytes = (largeSize ? U32_SIZE : 1);
    int numElements = readUnsigned(value, pos + 1, sizeBytes);
    // Extracts b1b0 to determine the integer size of the offset list.
    int offsetSize = (typeInfo & 0x3) + 1;
    int offsetStart = pos + 1 + sizeBytes;
    int dataStart = offsetStart + (numElements + 1) * offsetSize;
    return new ArrayInfo(numElements, offsetSize, offsetStart, dataStart);
  }

  /**
   * Returns a key at `id` in the Variant metadata.
   * @param metadata The Variant metadata
   * @param id The key id
   * @return The key
   * @throws MalformedVariantException if the Variant is malformed or if the id is out of bounds
   */
  public static String getMetadataKey(byte[] metadata, int id) {
    checkIndex(0, metadata.length);
    // Extracts the highest 2 bits in the metadata header to determine the integer size of the
    // offset list.
    int offsetSize = ((metadata[0] >> 6) & 0x3) + 1;
    int dictSize = readUnsigned(metadata, 1, offsetSize);
    if (id >= dictSize) {
      throw new MalformedVariantException(
          String.format("Invalid dictionary id: %d. dictionary size: %d", id, dictSize));
    }
    // There are a header byte, a `dictSize` with `offsetSize` bytes, and `(dictSize + 1)` offsets
    // before the string data.
    int stringStart = 1 + (dictSize + 2) * offsetSize;
    int offset = readUnsigned(metadata, 1 + (id + 1) * offsetSize, offsetSize);
    int nextOffset = readUnsigned(metadata, 1 + (id + 2) * offsetSize, offsetSize);
    if (offset > nextOffset) {
      throw new MalformedVariantException(
          String.format("Invalid offset: %d. next offset: %d", offset, nextOffset));
    }
    checkIndex(stringStart + nextOffset - 1, metadata.length);
    return new String(metadata, stringStart + offset, nextOffset - offset);
  }
}
