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
package org.apache.parquet.internal.column.columnindex;

import static org.apache.parquet.schema.OriginalType.BSON;
import static org.apache.parquet.schema.OriginalType.DECIMAL;
import static org.apache.parquet.schema.OriginalType.ENUM;
import static org.apache.parquet.schema.OriginalType.INTERVAL;
import static org.apache.parquet.schema.OriginalType.JSON;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Random;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveStringifier;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link BinaryTruncator}
 */
public class TestBinaryTruncator {

  private static final Logger LOG = LoggerFactory.getLogger(TestBinaryTruncator.class);
  private static final PrimitiveStringifier HEXA_STRINGIFIER =
      Types.required(BINARY).named("dummy_type").stringifier();
  private static final Random RANDOM = new Random(42);
  private static final CharsetDecoder UTF8_DECODER = StandardCharsets.UTF_8.newDecoder();

  static {
    UTF8_DECODER.onMalformedInput(CodingErrorAction.REPORT);
    UTF8_DECODER.onUnmappableCharacter(CodingErrorAction.REPORT);
  }

  // The maximum values in UTF-8 for the 1, 2, 3 and 4 bytes representations
  private static final String UTF8_1BYTE_MAX_CHAR = "\u007F";
  private static final String UTF8_2BYTES_MAX_CHAR = "\u07FF";
  private static final String UTF8_3BYTES_MAX_CHAR = "\uFFFF";
  private static final String UTF8_4BYTES_MAX_CHAR = "\uDBFF\uDFFF";

  @Test
  public void testNonStringTruncate() {
    BinaryTruncator truncator = BinaryTruncator.getTruncator(
        Types.required(BINARY).as(DECIMAL).precision(10).scale(2).named("test_binary_decimal"));
    assertThat(truncator.truncateMin(binary(0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA), 2))
        .isEqualTo(binary(0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA));
    assertThat(truncator.truncateMax(binary(0x01, 0x02, 0x03, 0x04, 0x05, 0x06), 2))
        .isEqualTo(binary(0x01, 0x02, 0x03, 0x04, 0x05, 0x06));
  }

  @Test
  public void testContractNonStringTypes() {
    testTruncator(
        Types.required(FIXED_LEN_BYTE_ARRAY)
            .length(8)
            .as(DECIMAL)
            .precision(18)
            .scale(4)
            .named("test_fixed_decimal"),
        false);
    testTruncator(
        Types.required(FIXED_LEN_BYTE_ARRAY).length(12).as(INTERVAL).named("test_fixed_interval"), false);
    testTruncator(Types.required(BINARY).as(DECIMAL).precision(10).scale(2).named("test_binary_decimal"), false);
    testTruncator(Types.required(INT96).named("test_int96"), false);
  }

  @Test
  public void testStringTruncate() {
    BinaryTruncator truncator =
        BinaryTruncator.getTruncator(Types.required(BINARY).as(UTF8).named("test_utf8"));

    // Truncate 1 byte characters
    assertThat(truncator.truncateMin(Binary.fromString("abcdef"), 3)).isEqualTo(Binary.fromString("abc"));
    assertThat(truncator.truncateMax(Binary.fromString("abcdef"), 3)).isEqualTo(Binary.fromString("abd"));

    // Truncate 1-2 bytes characters; the target length is "inside" a UTF-8 character
    assertThat(truncator.truncateMin(Binary.fromString("árvíztűrő"), 9)).isEqualTo(Binary.fromString("árvízt"));
    assertThat(truncator.truncateMax(Binary.fromString("árvíztűrő"), 9)).isEqualTo(Binary.fromString("árvízu"));

    // Truncate highest UTF-8 values -> unable to increment
    assertThat(truncator.truncateMin(
            Binary.fromString(UTF8_1BYTE_MAX_CHAR
                + UTF8_2BYTES_MAX_CHAR
                + UTF8_3BYTES_MAX_CHAR
                + UTF8_4BYTES_MAX_CHAR),
            5))
        .isEqualTo(Binary.fromString(UTF8_1BYTE_MAX_CHAR + UTF8_2BYTES_MAX_CHAR));
    assertThat(truncator.truncateMax(
            Binary.fromString(UTF8_1BYTE_MAX_CHAR
                + UTF8_2BYTES_MAX_CHAR
                + UTF8_3BYTES_MAX_CHAR
                + UTF8_4BYTES_MAX_CHAR),
            5))
        .isEqualTo(Binary.fromString(
            UTF8_1BYTE_MAX_CHAR + UTF8_2BYTES_MAX_CHAR + UTF8_3BYTES_MAX_CHAR + UTF8_4BYTES_MAX_CHAR));

    // Truncate highest UTF-8 values at the end -> increment the first possible character
    assertThat(truncator.truncateMax(
            Binary.fromString(UTF8_1BYTE_MAX_CHAR
                + UTF8_2BYTES_MAX_CHAR
                + "a"
                + UTF8_3BYTES_MAX_CHAR
                + UTF8_4BYTES_MAX_CHAR),
            10))
        .isEqualTo(Binary.fromString(UTF8_1BYTE_MAX_CHAR + UTF8_2BYTES_MAX_CHAR + "b" + UTF8_3BYTES_MAX_CHAR));

    // Truncate invalid UTF-8 values -> truncate without validity check
    assertThat(truncator.truncateMin(binary(0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA), 3))
        .isEqualTo(binary(0xFF, 0xFE, 0xFD));
    assertThat(truncator.truncateMax(binary(0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA), 3))
        .isEqualTo(binary(0xFF, 0xFE, 0xFE));
    assertThat(truncator.truncateMax(binary(0xFF, 0xFE, 0xFD, 0xFF, 0xFF, 0xFF), 5))
        .isEqualTo(binary(0xFF, 0xFE, 0xFE, 0x00, 0x00));
  }

  @Test
  public void testContractStringTypes() {
    testTruncator(Types.required(BINARY).named("test_binary"), true);
    testTruncator(Types.required(BINARY).as(UTF8).named("test_utf8"), true);
    testTruncator(Types.required(BINARY).as(ENUM).named("test_enum"), true);
    testTruncator(Types.required(BINARY).as(JSON).named("test_json"), true);
    testTruncator(Types.required(BINARY).as(BSON).named("test_bson"), true);
    testTruncator(Types.required(FIXED_LEN_BYTE_ARRAY).length(5).named("test_fixed"), true);
  }

  private void testTruncator(PrimitiveType type, boolean strict) {
    BinaryTruncator truncator = BinaryTruncator.getTruncator(type);
    Comparator<Binary> comparator = type.comparator();

    checkContract(truncator, comparator, Binary.fromString("aaaaaaaaaa"), strict, strict);
    checkContract(truncator, comparator, Binary.fromString("árvíztűrő tükörfúrógép"), strict, strict);
    checkContract(truncator, comparator, Binary.fromString("aaaaaaaaaa" + UTF8_3BYTES_MAX_CHAR), strict, strict);
    checkContract(
        truncator,
        comparator,
        Binary.fromString("a" + UTF8_3BYTES_MAX_CHAR + UTF8_1BYTE_MAX_CHAR),
        strict,
        strict);

    checkContract(
        truncator,
        comparator,
        Binary.fromConstantByteArray(new byte[] {(byte) 0xFE, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, (byte) 0xFF}),
        strict,
        strict);

    // Edge case: zero length -> unable to truncate
    checkContract(truncator, comparator, Binary.fromString(""), false, false);
    // Edge case: containing only UTF-8 max characters -> unable to truncate for max
    checkContract(
        truncator,
        comparator,
        Binary.fromString(UTF8_1BYTE_MAX_CHAR
            + UTF8_4BYTES_MAX_CHAR
            + UTF8_3BYTES_MAX_CHAR
            + UTF8_4BYTES_MAX_CHAR
            + UTF8_2BYTES_MAX_CHAR
            + UTF8_3BYTES_MAX_CHAR
            + UTF8_3BYTES_MAX_CHAR
            + UTF8_1BYTE_MAX_CHAR
            + UTF8_2BYTES_MAX_CHAR
            + UTF8_3BYTES_MAX_CHAR
            + UTF8_4BYTES_MAX_CHAR),
        strict,
        false);
    // Edge case: non-UTF-8; max bytes -> unable to truncate for max
    checkContract(
        truncator,
        comparator,
        binary(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF),
        strict,
        false);
  }

  // Checks the contract of truncator
  // strict means actual truncation is required and the truncated value is a valid UTF-8 string
  private void checkContract(
      BinaryTruncator truncator,
      Comparator<Binary> comparator,
      Binary value,
      boolean strictMin,
      boolean strictMax) {
    int length = value.length();

    // Edge cases: returning the original value if no truncation is required
    assertThat(truncator.truncateMin(value, length)).isSameAs(value);
    assertThat(truncator.truncateMax(value, length)).isSameAs(value);
    assertThat(truncator.truncateMin(value, random(length + 1, length * 2 + 1)))
        .isSameAs(value);
    assertThat(truncator.truncateMax(value, random(length + 1, length * 2 + 1)))
        .isSameAs(value);

    if (length > 1) {
      checkMinContract(truncator, comparator, value, length - 1, strictMin);
      checkMaxContract(truncator, comparator, value, length - 1, strictMax);
      checkMinContract(truncator, comparator, value, random(1, length - 1), strictMin);
      checkMaxContract(truncator, comparator, value, random(1, length - 1), strictMax);
    }

    // Edge case: possible to truncate min value to 0 length if original value is not empty
    checkMinContract(truncator, comparator, value, 0, strictMin);
    // Edge case: impossible to truncate max value to 0 length -> returning the original value
    assertThat(truncator.truncateMax(value, 0)).isSameAs(value);
  }

  private void checkMinContract(
      BinaryTruncator truncator, Comparator<Binary> comparator, Binary value, int length, boolean strict) {
    Binary truncated = truncator.truncateMin(value, length);
    LOG.debug(
        "\"{}\" --truncMin({})--> \"{}\" [{}]",
        value.toStringUsingUTF8(),
        length,
        truncated.toStringUsingUTF8(),
        HEXA_STRINGIFIER.stringify(truncated));
    assertThat(truncated)
        .as("truncatedMin(value) should be <= than value")
        .usingComparator(comparator)
        .isLessThanOrEqualTo(value);
    assertThat(truncated.length())
        .as("length of truncateMin(value) should not be > than the length of value")
        .isLessThanOrEqualTo(value.length());
    if (isValidUtf8(value)) {
      checkValidUtf8(truncated);
    }
    if (strict) {
      assertThat(truncated.length())
          .as("length of truncateMin(value) ahould be < than the length of value")
          .isLessThan(value.length());
    }
  }

  private void checkMaxContract(
      BinaryTruncator truncator, Comparator<Binary> comparator, Binary value, int length, boolean strict) {
    Binary truncated = truncator.truncateMax(value, length);
    LOG.debug(
        "\"{}\" --truncMax({})--> \"{}\" [{}]",
        value.toStringUsingUTF8(),
        length,
        truncated.toStringUsingUTF8(),
        HEXA_STRINGIFIER.stringify(truncated));
    assertThat(truncated)
        .as("truncatedMax(value) should be >= than value")
        .usingComparator(comparator)
        .isGreaterThanOrEqualTo(value);
    assertThat(truncated.length())
        .as("length of truncateMax(value) should not be > than the length of value")
        .isLessThanOrEqualTo(value.length());
    if (isValidUtf8(value)) {
      checkValidUtf8(truncated);
    }
    if (strict) {
      assertThat(truncated.length())
          .as("length of truncateMax(value) ahould be < than the length of value")
          .isLessThan(value.length());
    }
  }

  private static boolean isValidUtf8(Binary binary) {
    try {
      UTF8_DECODER.decode(binary.toByteBuffer());
      return true;
    } catch (CharacterCodingException e) {
      return false;
    }
  }

  private static void checkValidUtf8(Binary binary) {
    try {
      UTF8_DECODER.decode(binary.toByteBuffer());
    } catch (CharacterCodingException e) {
      throw new AssertionError("Truncated value should be a valid UTF-8 string", e);
    }
  }

  private static int random(int min, int max) {
    return RANDOM.nextInt(max - min + 1) + min;
  }

  private static Binary binary(int... unsignedBytes) {
    byte[] byteArray = new byte[unsignedBytes.length];
    for (int i = 0, n = byteArray.length; i < n; ++i) {
      int b = unsignedBytes[i];
      assert (0xFFFFFF00 & b) == 0;
      byteArray[i] = (byte) b;
    }
    return Binary.fromConstantByteArray(byteArray);
  }
}
