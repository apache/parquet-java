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
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.exc.InputCoercionException;
import java.io.IOException;
import java.math.BigDecimal;

/**
 * Parses JSON into {@link Variant} values using Jackson streaming.
 *
 * <p>This class isolates the Jackson dependency from {@link VariantBuilder},
 * so that core variant construction does not require Jackson on the classpath.
 *
 * <p>Ported from Apache Spark's {@code VariantBuilder.parseJson}.
 */
public final class VariantJsonParser {

  private static final JsonFactory JSON_FACTORY = JsonFactory.builder()
      .streamReadConstraints(StreamReadConstraints.builder()
          .maxNestingDepth(500)
          .maxStringLength(10_000_000)
          .maxDocumentLength(50_000_000L)
          .build())
      .build();

  private VariantJsonParser() {}

  /**
   * Parses a JSON string and returns the corresponding {@link Variant}.
   *
   * <p>Uses Jackson streaming parser for single-pass conversion
   * with no intermediate tree. Number handling preserves precision:
   * integers use the smallest fitting type, floating-point numbers
   * prefer decimal encoding (no scientific notation) and fall back
   * to double.
   *
   * @param json the JSON string to parse
   * @return the parsed Variant
   * @throws IOException if the JSON is malformed or an I/O error occurs
   */
  public static Variant parseJson(String json) throws IOException {
    try (JsonParser parser = JSON_FACTORY.createParser(json)) {
      parser.nextToken();
      return parseJson(parser);
    }
  }

  /**
   * Parses a JSON value from an already-positioned {@link JsonParser}
   * and returns the corresponding {@link Variant}. The parser must
   * have its current token set (i.e., {@code parser.nextToken()}
   * or equivalent must have been called).
   *
   * @param parser a positioned Jackson JsonParser
   * @return the parsed Variant
   * @throws IOException if the JSON is malformed or an I/O error occurs
   */
  public static Variant parseJson(JsonParser parser) throws IOException {
    VariantBuilder builder = new VariantBuilder();
    buildJson(builder, parser);
    return builder.build();
  }

  /**
   * Recursively builds a Variant value from the current position of a
   * Jackson streaming parser. Handles objects, arrays, strings, numbers
   * (int/long/decimal/double), booleans, and null.
   */
  private static void buildJson(VariantBuilder builder, JsonParser parser) throws IOException {
    JsonToken token = parser.currentToken();
    if (token == null) {
      throw new JsonParseException(parser, "Unexpected null token");
    }
    switch (token) {
      case START_OBJECT:
        buildJsonObject(builder, parser);
        break;
      case START_ARRAY:
        buildJsonArray(builder, parser);
        break;
      case VALUE_STRING:
        builder.appendString(parser.getText());
        break;
      case VALUE_NUMBER_INT:
        buildJsonInteger(builder, parser);
        break;
      case VALUE_NUMBER_FLOAT:
        buildJsonFloat(builder, parser);
        break;
      case VALUE_TRUE:
        builder.appendBoolean(true);
        break;
      case VALUE_FALSE:
        builder.appendBoolean(false);
        break;
      case VALUE_NULL:
        builder.appendNull();
        break;
      default:
        throw new JsonParseException(parser, "Unexpected token " + token);
    }
  }

  private static void buildJsonObject(VariantBuilder builder, JsonParser parser) throws IOException {
    VariantObjectBuilder obj = builder.startObject();
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      obj.appendKey(parser.currentName());
      parser.nextToken();
      buildJson(obj, parser);
    }
    builder.endObject();
  }

  private static void buildJsonArray(VariantBuilder builder, JsonParser parser) throws IOException {
    VariantArrayBuilder arr = builder.startArray();
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      buildJson(arr, parser);
    }
    builder.endArray();
  }

  private static void buildJsonInteger(VariantBuilder builder, JsonParser parser) throws IOException {
    try {
      appendSmallestLong(builder, parser.getLongValue());
    } catch (InputCoercionException ignored) {
      buildJsonFloat(builder, parser);
    }
  }

  private static void buildJsonFloat(VariantBuilder builder, JsonParser parser) throws IOException {
    if (!tryAppendDecimal(builder, parser.getText())) {
      builder.appendDouble(parser.getDoubleValue());
    }
  }

  /**
   * Appends a long value using the smallest integer type that fits.
   */
  private static void appendSmallestLong(VariantBuilder builder, long l) {
    if (l == (byte) l) {
      builder.appendByte((byte) l);
    } else if (l == (short) l) {
      builder.appendShort((short) l);
    } else if (l == (int) l) {
      builder.appendInt((int) l);
    } else {
      builder.appendLong(l);
    }
  }

  /**
   * Tries to parse a number string as a decimal. Only accepts plain
   * decimal format (digits, minus, dot -- no scientific notation).
   * Returns true if the number was successfully appended as a decimal.
   */
  private static boolean tryAppendDecimal(VariantBuilder builder, String input) {
    for (int i = 0; i < input.length(); i++) {
      char ch = input.charAt(i);
      if (ch != '-' && ch != '.' && !(ch >= '0' && ch <= '9')) {
        return false;
      }
    }
    BigDecimal d = new BigDecimal(input);
    if (d.scale() <= VariantUtil.MAX_DECIMAL16_PRECISION
        && d.precision() <= VariantUtil.MAX_DECIMAL16_PRECISION) {
      builder.appendDecimal(d);
      return true;
    }
    return false;
  }
}
