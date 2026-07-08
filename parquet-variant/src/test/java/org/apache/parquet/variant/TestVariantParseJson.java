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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import java.io.IOException;
import java.math.BigDecimal;
import org.junit.Test;

public class TestVariantParseJson {

  @Test
  public void testParseNull() throws IOException {
    Variant v = VariantJsonParser.parseJson("null");
    assertThat(v.getType()).isEqualTo(Variant.Type.NULL);
  }

  @Test
  public void testParseTrue() throws IOException {
    Variant v = VariantJsonParser.parseJson("true");
    assertThat(v.getType()).isEqualTo(Variant.Type.BOOLEAN);
    assertThat(v.getBoolean()).isTrue();
  }

  @Test
  public void testParseFalse() throws IOException {
    Variant v = VariantJsonParser.parseJson("false");
    assertThat(v.getType()).isEqualTo(Variant.Type.BOOLEAN);
    assertThat(v.getBoolean()).isFalse();
  }

  @Test
  public void testParseString() throws IOException {
    Variant v = VariantJsonParser.parseJson("\"hello world\"");
    assertThat(v.getType()).isEqualTo(Variant.Type.STRING);
    assertThat(v.getString()).isEqualTo("hello world");
  }

  @Test
  public void testParseSmallInteger() throws IOException {
    Variant v = VariantJsonParser.parseJson("42");
    assertThat(v.getType()).isEqualTo(Variant.Type.BYTE);
    assertThat(v.getLong()).isEqualTo(42);
  }

  @Test
  public void testParseShortInteger() throws IOException {
    Variant v = VariantJsonParser.parseJson("1000");
    assertThat(v.getType()).isEqualTo(Variant.Type.SHORT);
    assertThat(v.getLong()).isEqualTo(1000);
  }

  @Test
  public void testParseIntInteger() throws IOException {
    Variant v = VariantJsonParser.parseJson("100000");
    assertThat(v.getType()).isEqualTo(Variant.Type.INT);
    assertThat(v.getLong()).isEqualTo(100000);
  }

  @Test
  public void testParseLongInteger() throws IOException {
    Variant v = VariantJsonParser.parseJson("9999999999");
    assertThat(v.getType()).isEqualTo(Variant.Type.LONG);
    assertThat(v.getLong()).isEqualTo(9999999999L);
  }

  @Test
  public void testParseDecimalFloat() throws IOException {
    Variant v = VariantJsonParser.parseJson("3.14");
    Variant.Type type = v.getType();
    assertThat(type).isIn(Variant.Type.DECIMAL4, Variant.Type.DECIMAL8, Variant.Type.DECIMAL16);
    assertThat(v.getDecimal()).isEqualByComparingTo(new BigDecimal("3.14"));
  }

  @Test
  public void testParseScientificNotationDouble() throws IOException {
    Variant v = VariantJsonParser.parseJson("1.5e10");
    assertThat(v.getType()).isEqualTo(Variant.Type.DOUBLE);
    assertThat(v.getDouble()).isCloseTo(1.5e10, within(0.001));
  }

  @Test
  public void testParseLargeIntegerAsDecimal() throws IOException {
    String bigNum = "99999999999999999999";
    Variant v = VariantJsonParser.parseJson(bigNum);
    Variant.Type type = v.getType();
    assertThat(type).isIn(Variant.Type.DECIMAL4, Variant.Type.DECIMAL8, Variant.Type.DECIMAL16);
    assertThat(v.getDecimal()).isEqualByComparingTo(new BigDecimal(bigNum));
  }

  @Test
  public void testParseNegativeInteger() throws IOException {
    Variant v = VariantJsonParser.parseJson("-100");
    assertThat(v.getLong()).isEqualTo(-100);
  }

  @Test
  public void testParseNegativeDecimal() throws IOException {
    Variant v = VariantJsonParser.parseJson("-99.99");
    assertThat(v.getDecimal()).isEqualByComparingTo(new BigDecimal("-99.99"));
  }

  @Test
  public void testParseZero() throws IOException {
    Variant v = VariantJsonParser.parseJson("0");
    assertThat(v.getType()).isEqualTo(Variant.Type.BYTE);
    assertThat(v.getLong()).isEqualTo(0);
  }

  @Test
  public void testParseEmptyObject() throws IOException {
    Variant v = VariantJsonParser.parseJson("{}");
    assertThat(v.getType()).isEqualTo(Variant.Type.OBJECT);
    assertThat(v.numObjectElements()).isEqualTo(0);
  }

  @Test
  public void testParseSimpleObject() throws IOException {
    Variant v = VariantJsonParser.parseJson("{\"name\":\"John\",\"age\":30}");
    assertThat(v.getType()).isEqualTo(Variant.Type.OBJECT);
    assertThat(v.numObjectElements()).isEqualTo(2);
    assertThat(v.getFieldByKey("name").getString()).isEqualTo("John");
    assertThat(v.getFieldByKey("age").getLong()).isEqualTo(30);
  }

  @Test
  public void testParseNestedObject() throws IOException {
    Variant v = VariantJsonParser.parseJson("{\"user\":{\"id\":100,\"country\":\"US\"},\"active\":true}");
    assertThat(v.getType()).isEqualTo(Variant.Type.OBJECT);
    Variant user = v.getFieldByKey("user");
    assertThat(user.getType()).isEqualTo(Variant.Type.OBJECT);
    assertThat(user.getFieldByKey("id").getLong()).isEqualTo(100);
    assertThat(user.getFieldByKey("country").getString()).isEqualTo("US");
    assertThat(v.getFieldByKey("active").getBoolean()).isTrue();
  }

  @Test
  public void testParseEmptyArray() throws IOException {
    Variant v = VariantJsonParser.parseJson("[]");
    assertThat(v.getType()).isEqualTo(Variant.Type.ARRAY);
    assertThat(v.numArrayElements()).isEqualTo(0);
  }

  @Test
  public void testParseSimpleArray() throws IOException {
    Variant v = VariantJsonParser.parseJson("[1,2,3,\"four\"]");
    assertThat(v.getType()).isEqualTo(Variant.Type.ARRAY);
    assertThat(v.numArrayElements()).isEqualTo(4);
    assertThat(v.getElementAtIndex(0).getLong()).isEqualTo(1);
    assertThat(v.getElementAtIndex(1).getLong()).isEqualTo(2);
    assertThat(v.getElementAtIndex(2).getLong()).isEqualTo(3);
    assertThat(v.getElementAtIndex(3).getString()).isEqualTo("four");
  }

  @Test
  public void testParseNestedArray() throws IOException {
    Variant v = VariantJsonParser.parseJson("[[1,2],[3,4]]");
    assertThat(v.getType()).isEqualTo(Variant.Type.ARRAY);
    assertThat(v.numArrayElements()).isEqualTo(2);
    Variant inner = v.getElementAtIndex(0);
    assertThat(inner.getType()).isEqualTo(Variant.Type.ARRAY);
    assertThat(inner.getElementAtIndex(0).getLong()).isEqualTo(1);
    assertThat(inner.getElementAtIndex(1).getLong()).isEqualTo(2);
  }

  @Test
  public void testParseMixedArray() throws IOException {
    Variant v = VariantJsonParser.parseJson("[1,\"two\",true,null,3.14]");
    assertThat(v.getType()).isEqualTo(Variant.Type.ARRAY);
    assertThat(v.numArrayElements()).isEqualTo(5);
    assertThat(v.getElementAtIndex(0).getLong()).isEqualTo(1);
    assertThat(v.getElementAtIndex(1).getString()).isEqualTo("two");
    assertThat(v.getElementAtIndex(2).getBoolean()).isTrue();
    assertThat(v.getElementAtIndex(3).getType()).isEqualTo(Variant.Type.NULL);
    assertThat(v.getElementAtIndex(4).getDecimal()).isEqualByComparingTo(new BigDecimal("3.14"));
  }

  @Test
  public void testParseObjectWithNullValue() throws IOException {
    Variant v = VariantJsonParser.parseJson("{\"key\":null}");
    assertThat(v.getType()).isEqualTo(Variant.Type.OBJECT);
    assertThat(v.getFieldByKey("key").getType()).isEqualTo(Variant.Type.NULL);
  }

  @Test
  public void testParseComplexDocument() throws IOException {
    String json = "{\"userId\":12345,\"events\":["
        + "{\"eType\":\"login\",\"ts\":\"2026-01-15T10:30:00Z\"},"
        + "{\"eType\":\"purchase\",\"amount\":99.99}"
        + "]}";
    Variant v = VariantJsonParser.parseJson(json);
    assertThat(v.getType()).isEqualTo(Variant.Type.OBJECT);
    assertThat(v.getFieldByKey("userId").getLong()).isEqualTo(12345);
    Variant events = v.getFieldByKey("events");
    assertThat(events.getType()).isEqualTo(Variant.Type.ARRAY);
    assertThat(events.numArrayElements()).isEqualTo(2);
    assertThat(events.getElementAtIndex(0).getFieldByKey("eType").getString())
        .isEqualTo("login");
    assertThat(events.getElementAtIndex(1).getFieldByKey("amount").getDecimal())
        .isEqualByComparingTo(new BigDecimal("99.99"));
  }

  @Test
  public void testParseEmptyString() throws IOException {
    Variant v = VariantJsonParser.parseJson("\"\"");
    assertThat(v.getType()).isEqualTo(Variant.Type.STRING);
    assertThat(v.getString()).isEqualTo("");
  }

  @Test
  public void testParseUnicodeString() throws IOException {
    Variant v = VariantJsonParser.parseJson("\"\\u00e9l\\u00e8ve\"");
    assertThat(v.getType()).isEqualTo(Variant.Type.STRING);
    assertThat(v.getString()).isEqualTo("\u00e9l\u00e8ve");
  }

  @Test
  public void testParseUnicodeKey() throws IOException {
    Variant v = VariantJsonParser.parseJson("{\"\\u00e9l\\u00e8ve\": 42}");
    assertThat(v.getType()).isEqualTo(Variant.Type.OBJECT);
    Variant value = v.getFieldByKey("élève");
    assertThat(value).isNotNull();
    assertThat(value.getInt()).isEqualTo(42);
  }

  @Test
  public void testParseEscapedString() throws IOException {
    Variant v = VariantJsonParser.parseJson("\"hello\\nworld\"");
    assertThat(v.getType()).isEqualTo(Variant.Type.STRING);
    assertThat(v.getString()).isEqualTo("hello\nworld");
  }

  @Test
  public void testParseMalformedJson() {
    assertThatThrownBy(() -> VariantJsonParser.parseJson("{invalid"))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("was expecting double-quote to start field name");
  }

  @Test
  public void testParseIncompleteObject() {
    assertThatThrownBy(() -> VariantJsonParser.parseJson("{\"key\":"))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Unexpected end-of-input within/between Object entries");
  }

  @Test
  public void testParseJsonWithParser() throws IOException {
    JsonFactory factory = new JsonFactory();
    try (JsonParser parser = factory.createParser("{\"a\":1}")) {
      parser.nextToken();
      Variant v = VariantJsonParser.parseJson(parser);
      assertThat(v.getType()).isEqualTo(Variant.Type.OBJECT);
      assertThat(v.getFieldByKey("a").getLong()).isEqualTo(1);
    }
  }

  @Test
  public void testParseDuplicateKeysLastWins() throws IOException {
    Variant v = VariantJsonParser.parseJson("{\"k\":1,\"k\":2}");
    assertThat(v.getType()).isEqualTo(Variant.Type.OBJECT);
    assertThat(v.numObjectElements()).isEqualTo(1);
    assertThat(v.getFieldByKey("k").getLong()).isEqualTo(2);
  }

  @Test
  public void testParseDeeplyNested() throws IOException {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 20; i++) {
      sb.append("{\"n\":");
    }
    sb.append("42");
    for (int i = 0; i < 20; i++) {
      sb.append("}");
    }
    Variant v = VariantJsonParser.parseJson(sb.toString());
    for (int i = 0; i < 20; i++) {
      assertThat(v.getType()).isEqualTo(Variant.Type.OBJECT);
      v = v.getFieldByKey("n");
    }
    assertThat(v.getLong()).isEqualTo(42);
  }

  @Test
  public void testObjectKeysSorted() throws IOException {
    Variant v = VariantJsonParser.parseJson("{\"c\":3,\"a\":1,\"b\":2}");
    assertThat(v.getType()).isEqualTo(Variant.Type.OBJECT);
    assertThat(v.numObjectElements()).isEqualTo(3);
    assertThat(v.getFieldAtIndex(0).key).isEqualTo("a");
    assertThat(v.getFieldAtIndex(1).key).isEqualTo("b");
    assertThat(v.getFieldAtIndex(2).key).isEqualTo("c");
  }

  @Test
  public void testParseEmptyInput() {
    assertThatThrownBy(() -> VariantJsonParser.parseJson(""))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Unexpected null token");
  }

  @Test
  public void testParseNotJson() {
    assertThatThrownBy(() -> VariantJsonParser.parseJson("not json at all"))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Unrecognized token 'not'");
  }

  @Test
  public void testParseIncompleteArray() {
    assertThatThrownBy(() -> VariantJsonParser.parseJson("[1, 2,"))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Unexpected end-of-input within/between Array entries");
  }

  @Test
  public void testParseLargeJsonWithManyValues() throws IOException {
    StringBuilder sb = new StringBuilder("{");
    int numKeys = 1000;
    for (int i = 0; i < numKeys; i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("\"key").append(i).append("\":").append(i);
    }
    sb.append("}");

    Variant v = VariantJsonParser.parseJson(sb.toString());
    assertThat(v.getType()).isEqualTo(Variant.Type.OBJECT);
    assertThat(v.numObjectElements()).isEqualTo(numKeys);
    // Spot-check a few values
    assertThat(v.getFieldByKey("key0").getLong()).isEqualTo(0);
    assertThat(v.getFieldByKey("key500").getLong()).isEqualTo(500);
    assertThat(v.getFieldByKey("key999").getLong()).isEqualTo(999);
  }
}
