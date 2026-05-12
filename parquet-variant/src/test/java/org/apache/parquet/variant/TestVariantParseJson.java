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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import java.io.IOException;
import java.math.BigDecimal;
import org.junit.Assert;
import org.junit.Test;

public class TestVariantParseJson {

  @Test
  public void testParseNull() throws IOException {
    Variant v = VariantJsonParser.parseJson("null");
    Assert.assertEquals(Variant.Type.NULL, v.getType());
  }

  @Test
  public void testParseTrue() throws IOException {
    Variant v = VariantJsonParser.parseJson("true");
    Assert.assertEquals(Variant.Type.BOOLEAN, v.getType());
    Assert.assertTrue(v.getBoolean());
  }

  @Test
  public void testParseFalse() throws IOException {
    Variant v = VariantJsonParser.parseJson("false");
    Assert.assertEquals(Variant.Type.BOOLEAN, v.getType());
    Assert.assertFalse(v.getBoolean());
  }

  @Test
  public void testParseString() throws IOException {
    Variant v = VariantJsonParser.parseJson("\"hello world\"");
    Assert.assertEquals(Variant.Type.STRING, v.getType());
    Assert.assertEquals("hello world", v.getString());
  }

  @Test
  public void testParseSmallInteger() throws IOException {
    Variant v = VariantJsonParser.parseJson("42");
    Assert.assertEquals(Variant.Type.BYTE, v.getType());
    Assert.assertEquals(42, v.getLong());
  }

  @Test
  public void testParseShortInteger() throws IOException {
    Variant v = VariantJsonParser.parseJson("1000");
    Assert.assertEquals(Variant.Type.SHORT, v.getType());
    Assert.assertEquals(1000, v.getLong());
  }

  @Test
  public void testParseIntInteger() throws IOException {
    Variant v = VariantJsonParser.parseJson("100000");
    Assert.assertEquals(Variant.Type.INT, v.getType());
    Assert.assertEquals(100000, v.getLong());
  }

  @Test
  public void testParseLongInteger() throws IOException {
    Variant v = VariantJsonParser.parseJson("9999999999");
    Assert.assertEquals(Variant.Type.LONG, v.getType());
    Assert.assertEquals(9999999999L, v.getLong());
  }

  @Test
  public void testParseDecimalFloat() throws IOException {
    Variant v = VariantJsonParser.parseJson("3.14");
    Variant.Type type = v.getType();
    Assert.assertTrue(
        "Expected decimal type, got " + type,
        type == Variant.Type.DECIMAL4 || type == Variant.Type.DECIMAL8 || type == Variant.Type.DECIMAL16);
    Assert.assertEquals(0, new BigDecimal("3.14").compareTo(v.getDecimal()));
  }

  @Test
  public void testParseScientificNotationDouble() throws IOException {
    Variant v = VariantJsonParser.parseJson("1.5e10");
    Assert.assertEquals(Variant.Type.DOUBLE, v.getType());
    Assert.assertEquals(1.5e10, v.getDouble(), 0.001);
  }

  @Test
  public void testParseLargeIntegerAsDecimal() throws IOException {
    String bigNum = "99999999999999999999";
    Variant v = VariantJsonParser.parseJson(bigNum);
    Variant.Type type = v.getType();
    Assert.assertTrue(
        "Expected decimal type for big integer, got " + type,
        type == Variant.Type.DECIMAL4 || type == Variant.Type.DECIMAL8 || type == Variant.Type.DECIMAL16);
    Assert.assertEquals(0, new BigDecimal(bigNum).compareTo(v.getDecimal()));
  }

  @Test
  public void testParseNegativeInteger() throws IOException {
    Variant v = VariantJsonParser.parseJson("-100");
    Assert.assertEquals(-100, v.getLong());
  }

  @Test
  public void testParseNegativeDecimal() throws IOException {
    Variant v = VariantJsonParser.parseJson("-99.99");
    Assert.assertEquals(0, new BigDecimal("-99.99").compareTo(v.getDecimal()));
  }

  @Test
  public void testParseZero() throws IOException {
    Variant v = VariantJsonParser.parseJson("0");
    Assert.assertEquals(Variant.Type.BYTE, v.getType());
    Assert.assertEquals(0, v.getLong());
  }

  @Test
  public void testParseEmptyObject() throws IOException {
    Variant v = VariantJsonParser.parseJson("{}");
    Assert.assertEquals(Variant.Type.OBJECT, v.getType());
    Assert.assertEquals(0, v.numObjectElements());
  }

  @Test
  public void testParseSimpleObject() throws IOException {
    Variant v = VariantJsonParser.parseJson("{\"name\":\"John\",\"age\":30}");
    Assert.assertEquals(Variant.Type.OBJECT, v.getType());
    Assert.assertEquals(2, v.numObjectElements());
    Assert.assertEquals("John", v.getFieldByKey("name").getString());
    Assert.assertEquals(30, v.getFieldByKey("age").getLong());
  }

  @Test
  public void testParseNestedObject() throws IOException {
    Variant v = VariantJsonParser.parseJson("{\"user\":{\"id\":100,\"country\":\"US\"},\"active\":true}");
    Assert.assertEquals(Variant.Type.OBJECT, v.getType());
    Variant user = v.getFieldByKey("user");
    Assert.assertEquals(Variant.Type.OBJECT, user.getType());
    Assert.assertEquals(100, user.getFieldByKey("id").getLong());
    Assert.assertEquals("US", user.getFieldByKey("country").getString());
    Assert.assertTrue(v.getFieldByKey("active").getBoolean());
  }

  @Test
  public void testParseEmptyArray() throws IOException {
    Variant v = VariantJsonParser.parseJson("[]");
    Assert.assertEquals(Variant.Type.ARRAY, v.getType());
    Assert.assertEquals(0, v.numArrayElements());
  }

  @Test
  public void testParseSimpleArray() throws IOException {
    Variant v = VariantJsonParser.parseJson("[1,2,3,\"four\"]");
    Assert.assertEquals(Variant.Type.ARRAY, v.getType());
    Assert.assertEquals(4, v.numArrayElements());
    Assert.assertEquals(1, v.getElementAtIndex(0).getLong());
    Assert.assertEquals(2, v.getElementAtIndex(1).getLong());
    Assert.assertEquals(3, v.getElementAtIndex(2).getLong());
    Assert.assertEquals("four", v.getElementAtIndex(3).getString());
  }

  @Test
  public void testParseNestedArray() throws IOException {
    Variant v = VariantJsonParser.parseJson("[[1,2],[3,4]]");
    Assert.assertEquals(Variant.Type.ARRAY, v.getType());
    Assert.assertEquals(2, v.numArrayElements());
    Variant inner = v.getElementAtIndex(0);
    Assert.assertEquals(Variant.Type.ARRAY, inner.getType());
    Assert.assertEquals(1, inner.getElementAtIndex(0).getLong());
    Assert.assertEquals(2, inner.getElementAtIndex(1).getLong());
  }

  @Test
  public void testParseMixedArray() throws IOException {
    Variant v = VariantJsonParser.parseJson("[1,\"two\",true,null,3.14]");
    Assert.assertEquals(Variant.Type.ARRAY, v.getType());
    Assert.assertEquals(5, v.numArrayElements());
    Assert.assertEquals(1, v.getElementAtIndex(0).getLong());
    Assert.assertEquals("two", v.getElementAtIndex(1).getString());
    Assert.assertTrue(v.getElementAtIndex(2).getBoolean());
    Assert.assertEquals(Variant.Type.NULL, v.getElementAtIndex(3).getType());
    Assert.assertEquals(
        0, new BigDecimal("3.14").compareTo(v.getElementAtIndex(4).getDecimal()));
  }

  @Test
  public void testParseObjectWithNullValue() throws IOException {
    Variant v = VariantJsonParser.parseJson("{\"key\":null}");
    Assert.assertEquals(Variant.Type.OBJECT, v.getType());
    Assert.assertEquals(Variant.Type.NULL, v.getFieldByKey("key").getType());
  }

  @Test
  public void testParseComplexDocument() throws IOException {
    String json = "{\"userId\":12345,\"events\":["
        + "{\"eType\":\"login\",\"ts\":\"2026-01-15T10:30:00Z\"},"
        + "{\"eType\":\"purchase\",\"amount\":99.99}"
        + "]}";
    Variant v = VariantJsonParser.parseJson(json);
    Assert.assertEquals(Variant.Type.OBJECT, v.getType());
    Assert.assertEquals(12345, v.getFieldByKey("userId").getLong());
    Variant events = v.getFieldByKey("events");
    Assert.assertEquals(Variant.Type.ARRAY, events.getType());
    Assert.assertEquals(2, events.numArrayElements());
    Assert.assertEquals(
        "login", events.getElementAtIndex(0).getFieldByKey("eType").getString());
    Assert.assertEquals(
        0,
        new BigDecimal("99.99")
            .compareTo(events.getElementAtIndex(1)
                .getFieldByKey("amount")
                .getDecimal()));
  }

  @Test
  public void testParseEmptyString() throws IOException {
    Variant v = VariantJsonParser.parseJson("\"\"");
    Assert.assertEquals(Variant.Type.STRING, v.getType());
    Assert.assertEquals("", v.getString());
  }

  @Test
  public void testParseUnicodeString() throws IOException {
    Variant v = VariantJsonParser.parseJson("\"\\u00e9l\\u00e8ve\"");
    Assert.assertEquals(Variant.Type.STRING, v.getType());
    Assert.assertEquals("\u00e9l\u00e8ve", v.getString());
  }

  @Test
  public void testParseEscapedString() throws IOException {
    Variant v = VariantJsonParser.parseJson("\"hello\\nworld\"");
    Assert.assertEquals(Variant.Type.STRING, v.getType());
    Assert.assertEquals("hello\nworld", v.getString());
  }

  @Test(expected = IOException.class)
  public void testParseMalformedJson() throws IOException {
    VariantJsonParser.parseJson("{invalid");
  }

  @Test(expected = IOException.class)
  public void testParseIncompleteObject() throws IOException {
    VariantJsonParser.parseJson("{\"key\":");
  }

  @Test
  public void testParseJsonWithParser() throws IOException {
    JsonFactory factory = new JsonFactory();
    try (JsonParser parser = factory.createParser("{\"a\":1}")) {
      parser.nextToken();
      Variant v = VariantJsonParser.parseJson(parser);
      Assert.assertEquals(Variant.Type.OBJECT, v.getType());
      Assert.assertEquals(1, v.getFieldByKey("a").getLong());
    }
  }

  @Test
  public void testParseDuplicateKeysLastWins() throws IOException {
    Variant v = VariantJsonParser.parseJson("{\"k\":1,\"k\":2}");
    Assert.assertEquals(Variant.Type.OBJECT, v.getType());
    Assert.assertEquals(1, v.numObjectElements());
    Assert.assertEquals(2, v.getFieldByKey("k").getLong());
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
      Assert.assertEquals(Variant.Type.OBJECT, v.getType());
      v = v.getFieldByKey("n");
    }
    Assert.assertEquals(42, v.getLong());
  }

  @Test
  public void testObjectKeysSorted() throws IOException {
    Variant v = VariantJsonParser.parseJson("{\"c\":3,\"a\":1,\"b\":2}");
    Assert.assertEquals(Variant.Type.OBJECT, v.getType());
    Assert.assertEquals(3, v.numObjectElements());
    Assert.assertEquals("a", v.getFieldAtIndex(0).key);
    Assert.assertEquals("b", v.getFieldAtIndex(1).key);
    Assert.assertEquals("c", v.getFieldAtIndex(2).key);
  }

  @Test(expected = IOException.class)
  public void testParseEmptyInput() throws IOException {
    VariantJsonParser.parseJson("");
  }

  @Test(expected = IOException.class)
  public void testParseNotJson() throws IOException {
    VariantJsonParser.parseJson("not json at all");
  }

  @Test(expected = IOException.class)
  public void testParseIncompleteArray() throws IOException {
    VariantJsonParser.parseJson("[1, 2,");
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
    Assert.assertEquals(Variant.Type.OBJECT, v.getType());
    Assert.assertEquals(numKeys, v.numObjectElements());
    // Spot-check a few values
    Assert.assertEquals(0, v.getFieldByKey("key0").getLong());
    Assert.assertEquals(500, v.getFieldByKey("key500").getLong());
    Assert.assertEquals(999, v.getFieldByKey("key999").getLong());
  }
}
