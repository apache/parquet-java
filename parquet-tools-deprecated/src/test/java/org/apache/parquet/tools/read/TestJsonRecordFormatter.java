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

package org.apache.parquet.tools.read;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.tools.json.JsonRecordFormatter;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestJsonRecordFormatter {
  private <T> List<T> array(T... objects) {
    return Arrays.asList(objects);
  }

  private <T> Map.Entry<String, T> entry(final String key, final T value) {
    return new Map.Entry<String, T>() {
      @Override
      public String getKey() {
        return key;
      }

      @Override
      public T getValue() {
        return value;
      }

      @Override
      public T setValue(T value) {
        throw new UnsupportedOperationException();
      }
    };
  }

  private Map<String, ?> obj(Map.Entry<String, ?>... entries) throws IOException {
    Map<String, Object> entriesAsMap = new LinkedHashMap<String, Object>();
    for (Map.Entry<String, ?> entry : entries) {
      entriesAsMap.put(entry.getKey(), entry.getValue());
    }

    return entriesAsMap;
  }

  private SimpleRecord.NameValue kv(String name, Object value) {
    return new SimpleRecord.NameValue(name, value);
  }

  private String asJsonString(Object object) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(object);
  }

  @Test
  public void testFlatSchemaWithArrays() throws Exception {
    SimpleRecord simple = new SimpleRecord();
    MessageType schema = new MessageType("schema",
      new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "reqd"),
      new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "opt"),
      new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.INT32, "odd"),
      new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.INT64, "even")
    );

    simple.values.add(kv("reqd", "a required value"));
    simple.values.add(kv("opt", 1.2345));

    simple.values.add(kv("odd", 1));
    simple.values.add(kv("odd", 3));
    simple.values.add(kv("odd", 5));
    simple.values.add(kv("odd", 7));
    simple.values.add(kv("odd", 9));

    simple.values.add(kv("even", 2));
    simple.values.add(kv("even", 4));
    simple.values.add(kv("even", 6));
    simple.values.add(kv("even", 8));
    simple.values.add(kv("even", 10));

    String expected = asJsonString(
      obj(
        entry("reqd", "a required value"),
        entry("opt", 1.2345),
        entry("odd", array(1, 3, 5, 7, 9)),
        entry("even", array(2, 4, 6, 8, 10))
      )
    );

    String actual = JsonRecordFormatter
      .fromSchema(schema)
      .formatRecord(simple);

    assertEquals(expected, actual);
  }

  @Test
  public void testNestedGrouping() throws Exception {
    SimpleRecord simple = new SimpleRecord();
    MessageType schema = new MessageType("schema",
      new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.BINARY, "flat-string"),
      new GroupType(Type.Repetition.OPTIONAL, "subgroup",
        new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "flat-int"),
        new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.BINARY, "string-list")
      )
    );

    SimpleRecord subgroup = new SimpleRecord();
    subgroup.values.add(kv("flat-int", 12345));
    subgroup.values.add(kv("string-list", "two"));
    subgroup.values.add(kv("string-list", "four"));
    subgroup.values.add(kv("string-list", "six"));
    subgroup.values.add(kv("string-list", "eight"));
    subgroup.values.add(kv("string-list", "ten"));

    simple.values.add(kv("flat-string", "one"));
    simple.values.add(kv("flat-string", "two"));
    simple.values.add(kv("flat-string", "three"));
    simple.values.add(kv("flat-string", "four"));
    simple.values.add(kv("flat-string", "five"));

    simple.values.add(kv("subgroup", subgroup));

    String actual = JsonRecordFormatter
      .fromSchema(schema)
      .formatRecord(simple);

    String expected = asJsonString(
      obj(
        entry("flat-string", array("one", "two", "three", "four", "five")),
        entry("subgroup",
          obj(
            entry("flat-int", 12345),
            entry("string-list", array("two", "four", "six", "eight", "ten"))
          )
        )
      )
    );

    assertEquals(expected, actual);
  }

  @Test
  public void testGroupList() throws Exception {
    SimpleRecord simple = new SimpleRecord();
    MessageType schema = new MessageType("schema",
      new GroupType(Type.Repetition.REPEATED, "repeat-group",
        new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "flat-int"),
        new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.DOUBLE, "repeat-double")
      )
    );

    SimpleRecord repeatGroup = new SimpleRecord();
    repeatGroup.values.add(kv("flat-int", 76543));
    repeatGroup.values.add(kv("repeat-double", 1.2345));
    repeatGroup.values.add(kv("repeat-double", 5.6789));
    repeatGroup.values.add(kv("repeat-double", 10.11121314));
    repeatGroup.values.add(kv("repeat-double", 0.4321));
    repeatGroup.values.add(kv("repeat-double", 7.6543));
    simple.values.add(kv("repeat-group", repeatGroup));

    repeatGroup = new SimpleRecord();
    repeatGroup.values.add(kv("flat-int", 12345));
    repeatGroup.values.add(kv("repeat-double", 1.1));
    repeatGroup.values.add(kv("repeat-double", 1.2));
    repeatGroup.values.add(kv("repeat-double", 1.3));
    repeatGroup.values.add(kv("repeat-double", 1.4));
    repeatGroup.values.add(kv("repeat-double", 1.5));
    simple.values.add(kv("repeat-group", repeatGroup));

    repeatGroup = new SimpleRecord();
    repeatGroup.values.add(kv("flat-int", 10293));
    repeatGroup.values.add(kv("repeat-double", 9.5));
    repeatGroup.values.add(kv("repeat-double", 9.4));
    repeatGroup.values.add(kv("repeat-double", 9.3));
    repeatGroup.values.add(kv("repeat-double", 9.2));
    repeatGroup.values.add(kv("repeat-double", 9.1));
    simple.values.add(kv("repeat-group", repeatGroup));

    String actual = JsonRecordFormatter
      .fromSchema(schema)
      .formatRecord(simple);

    String expected = asJsonString(
      obj(
        entry("repeat-group",
          array(
            obj(
              entry("flat-int", 76543),
              entry("repeat-double", array(1.2345, 5.6789, 10.11121314, 0.4321, 7.6543))
            ),
            obj(
              entry("flat-int", 12345),
              entry("repeat-double", array(1.1, 1.2, 1.3, 1.4, 1.5))
            ),
            obj(
              entry("flat-int", 10293),
              entry("repeat-double", array(9.5, 9.4, 9.3, 9.2, 9.1))
            )
          )
        )
      )
    );

    assertEquals(expected, actual);
  }
}
