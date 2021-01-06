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

package org.apache.parquet.tools.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.tools.read.SimpleRecord;

import java.io.IOException;
import java.util.*;

public abstract class JsonRecordFormatter<T> {
  private static final int SINGLE_VALUE = 0;

  public static class JsonPrimitiveWriter extends JsonRecordFormatter<Object> {

    public JsonPrimitiveWriter(Type primitiveType) {
      super(primitiveType);
    }

    @Override
    protected Object formatResults(List<Object> listOfValues) {
      if (super.typeInfo.getRepetition() == Type.Repetition.REPEATED) {
        return listOfValues;
      } else {
        return listOfValues.get(SINGLE_VALUE);
      }
    }
  }

  public static class JsonGroupFormatter extends JsonRecordFormatter<SimpleRecord> {
    private final Map<String, JsonRecordFormatter> formatters;

    public JsonGroupFormatter(GroupType schema) {
      super(schema);

      this.formatters = buildWriters(schema);
    }

    private Map<String, JsonRecordFormatter> buildWriters(GroupType groupSchema) {
      Map<String, JsonRecordFormatter> writers = new LinkedHashMap<String, JsonRecordFormatter>();
      for (Type type : groupSchema.getFields()) {
        if (type.isPrimitive()) {
          writers.put(type.getName(), new JsonPrimitiveWriter(type));
        } else {
          writers.put(type.getName(), new JsonGroupFormatter((GroupType) type));
        }
      }

      return writers;
    }

    private Object add(SimpleRecord record) {
      return formatEntries(collateEntries(record));
    }

    private Map<String, List<Object>> collateEntries(SimpleRecord record) {
      Map<String, List<Object>> collatedEntries = new LinkedHashMap<String, List<Object>>();
      for (SimpleRecord.NameValue value : record.getValues()) {
        if (collatedEntries.containsKey(value.getName())) {
          collatedEntries.get(value.getName()).add(value.getValue());
        } else {
          List<Object> newResultListForKey = new ArrayList<Object>();
          newResultListForKey.add(value.getValue());
          collatedEntries.put(value.getName(), newResultListForKey);
        }
      }

      return collatedEntries;
    }

    private Object formatEntries(Map<String, List<Object>> entries) {
      Map<String, Object> results = new LinkedHashMap<String, Object>();
      for (Map.Entry<String, List<Object>> entry : entries.entrySet()) {
        JsonRecordFormatter formatter = formatters.get(entry.getKey());
        results.put(entry.getKey(), formatter.formatResults(entry.getValue()));
      }

      return results;
    }

    @Override
    protected Object formatResults(List<SimpleRecord> values) {
      if (super.typeInfo.getRepetition() == Type.Repetition.REPEATED) {
        List<Object> results = new ArrayList<Object>();
        for (SimpleRecord object : values) {
          results.add(add(object));
        }

        return results;
      } else {
        return add(values.get(SINGLE_VALUE));
      }
    }

    public String formatRecord(SimpleRecord value) throws IOException {
      ObjectMapper mapper = new ObjectMapper();
      return mapper.writeValueAsString(add(value));
    }
  }

  protected final Type typeInfo;

  protected JsonRecordFormatter(Type type) {
    this.typeInfo = type;
  }

  protected abstract Object formatResults(List<T> values);

  public static JsonGroupFormatter fromSchema(MessageType messageType) {
    return new JsonGroupFormatter(messageType);
  }
}
