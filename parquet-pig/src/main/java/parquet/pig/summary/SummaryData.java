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
package parquet.pig.summary;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonWriteNullProperties;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;

/**
 * Base class for a node of the data summary tree
 *
 * @author Julien Le Dem
 *
 */
@JsonWriteNullProperties(value = false)
public abstract class SummaryData {

  private static ObjectMapper objectMapper = new ObjectMapper();
  private static ObjectMapper prettyObjectMapper = new ObjectMapper();
  static {
    prettyObjectMapper.getSerializationConfig().set(Feature.INDENT_OUTPUT, true);
  }

  private long count;

  public static String toJSON(SummaryData summaryData) {
    return toJSON(summaryData, objectMapper);
  }

  public static String toPrettyJSON(SummaryData summaryData) {
    return toJSON(summaryData, prettyObjectMapper);
  }

  private static String toJSON(SummaryData summaryData, ObjectMapper mapper) {
    StringWriter stringWriter = new StringWriter();
    try {
      mapper.writeValue(stringWriter, summaryData);
    } catch (JsonGenerationException e) {
      throw new RuntimeException(e);
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return stringWriter.toString();
  }

  /**
   * parses JSON into the given class
   *
   * @param json
   * @param clazz
   * @return
   */
  public static <T extends SummaryData> T fromJSON(String json, Class<T> clazz) {
    try {
      return objectMapper.readValue(new StringReader(json), clazz);
    } catch (JsonParseException e) {
      throw new RuntimeException(e);
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * merges s2 into s1
   * @param s1
   * @param s2
   * @return
   */
  public static <T extends SummaryData> T merge(T s1, T s2) {
    if (s1 == null) {
      return s2;
    } else if (s2 == null) {
      return s1;
    } else {
      s1.merge(s2);
      return s1;
    }
  }

  protected FieldSchema getField(Schema schema, int i) {
    try {
      if (schema == null || i >= schema.size()) {
        return null;
      }
      FieldSchema field = schema.getField(i);
      return field;
    } catch (FrontendException e) {
      throw new RuntimeException(e);
    }
  }

  protected Schema getSchema(FieldSchema field) {
    return field == null ? null : field.schema;
  }

  protected String getName(FieldSchema field) {
    return field == null ? null : field.alias;
  }

  /**
   * add a single element to the structure
   * @param o never null
   */
  public void add(Object o) {
    ++count;
  }

  /**
   * merge the given input into this one
   * @param other never null
   */
  public void merge(SummaryData other) {
    this.count += other.count;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }


  @Override
  public String toString() {
    return toJSON(this);
  }
}
