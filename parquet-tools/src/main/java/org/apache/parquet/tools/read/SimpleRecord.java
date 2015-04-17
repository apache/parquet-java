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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.BinaryNode;

public class SimpleRecord {
  public static final int TAB_SIZE = 2;
  protected final List<NameValue> values;

  public SimpleRecord() {
    this.values = new ArrayList<NameValue>();
  }

  public void add(String name, Object value) {
    values.add(new NameValue(name,value));
  }
  
  public List<NameValue> getValues() {
    return Collections.unmodifiableList(values);
  }

  public String toString() {
    return values.toString();
  }

  public void prettyPrint() {
    prettyPrint(new PrintWriter(System.out,true));
  }

  public void prettyPrint(PrintWriter out) {
    prettyPrint(out, 0);
  }

  public void prettyPrint(PrintWriter out, int depth) {
    for (NameValue value : values) {
      out.print(Strings.repeat(".", depth));

      out.print(value.getName());
      Object val = value.getValue();
      if (val == null) {
        out.print(" = ");
        out.print("<null>");
      } else if (byte[].class == val.getClass()) {
        out.print(" = ");
        out.print(new BinaryNode((byte[]) val).asText());
      } else if (short[].class == val.getClass()) {
        out.print(" = ");
        out.print(Arrays.toString((short[])val));
      } else if (int[].class == val.getClass()) {
        out.print(" = ");
        out.print(Arrays.toString((int[])val));
      } else if (long[].class == val.getClass()) {
        out.print(" = ");
        out.print(Arrays.toString((long[])val));
      } else if (float[].class == val.getClass()) {
        out.print(" = ");
        out.print(Arrays.toString((float[])val));
      } else if (double[].class == val.getClass()) {
        out.print(" = ");
        out.print(Arrays.toString((double[])val));
      } else if (boolean[].class == val.getClass()) {
        out.print(" = ");
        out.print(Arrays.toString((boolean[])val));
      } else if (val.getClass().isArray()) {
        out.print(" = ");
        out.print(Arrays.deepToString((Object[])val));
      } else if (SimpleRecord.class.isAssignableFrom(val.getClass())) {
        out.println(":");
        ((SimpleRecord)val).prettyPrint(out, depth+1);
        continue;
      } else {
        out.print(" = ");
        out.print(String.valueOf(val));
      }

      out.println();
    }
  }

  public void prettyPrintJson(PrintWriter out) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    out.write(mapper.writeValueAsString(this.toJsonObject()));
  }

  protected Object toJsonObject() {
    Map<String, Object> result = Maps.newLinkedHashMap();
    for (NameValue value : values) {
      result.put(value.getName(), toJsonValue(value.getValue()));
    }
    return result;
  }

  protected static Object toJsonValue(Object val) {
    if (SimpleRecord.class.isAssignableFrom(val.getClass())) {
      return ((SimpleRecord) val).toJsonObject();
    } else if (byte[].class == val.getClass()) {
      return new BinaryNode((byte[]) val);
    } else {
      return val;
    }
  }

  public static final class NameValue {
    private final String name;
    private final Object value;

    public NameValue(String name, Object value) {
      this.name = name;
      this.value = value;
    }

    public String toString() {
      return name + ": " + value;
    }

    public String getName() {
      return name;
    }

    public Object getValue() {
      return value;
    }
  }
}

