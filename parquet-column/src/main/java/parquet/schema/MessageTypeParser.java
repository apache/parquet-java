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
package parquet.schema;

import java.util.Arrays;
import java.util.StringTokenizer;

import parquet.Log;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type.Repetition;
import parquet.schema.Types.GroupBuilder;
import parquet.schema.Types.PrimitiveBuilder;

/**
 * Parses a schema from a textual format similar to that described in the Dremel paper.
 *
 * @author Julien Le Dem
 */
public class MessageTypeParser {
  private static final Log LOG = Log.getLog(MessageTypeParser.class);

  private static class Tokenizer {

    private StringTokenizer st;

    private int line = 0;
    private StringBuffer currentLine = new StringBuffer();

    public Tokenizer(String schemaString, String string) {
      st = new StringTokenizer(schemaString, " ,;{}()\n\t=", true);
    }

    public String nextToken() {
      while (st.hasMoreTokens()) {
        String t = st.nextToken();
        if (t.equals("\n")) {
          ++ line;
          currentLine.setLength(0);
        } else {
          currentLine.append(t);
        }
        if (!isWhitespace(t)) {
          return t;
        }
      }
      throw new IllegalArgumentException("unexpected end of schema");
    }

    private boolean isWhitespace(String t) {
      return t.equals(" ") || t.equals("\t") || t.equals("\n");
    }

    public String getLocationString() {
      return "line " + line + ": " + currentLine.toString();
    }
  }

  private MessageTypeParser() {}

  /**
   *
   * @param input the text representation of the schema to parse
   * @return the corresponding object representation
   */
  public static MessageType parseMessageType(String input) {
    return parse(input);
  }

  private static MessageType parse(String schemaString) {
    Tokenizer st = new Tokenizer(schemaString, " ;{}()\n\t");
    Types.MessageTypeBuilder builder = Types.buildMessage();

    String t = st.nextToken();
    check(t, "message", "start with 'message'", st);
    String name = st.nextToken();
    addGroupTypeFields(st.nextToken(), st, builder);
    return builder.named(name);
  }

  private static void addGroupTypeFields(String t, Tokenizer st, Types.GroupBuilder builder) {
    check(t, "{", "start of message", st);
    while (!(t = st.nextToken()).equals("}")) {
      addType(t, st, builder);
    }
  }

  private static void addType(String t, Tokenizer st, Types.GroupBuilder builder) {
    Repetition repetition = asRepetition(t, st);

    // Read type.
    String type = st.nextToken();
    if ("group".equalsIgnoreCase(type)) {
      addGroupType(t, st, repetition, builder);
    } else {
      addPrimitiveType(t, st, asPrimitive(type, st), repetition, builder);
    }
  }

  private static void addGroupType(String t, Tokenizer st, Repetition r, GroupBuilder<?> builder) {
    GroupBuilder<?> childBuilder = builder.group(r);
    String name = st.nextToken();

    // Read annotation, if any.
    t = st.nextToken();
    OriginalType originalType = null;
    if (t.equalsIgnoreCase("(")) {
      originalType = OriginalType.valueOf(st.nextToken());
      childBuilder.as(originalType);
      check(st.nextToken(), ")", "original type ended by )", st);
      t = st.nextToken();
    }
    if (t.equals("=")) {
      childBuilder.id(Integer.parseInt(st.nextToken()));
      t = st.nextToken();
    }
    try {
      addGroupTypeFields(t, st, childBuilder);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("problem reading type: type = group, name = " + name + ", original type = " + originalType, e);
    }

    childBuilder.named(name);
  }

  private static void addPrimitiveType(String t, Tokenizer st, PrimitiveTypeName type, Repetition r, Types.GroupBuilder<?> builder) {
    PrimitiveBuilder<?> childBuilder = builder.primitive(type, r);

    if (type == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
      t = st.nextToken();
      // Read type length if the type is fixed_len_byte_array.
      if (!t.equalsIgnoreCase("(")) {
        throw new IllegalArgumentException("expecting (length) for field of type fixed_len_byte_array");
      }
      childBuilder.length(Integer.parseInt(st.nextToken()));
      check(st.nextToken(), ")", "type length ended by )", st);
    }

    String name = st.nextToken();

    // Read annotation, if any.
    t = st.nextToken();
    OriginalType originalType = null;
    if (t.equalsIgnoreCase("(")) {
      originalType = OriginalType.valueOf(st.nextToken());
      childBuilder.as(originalType);
      if (OriginalType.DECIMAL == originalType) {
        t = st.nextToken();
        // parse precision and scale
        if (t.equalsIgnoreCase("(")) {
          childBuilder.precision(Integer.parseInt(st.nextToken()));
          t = st.nextToken();
          if (t.equalsIgnoreCase(",")) {
            childBuilder.scale(Integer.parseInt(st.nextToken()));
            t = st.nextToken();
          }
          check(t, ")", "decimal type ended by )", st);
          t = st.nextToken();
        }
      } else {
        t = st.nextToken();
      }
      check(t, ")", "original type ended by )", st);
      t = st.nextToken();
    }
    if (t.equals("=")) {
      childBuilder.id(Integer.parseInt(st.nextToken()));
      t = st.nextToken();
    }
    check(t, ";", "field ended by ';'", st);

    try {
      childBuilder.named(name);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("problem reading type: type = " + type + ", name = " + name + ", original type = " + originalType, e);
    }
  }

  private static PrimitiveTypeName asPrimitive(String t, Tokenizer st) {
    try {
      return PrimitiveTypeName.valueOf(t.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("expected one of " + Arrays.toString(PrimitiveTypeName.values())  +" got " + t + " at " + st.getLocationString(), e);
    }
  }

  private static Repetition asRepetition(String t, Tokenizer st) {
    try {
      return Repetition.valueOf(t.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("expected one of " + Arrays.toString(Repetition.values())  +" got " + t + " at " + st.getLocationString(), e);
    }
  }

  private static void check(String t, String expected, String message, Tokenizer tokenizer) {
    if (!t.equalsIgnoreCase(expected)) {
      throw new IllegalArgumentException(message+ ": expected '" + expected + "' but got '" + t + "' at " + tokenizer.getLocationString());
    }
  }

}
