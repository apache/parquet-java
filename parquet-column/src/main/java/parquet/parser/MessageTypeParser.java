/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import parquet.Log;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type.Repetition;

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
      st = new StringTokenizer(schemaString, " ;{}()\n\t", true);
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

    String t = st.nextToken();
    check(t, "message", "start with 'message'", st);
    String name = st.nextToken();
    Type[] fields = readGroupTypeFields(st.nextToken(), st);
    return new MessageType(name, fields);
  }

  private static Type[] readGroupTypeFields(String t, Tokenizer st) {
    List<Type> types = new ArrayList<Type>();
    check(t, "{", "start of message", st);
    while (!(t = st.nextToken()).equals("}")) {
      types.add(readType(t, st));
    }
    return types.toArray(new Type[types.size()]);
  }

  private static Type readType(String t, Tokenizer st) {
    Repetition r = asRepetition(t, st);
    String type = st.nextToken();
    String name = st.nextToken();
    t = st.nextToken();
    OriginalType originalType = null;
    if (t.equalsIgnoreCase("(")) {
      originalType = OriginalType.valueOf(st.nextToken());
      check(st.nextToken(), ")", "original type ended by )", st);
      t = st.nextToken();
    }
    try {
      if (type.equalsIgnoreCase("group")) {
        Type[] fields = readGroupTypeFields(t, st);
        return new GroupType(r, name, originalType, fields);
      } else {
        PrimitiveTypeName p = asPrimitive(type, st);
        check(t, ";", "field ended by ';'", st);
        return new PrimitiveType(r, p, name, originalType);
      }
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
