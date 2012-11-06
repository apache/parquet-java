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
package redelm.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import redelm.Log;
import redelm.schema.GroupType;
import redelm.schema.MessageType;
import redelm.schema.PrimitiveType;
import redelm.schema.PrimitiveType.Primitive;
import redelm.schema.Type;
import redelm.schema.Type.Repetition;

public class MessageTypeParser {
  private static final Log LOG = Log.getLog(MessageTypeParser.class);

  private MessageTypeParser() {}

  public static MessageType parseMessageType(String input) {
    return parse(input);
  }

  private static MessageType parse(String schemaString) {
    StringTokenizer st = new StringTokenizer(schemaString, " ;{}\n\t", true);
    String t = nextToken(st);
    check(t, "message", "start with 'message'");
    String name = nextToken(st);
    Type[] fields = readGroupTypeFields(st);
    return new MessageType(name, fields);
  }

  private static Type[] readGroupTypeFields(StringTokenizer st) {
    List<Type> types = new ArrayList<Type>();
    String t = nextToken(st);
    check(t, "{", "start of message");
    while (!(t = nextToken(st)).equals("}")) {
      types.add(readType(t, st));
    }
    return types.toArray(new Type[types.size()]);
  }

  private static Type readType(String t, StringTokenizer st) {
    Repetition r = asRepetition(t);
    t = nextToken(st);
    String name = nextToken(st);
    if (t.equalsIgnoreCase("group")) {
      Type[] fields = readGroupTypeFields(st);
      return new GroupType(r, name, fields);
    } else {
      Primitive p = Primitive.valueOf(t.toUpperCase());
      check(nextToken(st), ";", "field ended by ;");
      return new PrimitiveType(r, p, name);
    }
  }

  private static Repetition asRepetition(String t) {
    try {
      return Repetition.valueOf(t.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("expected one of " + Arrays.toString(Repetition.values())  +" got " + t, e);
    }
  }

  private static void check(String t, String expected, String message) {
    if (!t.equalsIgnoreCase(expected)) {
      throw new IllegalArgumentException("expected: "+message+ ": '" + expected + "' got '" + t + "'");
    }
  }

  private static String nextToken(StringTokenizer st) {
    while (st.hasMoreTokens()) {
      String t = st.nextToken();
      if (!isWhitespace(t)) {
        return t;
      }
    }
    throw new IllegalArgumentException("unexpected end of schema");
  }

  private static boolean isWhitespace(String t) {
    return t.equals(" ") || t.equals("\t") || t.equals("\n");
  }
}
