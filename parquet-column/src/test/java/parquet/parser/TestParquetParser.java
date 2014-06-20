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

import static org.junit.Assert.assertEquals;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static parquet.schema.Type.Repetition.*;

import org.junit.Assert;
import org.junit.Test;

import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Types;

public class TestParquetParser {
    @Test
    public void testPaperExample() throws Exception {
        String example = "message Document {\n" +
                "  required int64 DocId;\n" +
                "  optional group Links {\n" +
                "    repeated int64 Backward;\n" +
                "    repeated int64 Forward; }\n" +
                "  repeated group Name {\n" +
                "    repeated group Language {\n" +
                "      required binary Code;\n" +
                "      required binary Country; }\n" +
                "    optional binary Url; }}";
        MessageType parsed = MessageTypeParser.parseMessageType(example);
        MessageType manuallyMade =
            new MessageType("Document",
                new PrimitiveType(REQUIRED, INT64, "DocId"),
                new GroupType(OPTIONAL, "Links",
                    new PrimitiveType(REPEATED, INT64, "Backward"),
                    new PrimitiveType(REPEATED, INT64, "Forward")),
                new GroupType(REPEATED, "Name",
                    new GroupType(REPEATED, "Language",
                        new PrimitiveType(REQUIRED, BINARY, "Code"),
                        new PrimitiveType(REQUIRED, BINARY, "Country")),
                    new PrimitiveType(OPTIONAL, BINARY, "Url")));
        assertEquals(manuallyMade, parsed);

        MessageType parsedThenReparsed = MessageTypeParser.parseMessageType(parsed.toString());

        assertEquals(manuallyMade, parsedThenReparsed);

        parsed = MessageTypeParser.parseMessageType("message m { required group a {required binary b;} required group c { required int64 d; }}");
        manuallyMade =
            new MessageType("m",
                new GroupType(REQUIRED, "a",
                    new PrimitiveType(REQUIRED, BINARY, "b")),
                new GroupType(REQUIRED, "c",
                    new PrimitiveType(REQUIRED, INT64, "d")));

        assertEquals(manuallyMade, parsed);

        parsedThenReparsed = MessageTypeParser.parseMessageType(parsed.toString());

        assertEquals(manuallyMade, parsedThenReparsed);
    }

  @Test
  public void testEachPrimitiveType() {
    Types.MessageTypeBuilder builder = Types.buildMessage();
    StringBuilder schema = new StringBuilder();
    schema.append("message EachType {\n");
    for (PrimitiveTypeName type : PrimitiveTypeName.values()) {
      // add a schema entry, e.g., "  required int32 int32_;\n"
      if (type == FIXED_LEN_BYTE_ARRAY) {
        schema.append("  required fixed_len_byte_array(3) fixed_;");
        builder.required(FIXED_LEN_BYTE_ARRAY).length(3).named("fixed_");
      } else {
        schema.append("  required ").append(type)
            .append(" ").append(type).append("_;\n");
        builder.required(type).named(type.toString() + "_");
      }
    }
    schema.append("}\n");
    MessageType expected = builder.named("EachType");

    MessageType parsed = MessageTypeParser.parseMessageType(schema.toString());

    Assert.assertEquals(expected, parsed);
    MessageType reparsed = MessageTypeParser.parseMessageType(parsed.toString());
    Assert.assertEquals(expected, reparsed);
  }

  @Test
  public void testUTF8Annotation() {
    String message = "message StringMessage {\n" +
        "  required binary string (UTF8);\n" +
        "}\n";

    MessageType parsed = MessageTypeParser.parseMessageType(message);
    MessageType expected = Types.buildMessage()
        .required(BINARY).as(OriginalType.UTF8).named("string")
        .named("StringMessage");

    Assert.assertEquals(expected, parsed);
    MessageType reparsed = MessageTypeParser.parseMessageType(parsed.toString());
    Assert.assertEquals(expected, reparsed);
  }

  @Test
  public void testMAPAnnotations() {
    // this is primarily to test group annotations
    String message = "message Message {\n" +
        "  optional group aMap (MAP) {\n" +
        "    repeated group map (MAP_KEY_VALUE) {\n" +
        "      required binary key (UTF8);\n" +
        "      required int32 value;\n" +
        "    }\n" +
        "  }\n" +
        "}\n";

    MessageType parsed = MessageTypeParser.parseMessageType(message);
    MessageType expected = Types.buildMessage()
        .optionalGroup()
            .repeatedGroup()
                .required(BINARY).as(OriginalType.UTF8).named("key")
                .required(INT32).named("value")
                .named("map")
            .named("aMap")
        .named("Message");

    Assert.assertEquals(expected, parsed);
    MessageType reparsed = MessageTypeParser.parseMessageType(parsed.toString());
    Assert.assertEquals(expected, reparsed);
  }

  @Test
  public void testLISTAnnotation() {
    // this is primarily to test group annotations
    String message = "message Message {\n" +
        "  required group aList (LIST) {\n" +
        "    repeated binary string (UTF8);\n" +
        "  }\n" +
        "}\n";

    MessageType parsed = MessageTypeParser.parseMessageType(message);
    MessageType expected = Types.buildMessage()
        .requiredGroup()
            .repeated(BINARY).as(OriginalType.UTF8).named("string")
            .named("aList")
        .named("Message");

    Assert.assertEquals(expected, parsed);
    MessageType reparsed = MessageTypeParser.parseMessageType(parsed.toString());
    Assert.assertEquals(expected, reparsed);
  }

  @Test
  public void testDecimalFixedAnnotation() {
    String message = "message DecimalMessage {\n" +
        "  required FIXED_LEN_BYTE_ARRAY(4) aDecimal (DECIMAL(9,2));\n" +
        "}\n";

    MessageType parsed = MessageTypeParser.parseMessageType(message);
    MessageType expected = Types.buildMessage()
        .required(FIXED_LEN_BYTE_ARRAY).length(4)
            .as(OriginalType.DECIMAL).precision(9).scale(2)
            .named("aDecimal")
        .named("DecimalMessage");

    Assert.assertEquals(expected, parsed);
    MessageType reparsed = MessageTypeParser.parseMessageType(parsed.toString());
    Assert.assertEquals(expected, reparsed);
  }

  @Test
  public void testDecimalBinaryAnnotation() {
    String message = "message DecimalMessage {\n" +
        "  required binary aDecimal (DECIMAL(9,2));\n" +
        "}\n";

    MessageType parsed = MessageTypeParser.parseMessageType(message);
    MessageType expected = Types.buildMessage()
        .required(BINARY).as(OriginalType.DECIMAL).precision(9).scale(2)
            .named("aDecimal")
        .named("DecimalMessage");

    Assert.assertEquals(expected, parsed);
    MessageType reparsed = MessageTypeParser.parseMessageType(parsed.toString());
    Assert.assertEquals(expected, reparsed);
  }

}
