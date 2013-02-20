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
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static parquet.schema.Type.Repetition.OPTIONAL;
import static parquet.schema.Type.Repetition.REPEATED;
import static parquet.schema.Type.Repetition.REQUIRED;

import org.junit.Test;

import parquet.parser.MessageTypeParser;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;

public class TestParquetParser {
    @Test
    public void test() throws Exception {
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
}
