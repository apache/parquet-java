package redelm.parser;

import static org.junit.Assert.assertEquals;
import static redelm.schema.PrimitiveType.Primitive.INT64;
import static redelm.schema.PrimitiveType.Primitive.STRING;
import static redelm.schema.Type.Repetition.OPTIONAL;
import static redelm.schema.Type.Repetition.REPEATED;
import static redelm.schema.Type.Repetition.REQUIRED;

import org.junit.Test;

import redelm.schema.GroupType;
import redelm.schema.MessageType;
import redelm.schema.PrimitiveType;

public class TestRedelmParser {
    @Test
    public void test() throws Exception {
        String example = "message Document {\n" +
                "  required int64 DocId;\n" +
                "  optional group Links {\n" +
                "    repeated int64 Backward;\n" +
                "    repeated int64 Forward; }\n" +
                "  repeated group Name {\n" +
                "    repeated group Language {\n" +
                "      required string Code;\n" +
                "      required string Country; }\n" +
                "    optional string Url; }}";
        MessageType parsed = RedelmParser.parseMessageType(example);
        MessageType manuallyMade =
            new MessageType("Document",
                new PrimitiveType(REQUIRED, INT64, "DocId"),
                new GroupType(OPTIONAL, "Links",
                    new PrimitiveType(REPEATED, INT64, "Backward"),
                    new PrimitiveType(REPEATED, INT64, "Forward")),
                new GroupType(REPEATED, "Name",
                    new GroupType(REPEATED, "Language",
                        new PrimitiveType(REQUIRED, STRING, "Code"),
                        new PrimitiveType(REQUIRED, STRING, "Country")),
                    new PrimitiveType(OPTIONAL, STRING, "Url")));
        assertEquals(manuallyMade, parsed);

        MessageType parsedThenReparsed = RedelmParser.parseMessageType(parsed.toString());

        assertEquals(manuallyMade, parsedThenReparsed);

        parsed = RedelmParser.parseMessageType("message m { required group a {required string b;}; required group c { required int64 d; };}");
        manuallyMade =
            new MessageType("m",
                new GroupType(REQUIRED, "a",
                    new PrimitiveType(REQUIRED, STRING, "b")),
                new GroupType(REQUIRED, "c",
                    new PrimitiveType(REQUIRED, INT64, "d")));

        assertEquals(manuallyMade, parsed);

        parsedThenReparsed = RedelmParser.parseMessageType(parsed.toString());

        assertEquals(manuallyMade, parsedThenReparsed);
    }
}
