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
package redelm.pig;

import static org.junit.Assert.assertEquals;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.junit.Test;

import redelm.parser.MessageTypeParser;
import redelm.schema.MessageType;

public class TestPigSchemaConverter {

  @Test
  public void test() throws Exception {
    PigSchemaConverter pigSchemaConverter = new PigSchemaConverter();
    Schema pigSchema = Utils.getSchemaFromString("a:chararray, b:{t:(c:chararray, d:chararray)}");
    MessageType schema = pigSchemaConverter.convert(pigSchema);
    String expected = "message pig_schema {\n" +
                      "  optional string a;\n" +
                      "  repeated group b_t {\n" +
                      "    optional string c;\n" +
                      "    optional string d;\n" +
                      "  }\n" +
                      "}\n";
    MessageType expectedMT = MessageTypeParser.parseMessageType(expected);
    assertEquals(expectedMT, schema);
    assertEquals(expected, schema.toString());
  }
}
