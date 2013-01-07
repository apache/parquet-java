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

  private void testConversion(String pigSchemaString, String redelmSchemaString) throws Exception {
    Schema pigSchema = Utils.getSchemaFromString(pigSchemaString);
    MessageType schema = PigSchemaConverter.convert(pigSchema);
    MessageType expectedMT = MessageTypeParser.parseMessageType(redelmSchemaString);
    assertEquals("converting "+pigSchemaString+" to "+redelmSchemaString, expectedMT, schema);
  }

  @Test
  public void testTupleBag() throws Exception {
    testConversion(
        "a:chararray, b:{t:(c:chararray, d:chararray)}",
        "message pig_schema {\n" +
        "  optional string a;\n" +
        "  optional group b {\n" +
        "    repeated group t {\n" +
        "      optional string c;\n" +
        "      optional string d;\n" +
        "    }\n" +
        "  }\n" +
        "}\n");
  }

  @Test
  public void testTupleBagWithAnonymousInnerField() throws Exception {
    testConversion(
        "a:chararray, b:{(c:chararray, d:chararray)}",
        "message pig_schema {\n" +
        "  optional string a;\n" +
        "  optional group b {\n" +
        "    repeated group bag {\n" + // the inner field in the bag is called "bag"
        "      optional string c;\n" +
        "      optional string d;\n" +
        "    }\n" +
        "  }\n" +
        "}\n");
  }

  @Test
  public void testMap() throws Exception {
    testConversion(
        "a:chararray, b:[(c:chararray, d:chararray)]",
        "message pig_schema {\n" +
        "  optional string a;\n" +
        "  optional group b {\n" +
        "    repeated group map {\n" +
        "      required string key;\n" +
        "      optional group value {\n" +
        "        optional string c;\n" +
        "        optional string d;\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}\n");
  }

  @Test
  public void testAnnonymousField() throws Exception {
    testConversion(
        "a:chararray, int",
        "message pig_schema {\n" +
        "  optional string a;\n" +
        "  optional int32 val_0;\n" +
        "}\n");
  }
}
