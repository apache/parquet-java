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
package parquet.avro;

import com.google.common.io.Resources;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.codehaus.jackson.node.NullNode;
import org.junit.Test;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import static org.junit.Assert.assertEquals;

public class TestAvroSchemaConverter {

  private void testConversion(Schema avroSchema, String schemaString) throws
      Exception {
    AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter();
    MessageType schema = avroSchemaConverter.convert(avroSchema);
    MessageType expectedMT = MessageTypeParser.parseMessageType(schemaString);
    assertEquals("converting " + schema + " to " + schemaString, expectedMT.toString(),
        schema.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTopLevelMustBeARecord() {
    new AvroSchemaConverter().convert(Schema.create(Schema.Type.INT));
  }

  @Test
  public void testAllTypes() throws Exception {
    Schema schema = new Schema.Parser().parse(
        Resources.getResource("all.avsc").openStream());
    testConversion(
        schema,
        "message parquet.avro.myrecord {\n" +
        // Avro nulls are not encoded, unless they are null unions
        "  required boolean myboolean;\n" +
        "  required int32 myint;\n" +
        "  required int64 mylong;\n" +
        "  required float myfloat;\n" +
        "  required double mydouble;\n" +
        "  required binary mybytes;\n" +
        "  required binary mystring (UTF8);\n" +
        "  required group mynestedrecord {\n" +
        "    required int32 mynestedint;\n" +
        "  }\n" +
        "  required binary myenum (ENUM);\n" +
        "  required group myarray (LIST) {\n" +
        "    repeated int32 array;\n" +
        "  }\n" +
        "  required group myemptyarray (LIST) {\n" +
        "    repeated int32 array;\n" +
        "  }\n" +
        "  optional group myoptionalarray (LIST) {\n" +
        "    repeated int32 array;\n" +
        "  }\n" +
        "  required group mymap (MAP) {\n" +
        "    repeated group map (MAP_KEY_VALUE) {\n" +
        "      required binary key (UTF8);\n" +
        "      required int32 value;\n" +
        "    }\n" +
        "  }\n" +
        "  required group myemptymap (MAP) {\n" +
        "    repeated group map (MAP_KEY_VALUE) {\n" +
        "      required binary key (UTF8);\n" +
        "      required int32 value;\n" +
        "    }\n" +
        "  }\n" +
        "  required fixed_len_byte_array myfixed(1);\n" +
        "}\n");
  }

  @Test
  public void testOptionalFields() throws Exception {
    Schema schema = Schema.createRecord("record1", null, null, false);
    Schema optionalInt = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type
        .NULL),
        Schema.create(Schema.Type.INT)));
    schema.setFields(Arrays.asList(
        new Schema.Field("myint", optionalInt, null, NullNode.getInstance())
    ));
    testConversion(
        schema,
        "message record1 {\n" +
        "  optional int32 myint;\n" +
        "}\n");
  }

  @Test
  public void testUnionOfTwoTypes() throws Exception {
    Schema schema = Schema.createRecord("record2", null, null, false);
    Schema multipleTypes = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type
        .NULL),
        Schema.create(Schema.Type.INT),
        Schema.create(Schema.Type.FLOAT)));
    schema.setFields(Arrays.asList(
        new Schema.Field("myunion", multipleTypes, null, NullNode.getInstance())));

    // Avro union is modelled using optional data members of thw different types;
    testConversion(
        schema,
        "message record2 {\n" +
        "  optional group myunion {\n" +
        "    optional int32 member0;\n" +
        "    optional float member1;\n" +
        "  }\n" +
        "}\n");
  }
}
