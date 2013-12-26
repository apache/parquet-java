/**
 * Copyright 2013 Lukas Nalezenec
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

package parquet.proto;

import com.google.protobuf.Message;
import org.junit.Test;
import parquet.proto.test.TestProtobuf;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import static org.junit.Assert.assertEquals;

public class ProtoSchemaConverterTest {

  /**
   * Converts given pbClass to parquet schema and compares it with expected parquet schema.
   */
  private void testConversion(Class<? extends Message> pbClass, String parquetSchemaString) throws
          Exception {
    ProtoSchemaConverter protoSchemaConverter = new ProtoSchemaConverter();
    MessageType schema = protoSchemaConverter.convert(pbClass);
    MessageType expectedMT = MessageTypeParser.parseMessageType(parquetSchemaString);
    assertEquals(expectedMT.toString(), schema.toString());
  }


  /**
   * Tests that all protocol buffer datatypes are converted to correct parquet datatypes.
   */
  @Test
  public void testConvertAllDatatypes() throws Exception {
    String expectedSchema =
      "message TestProtobuf.SchemaConverterAllDatatypes {\n" +
      "  optional double optionalDouble;\n" +
      "  optional float optionalFloat;\n" +
      "  optional int32 optionalInt32;\n" +
      "  optional int64 optionalInt64;\n" +
      "  optional int32 optionalUInt32;\n" +
      "  optional int64 optionalUInt64;\n" +
      "  optional int32 optionalSInt32;\n" +
      "  optional int64 optionalSInt64;\n" +
      "  optional int32 optionalFixed32;\n" +
      "  optional int64 optionalFixed64;\n" +
      "  optional int32 optionalSFixed32;\n" +
      "  optional int64 optionalSFixed64;\n" +
      "  optional boolean optionalBool;\n" +
      "  optional binary optionalString (UTF8);\n" +
      "  optional binary optionalBytes;\n" +
      "  optional group optionalMessage {\n" +
      "    optional int32 someId;\n" +
      "  }\n" +
      "  optional group pbgroup {\n" +
      "    optional int32 groupInt;\n" +
      "  }\n" +
      " optional binary optionalEnum (ENUM);" +
      "}";

    testConversion(TestProtobuf.SchemaConverterAllDatatypes.class, expectedSchema);
  }

  @Test
  public void testConvertRepetition() throws Exception {
    String expectedSchema =
      "message TestProtobuf.SchemaConverterRepetition {\n" +
        "  optional int32 optionalPrimitive;\n" +
        "  required int32 requiredPrimitive;\n" +
        "  optional group repeatedPrimitive (LIST) {\n" +
        "    repeated int32 repeatedPrimitive_tuple;\n" +
        "  }\n" +
        "  optional group optionalMessage {\n" +
        "    optional int32 someId;\n" +
        "  }\n" +
        "  required group requiredMessage {" +
        "    optional int32 someId;\n" +
        "  }\n" +
        "  optional group repeatedMessage (LIST) {" +
        "    repeated group repeatedMessage_tuple {\n" +
        "      optional int32 someId;\n" +
        "    }\n" +
        "  }\n" +
        "}";

    testConversion(TestProtobuf.SchemaConverterRepetition.class, expectedSchema);
  }
}
