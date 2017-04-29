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
package org.apache.parquet.proto;

import com.google.protobuf.Message;
import org.junit.Test;
import org.apache.parquet.proto.test.TestProto3;
import org.apache.parquet.proto.test.TestProtobuf;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

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
      "  optional double optionalDouble = 1;\n" +
      "  optional float optionalFloat = 2;\n" +
      "  optional int32 optionalInt32 = 3;\n" +
      "  optional int64 optionalInt64 = 4;\n" +
      "  optional int32 optionalUInt32 = 5;\n" +
      "  optional int64 optionalUInt64 = 6;\n" +
      "  optional int32 optionalSInt32 = 7;\n" +
      "  optional int64 optionalSInt64 = 8;\n" +
      "  optional int32 optionalFixed32 = 9;\n" +
      "  optional int64 optionalFixed64 = 10;\n" +
      "  optional int32 optionalSFixed32 = 11;\n" +
      "  optional int64 optionalSFixed64 = 12;\n" +
      "  optional boolean optionalBool = 13;\n" +
      "  optional binary optionalString (UTF8) = 14;\n" +
      "  optional binary optionalBytes = 15;\n" +
      "  optional group optionalMessage = 16 {\n" +
      "    optional int32 someId = 3;\n" +
      "  }\n" +
      "  optional group pbgroup = 17 {\n" +
      "    optional int32 groupInt = 2;\n" +
      "  }\n" +
      " optional binary optionalEnum (ENUM)  = 18;" +
      "}";

    testConversion(TestProtobuf.SchemaConverterAllDatatypes.class, expectedSchema);
  }

  /**
   * Tests that all protocol buffer datatypes are converted to correct parquet datatypes.
   */
  @Test
  public void testProto3ConvertAllDatatypes() throws Exception {
    String expectedSchema =
      "message TestProto3.SchemaConverterAllDatatypes {\n" +
        "  optional double optionalDouble = 1;\n" +
        "  optional float optionalFloat = 2;\n" +
        "  optional int32 optionalInt32 = 3;\n" +
        "  optional int64 optionalInt64 = 4;\n" +
        "  optional int32 optionalUInt32 = 5;\n" +
        "  optional int64 optionalUInt64 = 6;\n" +
        "  optional int32 optionalSInt32 = 7;\n" +
        "  optional int64 optionalSInt64 = 8;\n" +
        "  optional int32 optionalFixed32 = 9;\n" +
        "  optional int64 optionalFixed64 = 10;\n" +
        "  optional int32 optionalSFixed32 = 11;\n" +
        "  optional int64 optionalSFixed64 = 12;\n" +
        "  optional boolean optionalBool = 13;\n" +
        "  optional binary optionalString (UTF8) = 14;\n" +
        "  optional binary optionalBytes = 15;\n" +
        "  optional group optionalMessage = 16 {\n" +
        "    optional int32 someId = 3;\n" +
        "  }\n" +
        "  optional binary optionalEnum (ENUM) = 18;" +
        "  optional int32 someInt32 = 19;" +
        "  optional binary someString (UTF8) = 20;" +
        "  required group optionalMap (MAP) = 21 {\n" +
        "    repeated group key_value {\n" +
        "      required int64 key;\n" +
        "      optional group value {\n" +
        "        optional int32 someId = 3;\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}";

    testConversion(TestProto3.SchemaConverterAllDatatypes.class, expectedSchema);
  }

  @Test
  public void testConvertRepetition() throws Exception {
    String expectedSchema =
      "message TestProtobuf.SchemaConverterRepetition {\n" +
        "  optional int32 optionalPrimitive = 1;\n" +
        "  required int32 requiredPrimitive = 2;\n" +
        "  required group repeatedPrimitive (LIST) = 3 {\n" +
        "    repeated group list {\n" +
        "      required int32 element;\n" +
        "    }\n" +
        "  }\n" +
        "  optional group optionalMessage = 7 {\n" +
        "    optional int32 someId = 3;\n" +
        "  }\n" +
        "  required group requiredMessage = 8 {\n" +
        "    optional int32 someId= 3;\n" +
        "  }\n" +
        "  required group repeatedMessage (LIST) = 9 {\n" +
        "    repeated group list {\n" +
        "      optional int32 someId = 3;\n" +
        "    }\n" +
        "  }" +
        "}";

    testConversion(TestProtobuf.SchemaConverterRepetition.class, expectedSchema);
  }

  @Test
  public void testProto3ConvertRepetition() throws Exception {
    String expectedSchema =
      "message TestProto3.SchemaConverterRepetition {\n" +
        "  optional int32 optionalPrimitive = 1;\n" +
        "  required group repeatedPrimitive (LIST) = 3 {\n" +
        "    repeated group list {\n" +
        "      required int32 element;\n" +
        "    }\n" +
        "  }\n" +
        "  optional group optionalMessage = 7 {\n" +
        "    optional int32 someId = 3;\n" +
        "  }\n" +
        "  required group repeatedMessage (LIST) = 9 {\n" +
        "    repeated group list {\n" +
        "      optional int32 someId = 3;\n" +
        "    }\n" +
        "  }\n" +
        "}";

    testConversion(TestProto3.SchemaConverterRepetition.class, expectedSchema);
  }
}
