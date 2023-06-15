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

import org.apache.hadoop.fs.Path;
import org.apache.parquet.proto.test.TestProto3SchemaV1;
import org.apache.parquet.proto.test.TestProto3SchemaV2;
import org.apache.parquet.proto.test.TestProto3SchemaV3;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.parquet.proto.TestUtils.readMessages;
import static org.apache.parquet.proto.TestUtils.writeMessages;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * Tests for backward/forward compatibility while write and read parquet using different versions of protobuf schema.
 */
public class ProtoSchemaEvolutionTest {

  /**
   * Test we can read enum value (number) with an old schema even the value is missing in the old schema.
   */
  @Test
  public void testEnumSchemaWriteV2ReadV1() throws IOException {
    TestProto3SchemaV2.MessageSchema dataV2 = TestProto3SchemaV2.MessageSchema.newBuilder()
      .setOptionalLabelNumberPair(TestProto3SchemaV2.MessageSchema.LabelNumberPair.SECOND)
      .setOptionalString("string value")
      .build();
    Path file = writeMessages(dataV2);
    List<TestProto3SchemaV1.MessageSchema> messagesV1 = readMessages(file, TestProto3SchemaV1.MessageSchema.class);
    assertEquals(messagesV1.size(), 1);
    assertEquals(messagesV1.get(0).getOptionalLabelNumberPairValue(), 2);
  }

  /**
   * Write enum value unknown in V1 (thus "UNKNOWN_ENUM_VALUE_*"), and we can read it back with schema V2 that contains
   * the enum definition.
   */
  @Test
  public void testEnumSchemaWriteV1ReadV2() throws IOException {
    TestProto3SchemaV1.MessageSchema dataV1WithEnumValueFromV2 = TestProto3SchemaV1.MessageSchema.newBuilder()
      .setOptionalLabelNumberPairValue(2) // "2" is not defined in V1 enum, but the number is still accepted by protobuf
      .build();
    Path file = writeMessages(dataV1WithEnumValueFromV2);
    List<TestProto3SchemaV2.MessageSchema> messagesV2 = readMessages(file, TestProto3SchemaV2.MessageSchema.class);
    assertEquals(messagesV2.size(), 1);
    assertSame(messagesV2.get(0).getOptionalLabelNumberPair(), TestProto3SchemaV2.MessageSchema.LabelNumberPair.SECOND);
  }

  /**
   * Test we can ignore fields which are unknown during conversion. V3 schema has evolved but reading it into v1 have less fields then V3. Reading
   * it should still work and ignore fields it can't convert
   */
  @Test
  public void testEnumSchemaWriteV3ReadV1IgnoreUnknownField() throws IOException {
    TestProto3SchemaV3.MessageSchema dataV3 = TestProto3SchemaV3.MessageSchema.newBuilder()
      .setOptionalLabelNumberPair(TestProto3SchemaV3.MessageSchema.LabelNumberPair.SECOND)
      .setOptionalString("string value")
      .setOptionalInt32New(123)
      .addRepeatedDubMessageSchema(TestProto3SchemaV3.SubMessageSchema.newBuilder()
        .addOptionalFirstInt32(1)
        .setTestEnum(TestProto3SchemaV3.SubMessageSchema.SomeTestEnum.VALUE_X)
        .setOptionalFirstString("abc")
        .setLevel2Schema(TestProto3SchemaV3.Level2SubMessageSchema.newBuilder().addOptionalValues("axc").build())
        .build())
      .build();
    Path file = writeMessages(dataV3);
    List<TestProto3SchemaV1.MessageSchema> messagesV1 = readMessages(file, TestProto3SchemaV1.MessageSchema.class, true);
    assertEquals(messagesV1.size(), 1);
    assertEquals(messagesV1.get(0).getOptionalLabelNumberPairValue(), 2);
    assertEquals(messagesV1.get(0).getOptionalString(), "string value");
  }

  /**
   * Test we can read enum value (number) with an old schema even the value is missing in the old schema.
   */
  @Test
  public void testEnumSchemaWriteV1ReadV3IgnoreUnknownField() throws IOException {
    TestProto3SchemaV1.MessageSchema dataV1WithEnumValueFromV1 = TestProto3SchemaV1.MessageSchema.newBuilder()
      .setOptionalLabelNumberPairValue(2) // "2" is not defined in V1 enum, but the number is still accepted by protobuf
      .build();
    Path file = writeMessages(dataV1WithEnumValueFromV1);
    List<TestProto3SchemaV3.MessageSchema> messagesV3 = readMessages(file, TestProto3SchemaV3.MessageSchema.class);
    assertEquals(messagesV3.size(), 1);
    assertSame(messagesV3.get(0).getOptionalLabelNumberPair(), TestProto3SchemaV3.MessageSchema.LabelNumberPair.SECOND);
  }

  /**
   * Test nested conversion
   */
  @Test
  public void testEnumSchemaWriteV3ReadV3() throws IOException {
    TestProto3SchemaV3.MessageSchema dataV3 = TestProto3SchemaV3.MessageSchema.newBuilder()
      .setOptionalLabelNumberPair(TestProto3SchemaV3.MessageSchema.LabelNumberPair.SECOND)
      .setOptionalString("string value")
      .setOptionalInt32New(123)
      .addRepeatedDubMessageSchema(TestProto3SchemaV3.SubMessageSchema.newBuilder()
        .addOptionalFirstInt32(1)
        .setTestEnum(TestProto3SchemaV3.SubMessageSchema.SomeTestEnum.VALUE_X)
        .setOptionalFirstString("abc")
        .setLevel2Schema(TestProto3SchemaV3.Level2SubMessageSchema.newBuilder().addOptionalValues("axc").build())
        .build())
      .build();
    Path file = writeMessages(dataV3);
    List<TestProto3SchemaV3.MessageSchema> messagesV3 = readMessages(file, TestProto3SchemaV3.MessageSchema.class, false);
    assertEquals(messagesV3.size(), 1);
    assertEquals(messagesV3.get(0).getOptionalLabelNumberPairValue(), 2);
    assertEquals(messagesV3.get(0).getOptionalString(), "string value");
    assertEquals(messagesV3.get(0).getRepeatedDubMessageSchemaList().get(0).getOptionalFirstString(), "abc");
    assertEquals(messagesV3.get(0).getRepeatedDubMessageSchemaList().get(0).getOptionalFirstInt32(0), 1);
    assertEquals(messagesV3.get(0).getRepeatedDubMessageSchemaList().get(0).getLevel2Schema().getOptionalValues(0), "axc");
  }
}
