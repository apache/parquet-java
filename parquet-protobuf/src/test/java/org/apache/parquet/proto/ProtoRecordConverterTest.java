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

import static org.apache.parquet.proto.TestUtils.testData;
import static org.apache.parquet.proto.test.TestProtobuf.SchemaConverterAllDatatypes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

import com.google.protobuf.ByteString;
import java.util.List;
import org.apache.parquet.proto.test.TestProto3;
import org.apache.parquet.proto.test.TestProtobuf;
import org.junit.jupiter.api.Test;

public class ProtoRecordConverterTest {

  @Test
  public void testAllTypes() throws Exception {
    SchemaConverterAllDatatypes.Builder data;
    data = SchemaConverterAllDatatypes.newBuilder();

    data.setOptionalBool(true);
    data.setOptionalBytes(ByteString.copyFrom("someText", "UTF-8"));
    data.setOptionalDouble(0.577);
    data.setOptionalFloat(3.1415f);
    data.setOptionalEnum(SchemaConverterAllDatatypes.TestEnum.FIRST);
    data.setOptionalFixed32(1000 * 1000 * 1);
    data.setOptionalFixed64(1000 * 1000 * 1000 * 2);
    data.setOptionalInt32(1000 * 1000 * 3);
    data.setOptionalInt64(1000L * 1000 * 1000 * 4);
    data.setOptionalSFixed32(1000 * 1000 * 5);
    data.setOptionalSFixed64(1000L * 1000 * 1000 * 6);
    data.setOptionalSInt32(1000 * 1000 * 56);
    data.setOptionalSInt64(1000L * 1000 * 1000 * 7);
    data.setOptionalString("Good Will Hunting");
    data.setOptionalUInt32(1000 * 1000 * 8);
    data.setOptionalUInt64(1000L * 1000 * 1000 * 9);
    data.getOptionalMessageBuilder().setSomeId(1984);
    data.getPbGroupBuilder().setGroupInt(1492);

    SchemaConverterAllDatatypes dataBuilt = data.build();
    data.clear();

    List<TestProtobuf.SchemaConverterAllDatatypes> result;
    result = testData(dataBuilt);

    // data are fully checked in testData function. Lets do one more check.
    SchemaConverterAllDatatypes o = result.get(0);
    assertThat(o.getOptionalString()).isEqualTo("Good Will Hunting");

    assertThat(o.getOptionalBool()).isTrue();
    assertThat(o.getOptionalBytes()).isEqualTo(ByteString.copyFrom("someText", "UTF-8"));
    assertThat(o.getOptionalDouble()).isCloseTo(0.577, offset(0.00001));
    assertThat(o.getOptionalFloat()).isCloseTo(3.1415f, offset(0.00001f));
    assertThat(o.getOptionalEnum()).isEqualTo(SchemaConverterAllDatatypes.TestEnum.FIRST);
    assertThat(o.getOptionalFixed32()).isEqualTo(1000 * 1000 * 1);
    assertThat(o.getOptionalFixed64()).isEqualTo(1000 * 1000 * 1000 * 2);
    assertThat(o.getOptionalInt32()).isEqualTo(1000 * 1000 * 3);
    assertThat(o.getOptionalInt64()).isEqualTo(1000L * 1000 * 1000 * 4);
    assertThat(o.getOptionalSFixed32()).isEqualTo(1000 * 1000 * 5);
    assertThat(o.getOptionalSFixed64()).isEqualTo(1000L * 1000 * 1000 * 6);
    assertThat(o.getOptionalSInt32()).isEqualTo(1000 * 1000 * 56);
    assertThat(o.getOptionalSInt64()).isEqualTo(1000L * 1000 * 1000 * 7);
    assertThat(o.getOptionalUInt32()).isEqualTo(1000 * 1000 * 8);
    assertThat(o.getOptionalUInt64()).isEqualTo(1000L * 1000 * 1000 * 9);
    assertThat(o.getOptionalMessage().getSomeId()).isEqualTo(1984);
    assertThat(o.getPbGroup().getGroupInt()).isEqualTo(1492);
  }

  @Test
  public void testProto3AllTypes() throws Exception {
    TestProto3.SchemaConverterAllDatatypes.Builder data;
    data = TestProto3.SchemaConverterAllDatatypes.newBuilder();

    data.setOptionalBool(true);
    data.setOptionalBytes(ByteString.copyFrom("someText", "UTF-8"));
    data.setOptionalDouble(0.577);
    data.setOptionalFloat(3.1415f);
    data.setOptionalEnum(TestProto3.SchemaConverterAllDatatypes.TestEnum.FIRST);
    data.setOptionalFixed32(1000 * 1000 * 1);
    data.setOptionalFixed64(1000 * 1000 * 1000 * 2);
    data.setOptionalInt32(1000 * 1000 * 3);
    data.setOptionalInt64(1000L * 1000 * 1000 * 4);
    data.setOptionalSFixed32(1000 * 1000 * 5);
    data.setOptionalSFixed64(1000L * 1000 * 1000 * 6);
    data.setOptionalSInt32(1000 * 1000 * 56);
    data.setOptionalSInt64(1000L * 1000 * 1000 * 7);
    data.setOptionalString("Good Will Hunting");
    data.setOptionalUInt32(1000 * 1000 * 8);
    data.setOptionalUInt64(1000L * 1000 * 1000 * 9);
    data.getOptionalMessageBuilder().setSomeId(1984);

    TestProto3.SchemaConverterAllDatatypes dataBuilt = data.build();
    data.clear();

    List<TestProto3.SchemaConverterAllDatatypes> result;
    result = testData(dataBuilt);

    // data are fully checked in testData function. Lets do one more check.
    TestProto3.SchemaConverterAllDatatypes o = result.get(0);
    assertThat(o.getOptionalString()).isEqualTo("Good Will Hunting");

    assertThat(o.getOptionalBool()).isTrue();
    assertThat(o.getOptionalBytes()).isEqualTo(ByteString.copyFrom("someText", "UTF-8"));
    assertThat(o.getOptionalDouble()).isCloseTo(0.577, offset(0.00001));
    assertThat(o.getOptionalFloat()).isCloseTo(3.1415f, offset(0.00001f));
    assertThat(o.getOptionalEnum()).isEqualTo(TestProto3.SchemaConverterAllDatatypes.TestEnum.FIRST);
    assertThat(o.getOptionalFixed32()).isEqualTo(1000 * 1000 * 1);
    assertThat(o.getOptionalFixed64()).isEqualTo(1000 * 1000 * 1000 * 2);
    assertThat(o.getOptionalInt32()).isEqualTo(1000 * 1000 * 3);
    assertThat(o.getOptionalInt64()).isEqualTo(1000L * 1000 * 1000 * 4);
    assertThat(o.getOptionalSFixed32()).isEqualTo(1000 * 1000 * 5);
    assertThat(o.getOptionalSFixed64()).isEqualTo(1000L * 1000 * 1000 * 6);
    assertThat(o.getOptionalSInt32()).isEqualTo(1000 * 1000 * 56);
    assertThat(o.getOptionalSInt64()).isEqualTo(1000L * 1000 * 1000 * 7);
    assertThat(o.getOptionalUInt32()).isEqualTo(1000 * 1000 * 8);
    assertThat(o.getOptionalUInt64()).isEqualTo(1000L * 1000 * 1000 * 9);
    assertThat(o.getOptionalMessage().getSomeId()).isEqualTo(1984);
  }

  @Test
  public void testAllTypesMultiple() throws Exception {
    int count = 100;
    SchemaConverterAllDatatypes[] input = new SchemaConverterAllDatatypes[count];

    for (int i = 0; i < count; i++) {
      SchemaConverterAllDatatypes.Builder d = SchemaConverterAllDatatypes.newBuilder();

      if (i % 2 != 0) d.setOptionalBool(true);
      if (i % 3 != 0) d.setOptionalBytes(ByteString.copyFrom("someText " + i, "UTF-8"));
      if (i % 4 != 0) d.setOptionalDouble(0.577 * i);
      if (i % 5 != 0) d.setOptionalFloat(3.1415f * i);
      if (i % 6 != 0) d.setOptionalEnum(SchemaConverterAllDatatypes.TestEnum.FIRST);
      if (i % 7 != 0) d.setOptionalFixed32(1000 * i * 1);
      if (i % 8 != 0) d.setOptionalFixed64(1000 * i * 1000 * 2);
      if (i % 9 != 0) d.setOptionalInt32(1000 * i * 3);
      if (i % 2 != 1) d.setOptionalSFixed32(1000 * i * 5);
      if (i % 3 != 1) d.setOptionalSFixed64(1000 * i * 1000 * 6);
      if (i % 4 != 1) d.setOptionalSInt32(1000 * i * 56);
      if (i % 5 != 1) d.setOptionalSInt64(1000 * i * 1000 * 7);
      if (i % 6 != 1) d.setOptionalString("Good Will Hunting " + i);
      if (i % 7 != 1) d.setOptionalUInt32(1000 * i * 8);
      if (i % 8 != 1) d.setOptionalUInt64(1000 * i * 1000 * 9);
      if (i % 9 != 1) d.getOptionalMessageBuilder().setSomeId(1984 * i);
      if (i % 2 != 1) d.getPbGroupBuilder().setGroupInt(1492 * i);
      if (i % 3 != 1) d.setOptionalInt64(1000 * i * 1000 * 4);
      input[i] = d.build();
    }

    List<TestProtobuf.SchemaConverterAllDatatypes> result;
    result = testData(input);

    // data are fully checked in testData function. Lets do one more check.
    assertThat(result.get(0).getOptionalString()).isEqualTo("Good Will Hunting 0");
    assertThat(result.get(90).getOptionalString()).isEqualTo("Good Will Hunting 90");
  }

  @Test
  public void testProto3AllTypesMultiple() throws Exception {
    int count = 100;
    TestProto3.SchemaConverterAllDatatypes[] input = new TestProto3.SchemaConverterAllDatatypes[count];

    for (int i = 0; i < count; i++) {
      TestProto3.SchemaConverterAllDatatypes.Builder d = TestProto3.SchemaConverterAllDatatypes.newBuilder();

      if (i % 2 != 0) d.setOptionalBool(true);
      if (i % 3 != 0) d.setOptionalBytes(ByteString.copyFrom("someText " + i, "UTF-8"));
      if (i % 4 != 0) d.setOptionalDouble(0.577 * i);
      if (i % 5 != 0) d.setOptionalFloat(3.1415f * i);
      if (i % 6 != 0) d.setOptionalEnum(TestProto3.SchemaConverterAllDatatypes.TestEnum.FIRST);
      if (i % 7 != 0) d.setOptionalFixed32(1000 * i * 1);
      if (i % 8 != 0) d.setOptionalFixed64(1000 * i * 1000 * 2);
      if (i % 9 != 0) d.setOptionalInt32(1000 * i * 3);
      if (i % 2 != 1) d.setOptionalSFixed32(1000 * i * 5);
      if (i % 3 != 1) d.setOptionalSFixed64(1000 * i * 1000 * 6);
      if (i % 4 != 1) d.setOptionalSInt32(1000 * i * 56);
      if (i % 5 != 1) d.setOptionalSInt64(1000 * i * 1000 * 7);
      if (i % 6 != 1) d.setOptionalString("Good Will Hunting " + i);
      if (i % 7 != 1) d.setOptionalUInt32(1000 * i * 8);
      if (i % 8 != 1) d.setOptionalUInt64(1000 * i * 1000 * 9);
      if (i % 9 != 1) d.getOptionalMessageBuilder().setSomeId(1984 * i);
      if (i % 3 != 1) d.setOptionalInt64(1000 * i * 1000 * 4);
      input[i] = d.build();
    }

    List<TestProto3.SchemaConverterAllDatatypes> result;
    result = testData(input);

    // data are fully checked in testData function. Lets do one more check.
    assertThat(result.get(0).getOptionalString()).isEqualTo("Good Will Hunting 0");
    assertThat(result.get(90).getOptionalString()).isEqualTo("Good Will Hunting 90");
  }

  @Test
  public void testDefaults() throws Exception {
    SchemaConverterAllDatatypes.Builder data;
    data = SchemaConverterAllDatatypes.newBuilder();

    List<SchemaConverterAllDatatypes> result = testData(data.build());
    SchemaConverterAllDatatypes message = result.get(0);
    assertThat(message.getOptionalString()).isEqualTo("");
    assertThat(message.getOptionalBool()).isFalse();
    assertThat(message.getOptionalFixed32()).isEqualTo(0);
  }

  @Test
  public void testProto3Defaults() throws Exception {
    TestProto3.SchemaConverterAllDatatypes.Builder data;
    data = TestProto3.SchemaConverterAllDatatypes.newBuilder();

    List<TestProto3.SchemaConverterAllDatatypes> result = testData(data.build());
    TestProto3.SchemaConverterAllDatatypes message = result.get(0);
    assertThat(message.getOptionalString()).isEqualTo("");
    assertThat(message.getOptionalBool()).isFalse();
    assertThat(message.getOptionalFixed32()).isEqualTo(0);
  }

  @Test
  public void testRepeatedMessages() throws Exception {
    TestProtobuf.TopMessage.Builder top = TestProtobuf.TopMessage.newBuilder();
    top.addInnerBuilder().setOne("First inner");
    top.addInnerBuilder().setTwo("Second inner");
    top.addInnerBuilder().setThree("Third inner");

    TestProtobuf.TopMessage result = testData(top.build()).get(0);

    assertThat(result.getInnerCount()).isEqualTo(3);

    TestProtobuf.InnerMessage first = result.getInner(0);
    TestProtobuf.InnerMessage second = result.getInner(1);
    TestProtobuf.InnerMessage third = result.getInner(2);

    assertThat(first.getOne()).isEqualTo("First inner");
    assertThat(first.hasTwo()).isFalse();
    assertThat(first.hasThree()).isFalse();

    assertThat(second.getTwo()).isEqualTo("Second inner");
    assertThat(second.hasOne()).isFalse();
    assertThat(second.hasThree()).isFalse();

    assertThat(third.getThree()).isEqualTo("Third inner");
    assertThat(third.hasOne()).isFalse();
    assertThat(third.hasTwo()).isFalse();
  }

  @Test
  public void testProto3RepeatedMessages() throws Exception {
    TestProto3.TopMessage.Builder top = TestProto3.TopMessage.newBuilder();
    top.addInnerBuilder().setOne("First inner");
    top.addInnerBuilder().setTwo("Second inner");
    top.addInnerBuilder().setThree("Third inner");

    TestProto3.TopMessage result = testData(top.build()).get(0);

    assertThat(result.getInnerCount()).isEqualTo(3);

    TestProto3.InnerMessage first = result.getInner(0);
    TestProto3.InnerMessage second = result.getInner(1);
    TestProto3.InnerMessage third = result.getInner(2);

    assertThat(first.getOne()).isEqualTo("First inner");
    assertThat(first.getTwo()).isEmpty();
    assertThat(first.getThree()).isEmpty();

    assertThat(second.getTwo()).isEqualTo("Second inner");
    assertThat(second.getOne()).isEmpty();
    assertThat(second.getThree()).isEmpty();

    assertThat(third.getThree()).isEqualTo("Third inner");
    assertThat(third.getOne()).isEmpty();
    assertThat(third.getTwo()).isEmpty();
  }

  @Test
  public void testRepeatedInt() throws Exception {
    TestProtobuf.RepeatedIntMessage.Builder top = TestProtobuf.RepeatedIntMessage.newBuilder();

    top.addRepeatedInt(1);
    top.addRepeatedInt(2);
    top.addRepeatedInt(3);

    TestProtobuf.RepeatedIntMessage result = testData(top.build()).get(0);

    assertThat(result.getRepeatedIntCount()).isEqualTo(3);

    assertThat(result.getRepeatedInt(0)).isEqualTo(1);
    assertThat(result.getRepeatedInt(1)).isEqualTo(2);
    assertThat(result.getRepeatedInt(2)).isEqualTo(3);
  }

  @Test
  public void testProto3RepeatedInt() throws Exception {
    TestProto3.RepeatedIntMessage.Builder top = TestProto3.RepeatedIntMessage.newBuilder();

    top.addRepeatedInt(1);
    top.addRepeatedInt(2);
    top.addRepeatedInt(3);

    TestProto3.RepeatedIntMessage result = testData(top.build()).get(0);

    assertThat(result.getRepeatedIntCount()).isEqualTo(3);

    assertThat(result.getRepeatedInt(0)).isEqualTo(1);
    assertThat(result.getRepeatedInt(1)).isEqualTo(2);
    assertThat(result.getRepeatedInt(2)).isEqualTo(3);
  }

  @Test
  public void testLargeProtobufferFieldId() throws Exception {
    TestProtobuf.HighIndexMessage.Builder builder = TestProtobuf.HighIndexMessage.newBuilder();
    builder.addRepeatedInt(1);
    builder.addRepeatedInt(2);

    testData(builder.build());
  }

  @Test
  public void testProto3LargeProtobufferFieldId() throws Exception {
    TestProto3.HighIndexMessage.Builder builder = TestProto3.HighIndexMessage.newBuilder();
    builder.addRepeatedInt(1);
    builder.addRepeatedInt(2);

    testData(builder.build());
  }

  @Test
  public void testUnknownEnum() throws Exception {
    TestProto3.SchemaConverterAllDatatypes.Builder data;
    data = TestProto3.SchemaConverterAllDatatypes.newBuilder();
    data.setOptionalEnumValue(42);

    TestProto3.SchemaConverterAllDatatypes dataBuilt = data.build();
    data.clear();

    List<TestProto3.SchemaConverterAllDatatypes> result;
    result = testData(dataBuilt);

    // data are fully checked in testData function. Lets do one more check.
    TestProto3.SchemaConverterAllDatatypes o = result.get(0);
    assertThat(o.getOptionalEnum()).isSameAs(TestProto3.SchemaConverterAllDatatypes.TestEnum.UNRECOGNIZED);
    assertThat(o.getOptionalEnumValue()).isEqualTo(42);
  }
}
