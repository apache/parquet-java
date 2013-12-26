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

import com.google.protobuf.ByteString;
import org.junit.Test;
import parquet.proto.test.TestProtobuf;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static parquet.proto.TestUtils.testData;
import static parquet.proto.test.TestProtobuf.SchemaConverterAllDatatypes;

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

    //data are fully checked in testData function. Lets do one more check.
    SchemaConverterAllDatatypes o = result.get(0);
    assertEquals("Good Will Hunting", o.getOptionalString());

    assertEquals(true, o.getOptionalBool());
    assertEquals(ByteString.copyFrom("someText", "UTF-8"), o.getOptionalBytes());
    assertEquals(0.577, o.getOptionalDouble(), 0.00001);
    assertEquals(3.1415f, o.getOptionalFloat(), 0.00001);
    assertEquals(SchemaConverterAllDatatypes.TestEnum.FIRST, o.getOptionalEnum());
    assertEquals(1000 * 1000 * 1, o.getOptionalFixed32());
    assertEquals(1000 * 1000 * 1000 * 2, o.getOptionalFixed64());
    assertEquals(1000 * 1000 * 3, o.getOptionalInt32());
    assertEquals(1000L * 1000 * 1000 * 4, o.getOptionalInt64());
    assertEquals(1000 * 1000 * 5, o.getOptionalSFixed32());
    assertEquals(1000L * 1000 * 1000 * 6, o.getOptionalSFixed64());
    assertEquals(1000 * 1000 * 56, o.getOptionalSInt32());
    assertEquals(1000L * 1000 * 1000 * 7, o.getOptionalSInt64());
    assertEquals(1000 * 1000 * 8, o.getOptionalUInt32());
    assertEquals(1000L * 1000 * 1000 * 9, o.getOptionalUInt64());
    assertEquals(1984, o.getOptionalMessage().getSomeId());
    assertEquals(1492, o.getPbGroup().getGroupInt());
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

    //data are fully checked in testData function. Lets do one more check.
    assertEquals("Good Will Hunting 0", result.get(0).getOptionalString());
    assertEquals("Good Will Hunting 90", result.get(90).getOptionalString());
  }


  @Test
  public void testDefaults() throws Exception {
    SchemaConverterAllDatatypes.Builder data;
    data = SchemaConverterAllDatatypes.newBuilder();

    List<SchemaConverterAllDatatypes> result = testData(data.build());
    SchemaConverterAllDatatypes message = result.get(0);
    assertEquals("", message.getOptionalString());
    assertEquals(false, message.getOptionalBool());
    assertEquals(0, message.getOptionalFixed32());
  }
}
