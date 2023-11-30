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
package org.apache.parquet.thrift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.thrift.ThriftRecordConverter.FieldEnumConverter;
import org.apache.parquet.thrift.struct.ThriftField;
import org.apache.parquet.thrift.struct.ThriftField.Requirement;
import org.apache.parquet.thrift.struct.ThriftType;
import org.apache.parquet.thrift.struct.ThriftType.EnumType;
import org.apache.parquet.thrift.struct.ThriftType.EnumValue;
import org.apache.parquet.thrift.struct.ThriftType.StructType;
import org.apache.parquet.thrift.test.compat.StructWithUnionV1;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.junit.Test;

public class TestThriftRecordConverter {
  @Test
  public void testUnknownEnumThrowsGoodException() throws Exception {
    EnumType et = new EnumType(Arrays.asList(new EnumValue(77, "hello")));
    ThriftField field = new ThriftField("name", (short) 1, Requirement.REQUIRED, et);

    ArrayList<TProtocol> events = new ArrayList<TProtocol>();

    FieldEnumConverter conv = new FieldEnumConverter(events, field);

    conv.addBinary(Binary.fromString("hello"));

    assertEquals(1, events.size());
    assertEquals(77, events.get(0).readI32());

    try {
      conv.addBinary(Binary.fromString("FAKE_ENUM_VALUE"));
      fail("this should throw");
    } catch (ParquetDecodingException e) {
      assertEquals(
          ("Unrecognized enum value: FAKE_ENUM_VALUE known values: {Binary{\"hello\"}=77} in {\n"
                  + "  \"name\" : \"name\",\n"
                  + "  \"fieldId\" : 1,\n"
                  + "  \"requirement\" : \"REQUIRED\",\n"
                  + "  \"type\" : {\n"
                  + "    \"id\" : \"ENUM\",\n"
                  + "    \"values\" : [ {\n"
                  + "      \"id\" : 77,\n"
                  + "      \"name\" : \"hello\"\n"
                  + "    } ],\n"
                  + "    \"logicalTypeAnnotation\" : null\n"
                  + "  }\n"
                  + "}")
              .replace("\n", System.lineSeparator()),
          e.getMessage());
    }
  }

  @Test
  public void constructorDoesNotRequireStructOrUnionTypeMeta() throws Exception {
    String jsonWithNoStructOrUnionMeta = String.join(
        "\n",
        Files.readAllLines(
            new File(
                    "src/test/resources/org/apache/parquet/thrift/StructWithUnionV1NoStructOrUnionMeta.json")
                .toPath(),
            StandardCharsets.UTF_8));

    StructType noStructOrUnionMeta = (StructType) ThriftType.fromJSON(jsonWithNoStructOrUnionMeta);

    // this used to throw, see PARQUET-346
    new ThriftRecordConverter<StructWithUnionV1>(
        new ThriftReader<StructWithUnionV1>() {
          @Override
          public StructWithUnionV1 readOneRecord(TProtocol protocol) throws TException {
            return null;
          }
        },
        "name",
        new ThriftSchemaConverter().convert(StructWithUnionV1.class),
        noStructOrUnionMeta);
  }
}
