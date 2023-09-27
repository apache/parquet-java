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

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.thrift.projection.StrictFieldProjectionFilter;
import org.apache.parquet.thrift.projection.deprecated.DeprecatedFieldProjectionFilter;
import org.apache.parquet.thrift.struct.ThriftField;
import org.apache.parquet.thrift.struct.ThriftType;
import org.apache.parquet.thrift.struct.ThriftType.StructType;
import org.apache.thrift.TBase;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.apache.parquet.thrift.struct.ThriftField.Requirement.REQUIRED;
import static org.junit.Assert.assertEquals;

public class TestThriftSchemaConverter {
  @Test
  public void testConvertStructCreatedViaDeprecatedConstructor() {
    String expected = "message ParquetSchema {\n" +
      "  required binary a (UTF8) = 1;\n" +
      "  required binary b (UTF8) = 2;\n" +
      "}\n";

    ThriftSchemaConverter converter = new ThriftSchemaConverter();

    StructType structType = new StructType(
      Arrays.asList(
        new ThriftField("a", (short) 1, REQUIRED, new ThriftType.StringType()),
        new ThriftField("b", (short) 2, REQUIRED, new ThriftType.StringType())
      )
    );

    final MessageType converted = converter.convert(structType);
    assertEquals(MessageTypeParser.parseMessageType(expected), converted);
  }

  public static void shouldGetProjectedSchema(String deprecatedFilterDesc, String strictFilterDesc, String expectedSchemaStr, Class<? extends TBase<?,?>> thriftClass) {
    MessageType depRequestedSchema = getDeprecatedFilteredSchema(deprecatedFilterDesc, thriftClass);
    MessageType strictRequestedSchema = getStrictFilteredSchema(strictFilterDesc, thriftClass);
    MessageType expectedSchema = parseMessageType(expectedSchemaStr);
    assertEquals(expectedSchema, depRequestedSchema);
    assertEquals(expectedSchema, strictRequestedSchema);
  }

  private static MessageType getDeprecatedFilteredSchema(String filterDesc, Class<? extends TBase<?,?>> thriftClass) {
    DeprecatedFieldProjectionFilter fieldProjectionFilter = new DeprecatedFieldProjectionFilter(filterDesc);
    return new ThriftSchemaConverter(fieldProjectionFilter).convert(thriftClass);
  }

  private static MessageType getStrictFilteredSchema(String semicolonDelimitedString, Class<? extends TBase<?,?>> thriftClass) {
    StrictFieldProjectionFilter fieldProjectionFilter = StrictFieldProjectionFilter.fromSemicolonDelimitedString(semicolonDelimitedString);
    return new ThriftSchemaConverter(fieldProjectionFilter).convert(thriftClass);
  }
}
