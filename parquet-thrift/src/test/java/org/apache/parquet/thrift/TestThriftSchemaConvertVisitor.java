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

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Type;

import org.apache.parquet.thrift.projection.FieldProjectionFilter;
import org.apache.parquet.thrift.struct.ThriftField;
import org.apache.parquet.thrift.struct.ThriftType;
import org.apache.parquet.thrift.struct.ThriftType.StructType;
import org.apache.parquet.thrift.struct.ThriftType.StructType.StructOrUnionType;

import org.junit.Test;

import static org.apache.parquet.schema.Type.Repetition;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.thrift.struct.ThriftField.Requirement;
import static org.junit.Assert.assertEquals;

public class TestThriftSchemaConvertVisitor {

  @Test
  public void testConvertBasicI64Type() {
    String fieldName = "i64Type";
    Short fieldId = 0;

    ThriftType.I64Type timestampI64Type = new ThriftType.I64Type();
    ThriftField field = new ThriftField(fieldName, fieldId, Requirement.REQUIRED, timestampI64Type);
    List<ThriftField> thriftFields = new ArrayList<ThriftField>(1);
    thriftFields.add(field);
    StructType thriftStruct = new StructType(thriftFields, StructOrUnionType.STRUCT);
    MessageType actual = ThriftSchemaConvertVisitor.convert(thriftStruct, FieldProjectionFilter.ALL_COLUMNS);

    Type expectedParquetField = Types.primitive(PrimitiveTypeName.INT64, Repetition.REQUIRED)
      .named(fieldName)
      .withId(fieldId);

    MessageType expected = Types
      .buildMessage()
      .addFields(expectedParquetField)
      .named("ParquetSchema");

    assertEquals(expected, actual);
  }

  @Test
  public void testConvertLogicalI64Type() {
    LogicalTypeAnnotation timestampLogicalType = timestampType(true, TimeUnit.MILLIS);
    String fieldName = "logicalI64Type";
    Short fieldId = 0;

    ThriftType.I64Type timestampI64Type = new ThriftType.I64Type();
    timestampI64Type.setLogicalTypeAnnotation(timestampLogicalType);
    ThriftField field = new ThriftField(fieldName, fieldId, Requirement.REQUIRED, timestampI64Type);
    List<ThriftField> thriftFields = new ArrayList<ThriftField>(1);
    thriftFields.add(field);
    StructType thriftStruct = new StructType(thriftFields, StructOrUnionType.STRUCT);
    MessageType actual = ThriftSchemaConvertVisitor.convert(thriftStruct, FieldProjectionFilter.ALL_COLUMNS);

    Type expectedParquetField = Types.primitive(PrimitiveTypeName.INT64, Repetition.REQUIRED)
      .as(timestampLogicalType)
      .named(fieldName)
      .withId(fieldId);

    MessageType expected = Types
      .buildMessage()
      .addFields(expectedParquetField)
      .named("ParquetSchema");

    assertEquals(expected, actual);
  }
}
