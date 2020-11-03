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
import static org.apache.parquet.thrift.struct.ThriftField.Requirement;
import static org.junit.Assert.assertEquals;

public class TestThriftSchemaConvertVisitor {

  private MessageType buildOneFieldParquetMessage(Type expectedParquetField) {
    return Types.buildMessage()
      .addFields(expectedParquetField)
      .named("ParquetSchema");
  }

  private StructType buildOneFieldThriftStructType(String fieldName, Short fieldId, ThriftType thriftType) {
    ThriftField inputThriftField = new ThriftField(fieldName, fieldId, Requirement.REQUIRED, thriftType);
    List<ThriftField> fields = new ArrayList<ThriftField>(1);
    fields.add(inputThriftField);
    return new StructType(fields, StructOrUnionType.STRUCT);
  }

  @Test
  public void testConvertBasicI32Type() {
    String fieldName = "i32Type";
    Short fieldId = 0;

    ThriftType i32Type = new ThriftType.I32Type();

    StructType thriftStruct = buildOneFieldThriftStructType(fieldName, fieldId, i32Type);
    MessageType actual = ThriftSchemaConvertVisitor.convert(thriftStruct, FieldProjectionFilter.ALL_COLUMNS);

    Type expectedParquetField = Types.primitive(PrimitiveTypeName.INT32, Repetition.REQUIRED)
      .named(fieldName)
      .withId(fieldId);
    MessageType expected = buildOneFieldParquetMessage(expectedParquetField);

    assertEquals(expected, actual);
  }

  @Test
  public void testConvertLogicalI32Type() {
    LogicalTypeAnnotation timeLogicalType = LogicalTypeAnnotation.timeType(true, TimeUnit.MILLIS);
    String fieldName = "timeI32Type";
    Short fieldId = 0;

    ThriftType timeI32Type = new ThriftType.I32Type();
    timeI32Type.setLogicalTypeAnnotation(timeLogicalType);

    StructType thriftStruct = buildOneFieldThriftStructType(fieldName, fieldId, timeI32Type);
    MessageType actual = ThriftSchemaConvertVisitor.convert(thriftStruct, FieldProjectionFilter.ALL_COLUMNS);

    Type expectedParquetField = Types.primitive(PrimitiveTypeName.INT32, Repetition.REQUIRED)
      .as(timeLogicalType)
      .named(fieldName)
      .withId(fieldId);
    MessageType expected = buildOneFieldParquetMessage(expectedParquetField);

    assertEquals(expected, actual);
  }

  @Test
  public void testConvertBasicI64Type() {
    String fieldName = "i64Type";
    Short fieldId = 0;

    ThriftType i64Type = new ThriftType.I64Type();
    
    StructType thriftStruct = buildOneFieldThriftStructType(fieldName, fieldId, i64Type);
    MessageType actual = ThriftSchemaConvertVisitor.convert(thriftStruct, FieldProjectionFilter.ALL_COLUMNS);

    Type expectedParquetField = Types.primitive(PrimitiveTypeName.INT64, Repetition.REQUIRED)
      .named(fieldName)
      .withId(fieldId);
    MessageType expected = buildOneFieldParquetMessage(expectedParquetField);

    assertEquals(expected, actual);
  }

  @Test
  public void testConvertLogicalI64Type() {
    LogicalTypeAnnotation timestampLogicalType = LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS);
    String fieldName = "logicalI64Type";
    Short fieldId = 0;

    ThriftType timestampI64Type = new ThriftType.I64Type();
    timestampI64Type.setLogicalTypeAnnotation(timestampLogicalType);
    
    StructType thriftStruct = buildOneFieldThriftStructType(fieldName, fieldId, timestampI64Type);
    MessageType actual = ThriftSchemaConvertVisitor.convert(thriftStruct, FieldProjectionFilter.ALL_COLUMNS);

    Type expectedParquetField = Types.primitive(PrimitiveTypeName.INT64, Repetition.REQUIRED)
      .as(timestampLogicalType)
      .named(fieldName)
      .withId(fieldId);
    MessageType expected = buildOneFieldParquetMessage(expectedParquetField);

    assertEquals(expected, actual);
  }

  @Test
  public void testConvertStringType() {
    LogicalTypeAnnotation stringLogicalType = LogicalTypeAnnotation.stringType();
    String fieldName = "stringType";
    Short fieldId = 0;

    ThriftType stringType = new ThriftType.StringType();
    
    StructType thriftStruct = buildOneFieldThriftStructType(fieldName, fieldId, stringType);
    MessageType actual = ThriftSchemaConvertVisitor.convert(thriftStruct, FieldProjectionFilter.ALL_COLUMNS);

    Type expectedParquetField = Types.primitive(PrimitiveTypeName.BINARY, Repetition.REQUIRED)
      .as(stringLogicalType)
      .named(fieldName)
      .withId(fieldId);
    MessageType expected = buildOneFieldParquetMessage(expectedParquetField);

    assertEquals(expected, actual);

  }

  @Test
  public void testConvertLogicalBinaryType() {
    LogicalTypeAnnotation jsonLogicalType = LogicalTypeAnnotation.jsonType();
    String fieldName = "logicalBinaryType";
    Short fieldId = 0;

    ThriftType.StringType jsonBinaryType = new ThriftType.StringType();
    jsonBinaryType.setBinary(true);
    jsonBinaryType.setLogicalTypeAnnotation(jsonLogicalType);
    
    StructType thriftStruct = buildOneFieldThriftStructType(fieldName, fieldId, jsonBinaryType);
    MessageType actual = ThriftSchemaConvertVisitor.convert(thriftStruct, FieldProjectionFilter.ALL_COLUMNS);

    Type expectedParquetField = Types.primitive(PrimitiveTypeName.BINARY, Repetition.REQUIRED)
      .as(jsonLogicalType)
      .named(fieldName)
      .withId(fieldId);
    MessageType expected = buildOneFieldParquetMessage(expectedParquetField);

    assertEquals(expected, actual);
  }
}
