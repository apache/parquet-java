/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop.example;

import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Before;
import org.junit.Test;

public class TestValidatingUnsignedIntegerRecordConsumer {

  private RecordConsumer mockDelegate;
  private ValidatingUnsignedIntegerRecordConsumer validator;

  @Before
  public void setUp() {
    mockDelegate = mock(RecordConsumer.class);
  }

  @Test
  public void testValidUint8Values() {
    MessageType schema = Types.buildMessage()
        .required(INT32)
        .as(intType(8, false))
        .named("uint8_field")
        .named("test_schema");

    validator = new ValidatingUnsignedIntegerRecordConsumer(mockDelegate, schema);

    validator.startMessage();
    validator.startField("uint8_field", 0);

    validator.addInteger(0);
    validator.addInteger(255);
    validator.addInteger(128);

    verify(mockDelegate).addInteger(0);
    verify(mockDelegate).addInteger(255);
    verify(mockDelegate).addInteger(128);
  }

  @Test
  public void testInvalidUint8Values() {
    MessageType schema = Types.buildMessage()
        .required(INT32)
        .as(intType(8, false))
        .named("uint8_field")
        .named("test_schema");

    validator = new ValidatingUnsignedIntegerRecordConsumer(mockDelegate, schema);

    validator.startMessage();
    validator.startField("uint8_field", 0);

    assertThrows(InvalidRecordException.class, () -> {
      validator.addInteger(-1);
    });

    assertThrows(InvalidRecordException.class, () -> {
      validator.addInteger(256);
    });
  }

  @Test
  public void testValidUint16Values() {
    MessageType schema = Types.buildMessage()
        .required(INT32)
        .as(intType(16, false))
        .named("uint16_field")
        .named("test_schema");

    validator = new ValidatingUnsignedIntegerRecordConsumer(mockDelegate, schema);

    validator.startMessage();
    validator.startField("uint16_field", 0);

    validator.addInteger(0);
    validator.addInteger(65535);
    validator.addInteger(32768);

    verify(mockDelegate).addInteger(0);
    verify(mockDelegate).addInteger(65535);
    verify(mockDelegate).addInteger(32768);
  }

  @Test
  public void testInvalidUint16Values() {
    MessageType schema = Types.buildMessage()
        .required(INT32)
        .as(intType(16, false))
        .named("uint16_field")
        .named("test_schema");

    validator = new ValidatingUnsignedIntegerRecordConsumer(mockDelegate, schema);

    validator.startMessage();
    validator.startField("uint16_field", 0);

    assertThrows(InvalidRecordException.class, () -> {
      validator.addInteger(-1);
    });

    assertThrows(InvalidRecordException.class, () -> {
      validator.addInteger(65536);
    });
  }

  @Test
  public void testValidUint32Values() {
    MessageType schema = Types.buildMessage()
        .required(INT32)
        .as(intType(32, false))
        .named("uint32_field")
        .named("test_schema");

    validator = new ValidatingUnsignedIntegerRecordConsumer(mockDelegate, schema);

    validator.startMessage();
    validator.startField("uint32_field", 0);

    validator.addInteger(0);
    validator.addInteger(Integer.MAX_VALUE);

    verify(mockDelegate).addInteger(0);
    verify(mockDelegate).addInteger(Integer.MAX_VALUE);
  }

  @Test
  public void testInvalidUint32Values() {
    MessageType schema = Types.buildMessage()
        .required(INT32)
        .as(intType(32, false))
        .named("uint32_field")
        .named("test_schema");

    validator = new ValidatingUnsignedIntegerRecordConsumer(mockDelegate, schema);

    validator.startMessage();
    validator.startField("uint32_field", 0);

    assertThrows(InvalidRecordException.class, () -> {
      validator.addInteger(-1);
    });
  }

  @Test
  public void testValidUint64Values() {
    MessageType schema = Types.buildMessage()
        .required(INT64)
        .as(intType(64, false))
        .named("uint64_field")
        .named("test_schema");

    validator = new ValidatingUnsignedIntegerRecordConsumer(mockDelegate, schema);

    validator.startMessage();
    validator.startField("uint64_field", 0);

    validator.addLong(0L);
    validator.addLong(Long.MAX_VALUE);

    verify(mockDelegate).addLong(0L);
    verify(mockDelegate).addLong(Long.MAX_VALUE);
  }

  @Test
  public void testInvalidUint64Values() {
    MessageType schema = Types.buildMessage()
        .required(INT64)
        .as(intType(64, false))
        .named("uint64_field")
        .named("test_schema");

    validator = new ValidatingUnsignedIntegerRecordConsumer(mockDelegate, schema);

    validator.startMessage();
    validator.startField("uint64_field", 0);

    assertThrows(InvalidRecordException.class, () -> {
      validator.addLong(-1L);
    });
  }

  @Test
  public void testSignedIntegerTypesNotValidated() {
    MessageType schema = Types.buildMessage()
        .required(INT32)
        .as(intType(32, true))
        .named("signed_field")
        .named("test_schema");

    validator = new ValidatingUnsignedIntegerRecordConsumer(mockDelegate, schema);

    validator.startMessage();
    validator.startField("signed_field", 0);

    validator.addInteger(-1);
    validator.addInteger(-100);

    verify(mockDelegate).addInteger(-1);
    verify(mockDelegate).addInteger(-100);
  }

  @Test
  public void testNonIntegerTypesIgnored() {
    MessageType schema = Types.buildMessage()
        .required(BINARY)
        .named("binary_field")
        .required(FLOAT)
        .named("float_field")
        .named("test_schema");

    validator = new ValidatingUnsignedIntegerRecordConsumer(mockDelegate, schema);

    validator.startMessage();

    validator.startField("binary_field", 0);
    validator.addBinary(Binary.fromString("test"));
    validator.endField("binary_field", 0);

    validator.startField("float_field", 1);
    validator.addFloat(-1.5f);
    validator.endField("float_field", 1);

    verify(mockDelegate).addBinary(Binary.fromString("test"));
    verify(mockDelegate).addFloat(-1.5f);
  }

  @Test
  public void testAllRecordConsumerMethodsDelegated() {
    MessageType schema =
        Types.buildMessage().required(INT32).named("test_field").named("test_schema");

    validator = new ValidatingUnsignedIntegerRecordConsumer(mockDelegate, schema);

    validator.startMessage();
    validator.startField("test", 0);
    validator.addBoolean(true);
    validator.addFloat(1.0f);
    validator.addDouble(1.0);
    validator.endField("test", 0);
    validator.startGroup();
    validator.endGroup();
    validator.endMessage();

    verify(mockDelegate).startMessage();
    verify(mockDelegate).startField("test", 0);
    verify(mockDelegate).addBoolean(true);
    verify(mockDelegate).addFloat(1.0f);
    verify(mockDelegate).addDouble(1.0);
    verify(mockDelegate).endField("test", 0);
    verify(mockDelegate).startGroup();
    verify(mockDelegate).endGroup();
    verify(mockDelegate).endMessage();
  }
}
