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
package org.apache.parquet.io;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.lang.reflect.Field;
import java.util.Deque;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class ValidatingRecordConsumerTest {
  private static MessageType vectorSchema(String element) {
    return MessageTypeParser.parseMessageType("message m { required group v (VECTOR(2)) {"
        + " repeated group list { required " + element + "; } } }");
  }

  private static void startVector(RecordConsumer c) {
    c.startField("v", 0);
    c.startGroup();
  }

  private static void startEntry(RecordConsumer c) {
    c.startGroup();
    c.startField("element", 0);
  }

  private static void endEntry(RecordConsumer c) {
    c.endField("element", 0);
    c.endGroup();
  }

  private static void addEntry(RecordConsumer c, float value) {
    startEntry(c);
    c.addFloat(value);
    endEntry(c);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 3})
  public void testVectorLengthRejectedAtBoundary(int count) {
    MessageType schema = vectorSchema("float element");
    RecordConsumer delegate = mock(RecordConsumer.class);
    ValidatingRecordConsumer c = new ValidatingRecordConsumer(delegate, schema);
    c.startMessage();
    startVector(c);
    c.startField("list", 0);
    for (int i = 0; i < Math.min(count, 2); i++) addEntry(c, i);
    if (count == 1) {
      c.endField("list", 0);
      clearInvocations(delegate);
      assertThatThrownBy(c::endGroup)
          .isInstanceOf(InvalidRecordException.class)
          .hasMessage("Invalid VECTOR length for v: expected 2, got 1");
      verifyNoInteractions(delegate);
    } else {
      clearInvocations(delegate);
      assertThatThrownBy(c::startGroup)
          .isInstanceOf(InvalidRecordException.class)
          .hasMessage("Invalid VECTOR length for v: expected 2, got 3");
      verifyNoInteractions(delegate);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"v", "element"})
  public void testEmptyRequiredVectorField(String field) {
    MessageType schema = vectorSchema("float element");
    RecordConsumer delegate = mock(RecordConsumer.class);
    ValidatingRecordConsumer c = new ValidatingRecordConsumer(delegate, schema);
    c.startMessage();
    c.startField("v", 0);
    if (field.equals("element")) {
      c.startGroup();
      c.startField("list", 0);
      startEntry(c);
    }
    clearInvocations(delegate);
    assertThatThrownBy(() -> c.endField(field, 0))
        .isInstanceOf(InvalidRecordException.class)
        .hasMessage("required field is missing "
            + (field.equals("v") ? schema.getType("v") : schema.getType("v", "list", "element")));
    verifyNoInteractions(delegate);
  }

  private static Stream<Arguments> rejectedVectorElements() {
    return Stream.of(
        Arguments.of(
            "float element",
            (Consumer<RecordConsumer>) c -> c.addFloat(Float.NaN),
            "Non-finite VECTOR element in v"),
        Arguments.of(
            "double element",
            (Consumer<RecordConsumer>) c -> c.addDouble(Double.POSITIVE_INFINITY),
            "Non-finite VECTOR element in v"),
        Arguments.of(
            "fixed_len_byte_array(2) element (FLOAT16)",
            (Consumer<RecordConsumer>)
                c -> c.addBinary(Binary.fromConstantByteArray(new byte[] {0, (byte) 0xfc})),
            "Non-finite VECTOR element in v"),
        Arguments.of(
            "fixed_len_byte_array(2) element (FLOAT16)",
            (Consumer<RecordConsumer>) c -> c.addBinary(Binary.fromConstantByteArray(new byte[] {0})),
            "Invalid FLOAT16 VECTOR element length: 1"));
  }

  @ParameterizedTest
  @MethodSource("rejectedVectorElements")
  public void testVectorElementRejectedImmediately(String element, Consumer<RecordConsumer> add, String message) {
    MessageType schema = vectorSchema(element);
    RecordConsumer delegate = mock(RecordConsumer.class);
    ValidatingRecordConsumer c = new ValidatingRecordConsumer(delegate, schema);
    c.startMessage();
    startVector(c);
    c.startField("list", 0);
    startEntry(c);
    clearInvocations(delegate);
    assertThatThrownBy(() -> add.accept(c))
        .isInstanceOf(InvalidRecordException.class)
        .hasMessage(message);
    verifyNoInteractions(delegate);
  }

  private static final RecordConsumer NOOP = new RecordConsumer() {
    @Override
    public void startMessage() {}

    @Override
    public void endMessage() {}

    @Override
    public void startField(String field, int index) {}

    @Override
    public void endField(String field, int index) {}

    @Override
    public void startGroup() {}

    @Override
    public void endGroup() {}

    @Override
    public void addInteger(int value) {}

    @Override
    public void addLong(long value) {}

    @Override
    public void addBoolean(boolean value) {}

    @Override
    public void addBinary(Binary value) {}

    @Override
    public void addFloat(float value) {}

    @Override
    public void addDouble(double value) {}
  };

  @Test
  public void testNoPreviousFieldLeakAfterMessage() throws Exception {
    MessageType schema = new MessageType(
        "test", new PrimitiveType(REQUIRED, INT32, "a"), new PrimitiveType(REQUIRED, INT32, "b"));

    ValidatingRecordConsumer consumer = new ValidatingRecordConsumer(NOOP, schema);
    Field previousFieldField = ValidatingRecordConsumer.class.getDeclaredField("previousField");
    previousFieldField.setAccessible(true);

    for (int row = 0; row < 2; row++) {
      consumer.startMessage();
      consumer.startField("a", 0);
      consumer.addInteger(1);
      consumer.endField("a", 0);
      consumer.startField("b", 1);
      consumer.addInteger(2);
      consumer.endField("b", 1);
      consumer.endMessage();

      Deque<?> previousField = (Deque<?>) previousFieldField.get(consumer);
      assertThat(previousField)
          .as("previousField deque should be empty after endMessage (row " + row + ")")
          .isEmpty();
    }
  }
}
