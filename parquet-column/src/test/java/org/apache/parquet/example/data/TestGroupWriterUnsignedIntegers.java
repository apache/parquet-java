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
package org.apache.parquet.example.data;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;

class TestGroupWriterUnsignedIntegers {
  @Test
  void rejectsOutOfRangeUnsignedIntegers() {
    for (String annotation : new String[] {"UINT_8", "UINT_16"}) {
      MessageType schema =
          MessageTypeParser.parseMessageType("message m { repeated int32 value (" + annotation + "); }");
      for (int value : new int[] {-1, annotation.equals("UINT_8") ? 256 : 65536}) {
        Group group = new SimpleGroupFactory(schema).newGroup().append("value", value);
        GroupWriter writer = new GroupWriter(mock(RecordConsumer.class), schema);
        assertThatThrownBy(() -> writer.write(group))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("value")
            .hasMessageContaining(Integer.toString(value));
      }
    }
  }

  @Test
  void preservesUnsignedBoundaries() {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message m { repeated int32 small (UINT_8); repeated int32 large (UINT_16); required int32 full (UINT_32); }");
    Group group = new SimpleGroupFactory(schema)
        .newGroup()
        .append("small", 0)
        .append("small", 255)
        .append("large", 0)
        .append("large", 65535)
        .append("full", -1);
    RecordConsumer consumer = mock(RecordConsumer.class);
    assertThatCode(() -> new GroupWriter(consumer, schema).write(group)).doesNotThrowAnyException();
    verify(consumer).addInteger(255);
    verify(consumer).addInteger(65535);
    verify(consumer).addInteger(-1);
  }

  @Test
  void rejectsInvalidUnsignedValuesInNestedGroups() {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message m { required group nested { required int32 value (UINT_16); } }");
    Group group = new SimpleGroupFactory(schema).newGroup();
    group.addGroup("nested").append("value", -1);
    GroupWriter writer = new GroupWriter(mock(RecordConsumer.class), schema);
    assertThatThrownBy(() -> writer.write(group))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("value")
        .hasMessageContaining("-1");
  }

  @Test
  void preservesSignedAndUnannotatedIntegers() {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message m { required int32 signed (INT_8); required int32 raw; required int64 full (UINT_64); }");
    Group group = new SimpleGroupFactory(schema)
        .newGroup()
        .append("signed", -128)
        .append("raw", Integer.MIN_VALUE)
        .append("full", -1L);
    RecordConsumer consumer = mock(RecordConsumer.class);
    new GroupWriter(consumer, schema).write(group);
    verify(consumer).addInteger(-128);
    verify(consumer).addInteger(Integer.MIN_VALUE);
    verify(consumer).addLong(-1L);
  }
}
