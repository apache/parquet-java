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
import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.Deque;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

public class ValidatingRecordConsumerTest {

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
      assertEquals(
          "previousField deque should be empty after endMessage (row " + row + ")", 0, previousField.size());
    }
  }
}
