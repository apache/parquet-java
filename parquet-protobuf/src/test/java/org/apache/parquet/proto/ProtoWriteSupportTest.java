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

import com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.proto.test.TestProto3;
import org.apache.parquet.proto.test.TestProtobuf;

import java.io.IOException;
import java.util.List;

public class ProtoWriteSupportTest {

  private <T extends Message> ProtoWriteSupport<T> createReadConsumerInstance(Class<T> cls, RecordConsumer readConsumerMock) {
    return createReadConsumerInstance(cls, readConsumerMock, new Configuration());
  }

  private <T extends Message> ProtoWriteSupport<T> createReadConsumerInstance(Class<T> cls, RecordConsumer readConsumerMock, Configuration conf) {
    ProtoWriteSupport support = new ProtoWriteSupport(cls);
    support.init(conf);
    support.prepareForWrite(readConsumerMock);
    return support;
  }

  @Test
  public void testSimplestMessage() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.InnerMessage.class, readConsumerMock);

    TestProtobuf.InnerMessage.Builder msg = TestProtobuf.InnerMessage.newBuilder();
    msg.setOne("oneValue");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromString("oneValue"));
    inOrder.verify(readConsumerMock).endField("one", 0);

    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3SimplestMessage() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.InnerMessage.class, readConsumerMock);

    TestProto3.InnerMessage.Builder msg = TestProto3.InnerMessage.newBuilder();
    msg.setOne("oneValue");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromString("oneValue"));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromString(""));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).startField("three", 2);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromString(""));
    inOrder.verify(readConsumerMock).endField("three", 2);

    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testRepeatedIntMessageSpecsCompliant() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.RepeatedIntMessage.class, readConsumerMock, conf);

    TestProtobuf.RepeatedIntMessage.Builder msg = TestProtobuf.RepeatedIntMessage.newBuilder();
    msg.addRepeatedInt(1323);
    msg.addRepeatedInt(54469);

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("repeatedInt", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("list", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).addInteger(1323);
    inOrder.verify(readConsumerMock).endField("element", 0);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).addInteger(54469);
    inOrder.verify(readConsumerMock).endField("element", 0);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("list", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("repeatedInt", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testRepeatedIntMessage() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.RepeatedIntMessage.class, readConsumerMock);

    TestProtobuf.RepeatedIntMessage.Builder msg = TestProtobuf.RepeatedIntMessage.newBuilder();
    msg.addRepeatedInt(1323);
    msg.addRepeatedInt(54469);

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("repeatedInt", 0);
    inOrder.verify(readConsumerMock).addInteger(1323);
    inOrder.verify(readConsumerMock).addInteger(54469);
    inOrder.verify(readConsumerMock).endField("repeatedInt", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testRepeatedIntMessageEmptySpecsCompliant() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.RepeatedIntMessage.class, readConsumerMock, conf);

    TestProtobuf.RepeatedIntMessage.Builder msg = TestProtobuf.RepeatedIntMessage.newBuilder();

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testRepeatedIntMessageEmpty() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.RepeatedIntMessage.class, readConsumerMock);

    TestProtobuf.RepeatedIntMessage.Builder msg = TestProtobuf.RepeatedIntMessage.newBuilder();

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3RepeatedIntMessageSpecsCompliant() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.RepeatedIntMessage.class, readConsumerMock, conf);

    TestProto3.RepeatedIntMessage.Builder msg = TestProto3.RepeatedIntMessage.newBuilder();
    msg.addRepeatedInt(1323);
    msg.addRepeatedInt(54469);

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("repeatedInt", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("list", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).addInteger(1323);
    inOrder.verify(readConsumerMock).endField("element", 0);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).addInteger(54469);
    inOrder.verify(readConsumerMock).endField("element", 0);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("list", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("repeatedInt", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3RepeatedIntMessage() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.RepeatedIntMessage.class, readConsumerMock);

    TestProto3.RepeatedIntMessage.Builder msg = TestProto3.RepeatedIntMessage.newBuilder();
    msg.addRepeatedInt(1323);
    msg.addRepeatedInt(54469);

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("repeatedInt", 0);
    inOrder.verify(readConsumerMock).addInteger(1323);
    inOrder.verify(readConsumerMock).addInteger(54469);
    inOrder.verify(readConsumerMock).endField("repeatedInt", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3RepeatedIntMessageEmptySpecsCompliant() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.RepeatedIntMessage.class, readConsumerMock, conf);

    TestProto3.RepeatedIntMessage.Builder msg = TestProto3.RepeatedIntMessage.newBuilder();

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3RepeatedIntMessageEmpty() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.RepeatedIntMessage.class, readConsumerMock);

    TestProto3.RepeatedIntMessage.Builder msg = TestProto3.RepeatedIntMessage.newBuilder();

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testMapIntMessageSpecsCompliant() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.MapIntMessage.class, readConsumerMock, conf);

    TestProtobuf.MapIntMessage.Builder msg = TestProtobuf.MapIntMessage.newBuilder();
    msg.putMapInt(123, 1);
    msg.putMapInt(234, 2);
    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("mapInt", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key_value", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key", 0);
    inOrder.verify(readConsumerMock).addInteger(123);
    inOrder.verify(readConsumerMock).endField("key", 0);
    inOrder.verify(readConsumerMock).startField("value", 1);
    inOrder.verify(readConsumerMock).addInteger(1);
    inOrder.verify(readConsumerMock).endField("value", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key", 0);
    inOrder.verify(readConsumerMock).addInteger(234);
    inOrder.verify(readConsumerMock).endField("key", 0);
    inOrder.verify(readConsumerMock).startField("value", 1);
    inOrder.verify(readConsumerMock).addInteger(2);
    inOrder.verify(readConsumerMock).endField("value", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("key_value", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("mapInt", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testMapIntMessage() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.MapIntMessage.class, readConsumerMock);

    TestProtobuf.MapIntMessage.Builder msg = TestProtobuf.MapIntMessage.newBuilder();
    msg.putMapInt(123, 1);
    msg.putMapInt(234, 2);
    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("mapInt", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key", 0);
    inOrder.verify(readConsumerMock).addInteger(123);
    inOrder.verify(readConsumerMock).endField("key", 0);
    inOrder.verify(readConsumerMock).startField("value", 1);
    inOrder.verify(readConsumerMock).addInteger(1);
    inOrder.verify(readConsumerMock).endField("value", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key", 0);
    inOrder.verify(readConsumerMock).addInteger(234);
    inOrder.verify(readConsumerMock).endField("key", 0);
    inOrder.verify(readConsumerMock).startField("value", 1);
    inOrder.verify(readConsumerMock).addInteger(2);
    inOrder.verify(readConsumerMock).endField("value", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("mapInt", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testMapIntMessageEmptySpecsCompliant() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.MapIntMessage.class, readConsumerMock, conf);

    TestProtobuf.MapIntMessage.Builder msg = TestProtobuf.MapIntMessage.newBuilder();
    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testMapIntMessageEmpty() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.MapIntMessage.class, readConsumerMock);

    TestProtobuf.MapIntMessage.Builder msg = TestProtobuf.MapIntMessage.newBuilder();
    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3MapIntMessageSpecsCompliant() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.MapIntMessage.class, readConsumerMock, conf);

    TestProto3.MapIntMessage.Builder msg = TestProto3.MapIntMessage.newBuilder();
    msg.putMapInt(123, 1);
    msg.putMapInt(234, 2);
    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("mapInt", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key_value", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key", 0);
    inOrder.verify(readConsumerMock).addInteger(123);
    inOrder.verify(readConsumerMock).endField("key", 0);
    inOrder.verify(readConsumerMock).startField("value", 1);
    inOrder.verify(readConsumerMock).addInteger(1);
    inOrder.verify(readConsumerMock).endField("value", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key", 0);
    inOrder.verify(readConsumerMock).addInteger(234);
    inOrder.verify(readConsumerMock).endField("key", 0);
    inOrder.verify(readConsumerMock).startField("value", 1);
    inOrder.verify(readConsumerMock).addInteger(2);
    inOrder.verify(readConsumerMock).endField("value", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("key_value", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("mapInt", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3MapIntMessage() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.MapIntMessage.class, readConsumerMock);

    TestProto3.MapIntMessage.Builder msg = TestProto3.MapIntMessage.newBuilder();
    msg.putMapInt(123, 1);
    msg.putMapInt(234, 2);
    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("mapInt", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key", 0);
    inOrder.verify(readConsumerMock).addInteger(123);
    inOrder.verify(readConsumerMock).endField("key", 0);
    inOrder.verify(readConsumerMock).startField("value", 1);
    inOrder.verify(readConsumerMock).addInteger(1);
    inOrder.verify(readConsumerMock).endField("value", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key", 0);
    inOrder.verify(readConsumerMock).addInteger(234);
    inOrder.verify(readConsumerMock).endField("key", 0);
    inOrder.verify(readConsumerMock).startField("value", 1);
    inOrder.verify(readConsumerMock).addInteger(2);
    inOrder.verify(readConsumerMock).endField("value", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("mapInt", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3MapIntMessageEmptySpecsCompliant() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.MapIntMessage.class, readConsumerMock, conf);

    TestProto3.MapIntMessage.Builder msg = TestProto3.MapIntMessage.newBuilder();
    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3MapIntMessageEmpty() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.MapIntMessage.class, readConsumerMock);

    TestProto3.MapIntMessage.Builder msg = TestProto3.MapIntMessage.newBuilder();
    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testRepeatedInnerMessageMessage_message() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.TopMessage.class, readConsumerMock);

    TestProtobuf.TopMessage.Builder msg = TestProtobuf.TopMessage.newBuilder();
    msg.addInnerBuilder().setOne("one").setTwo("two");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("two".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testRepeatedInnerMessageSpecsCompliantMessage_message() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.TopMessage.class, readConsumerMock, conf);

    TestProtobuf.TopMessage.Builder msg = TestProtobuf.TopMessage.newBuilder();
    msg.addInnerBuilder().setOne("one").setTwo("two");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("list", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("two".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("element", 0);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("list", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3RepeatedInnerMessageMessage_message() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);;
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.TopMessage.class, readConsumerMock);

    TestProto3.TopMessage.Builder msg = TestProto3.TopMessage.newBuilder();
    msg.addInnerBuilder().setOne("one").setTwo("two");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("two".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).startField("three", 2);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("three", 2);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3RepeatedInnerMessageSpecsCompliantMessage_message() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.TopMessage.class, readConsumerMock, conf);

    TestProto3.TopMessage.Builder msg = TestProto3.TopMessage.newBuilder();
    msg.addInnerBuilder().setOne("one").setTwo("two");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("list", 0);
    inOrder.verify(readConsumerMock).startGroup();

    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("two".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).startField("three", 2);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("three", 2);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("element", 0);

    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("list", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }


  @Test
  public void testRepeatedInnerMessageSpecsCompliantMessage_scalar() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.TopMessage.class, readConsumerMock, conf);

    TestProtobuf.TopMessage.Builder msg = TestProtobuf.TopMessage.newBuilder();
    msg.addInnerBuilder().setOne("one");
    msg.addInnerBuilder().setTwo("two");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("list", 0);

    //first inner message
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("element", 0);
    inOrder.verify(readConsumerMock).endGroup();

    //second inner message
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("two".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("element", 0);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("list", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testRepeatedInnerMessageMessage_scalar() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.TopMessage.class, readConsumerMock);

    TestProtobuf.TopMessage.Builder msg = TestProtobuf.TopMessage.newBuilder();
    msg.addInnerBuilder().setOne("one");
    msg.addInnerBuilder().setTwo("two");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);

    //first inner message
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).endGroup();

    //second inner message
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("two".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3RepeatedInnerMessageMessage_scalar() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.TopMessage.class, readConsumerMock);

    TestProto3.TopMessage.Builder msg = TestProto3.TopMessage.newBuilder();
    msg.addInnerBuilder().setOne("one");
    msg.addInnerBuilder().setTwo("two");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);

    //first inner message
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).startField("three", 2);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("three", 2);
    inOrder.verify(readConsumerMock).endGroup();

    //second inner message
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("two".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).startField("three", 2);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("three", 2);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3RepeatedInnerMessageSpecsCompliantMessage_scalar() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.TopMessage.class, readConsumerMock, conf);

    TestProto3.TopMessage.Builder msg = TestProto3.TopMessage.newBuilder();
    msg.addInnerBuilder().setOne("one");
    msg.addInnerBuilder().setTwo("two");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("list", 0);

    //first inner message
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).startField("three", 2);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("three", 2);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("element", 0);
    inOrder.verify(readConsumerMock).endGroup();

    //second inner message
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("two".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).startField("three", 2);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("three", 2);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("element", 0);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("list", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testOptionalInnerMessage() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.MessageA.class, readConsumerMock);

    TestProtobuf.MessageA.Builder msg = TestProtobuf.MessageA.newBuilder();
    msg.getInnerBuilder().setOne("one");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3OptionalInnerMessage() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.MessageA.class, readConsumerMock);

    TestProto3.MessageA.Builder msg = TestProto3.MessageA.newBuilder();
    msg.getInnerBuilder().setOne("one");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).startField("three", 2);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("three", 2);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testMessageWithExtensions() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.Vehicle.class, readConsumerMock);

    TestProtobuf.Vehicle.Builder msg = TestProtobuf.Vehicle.newBuilder();
    msg.setHorsePower(300);
    // Currently there's no support for extension fields. This test tests that the extension field
    // will cause an exception.
    msg.setExtension(TestProtobuf.Airplane.wingSpan, 50);

    instance.write(msg.build());
  }

  @Test
  public void testMessageOneOf() {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport<TestProto3.OneOfTestMessage> spyWriter = createReadConsumerInstance(TestProto3.OneOfTestMessage.class, readConsumerMock);
    final int theInt = 99;

    TestProto3.OneOfTestMessage.Builder msg = TestProto3.OneOfTestMessage.newBuilder();
    msg.setSecond(theInt);
    spyWriter.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("second", 1);
    inOrder.verify(readConsumerMock).addInteger(theInt);
    inOrder.verify(readConsumerMock).endField("second", 1);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  /**
   * Ensure that a message with a oneOf gets written out correctly and can be
   * read back as expected.
   */
  @Test
  public void testMessageOneOfRoundTrip() throws IOException {

    TestProto3.OneOfTestMessage.Builder msgBuilder = TestProto3.OneOfTestMessage.newBuilder();
    msgBuilder.setSecond(99);
    TestProto3.OneOfTestMessage theMessage = msgBuilder.build();

    TestProto3.OneOfTestMessage.Builder msgBuilder2 = TestProto3.OneOfTestMessage.newBuilder();
    TestProto3.OneOfTestMessage theMessageNothingSet = msgBuilder2.build();

    //Write them out
    Path tmpFilePath = TestUtils.writeMessages(theMessage, theMessageNothingSet);

    //Read it back!
    List<TestProto3.OneOfTestMessage> gotBack = TestUtils.readMessages(tmpFilePath, TestProto3.OneOfTestMessage.class);

    //First message
    TestProto3.OneOfTestMessage gotBackFirst = gotBack.get(0);
    assert gotBackFirst.getSecond() == 99;
    assert gotBackFirst.getTheOneofCase() == TestProto3.OneOfTestMessage.TheOneofCase.SECOND;

    //Second message with nothing set
    TestProto3.OneOfTestMessage gotBackSecond = gotBack.get(1);
    assert gotBackSecond.getTheOneofCase() == TestProto3.OneOfTestMessage.TheOneofCase.THEONEOF_NOT_SET;
  }
}
