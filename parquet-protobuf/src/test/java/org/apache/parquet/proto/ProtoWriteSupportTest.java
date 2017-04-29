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
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.proto.test.TestProto3;
import org.apache.parquet.proto.test.TestProtobuf;

public class ProtoWriteSupportTest {

  private <T extends Message> ProtoWriteSupport<T> createReadConsumerInstance(Class<T> cls, RecordConsumer readConsumerMock) {
    ProtoWriteSupport support = new ProtoWriteSupport(cls);
    support.init(new Configuration());
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
    inOrder.verify(readConsumerMock).startField("list", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("two".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("list", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3RepeatedInnerMessageMessage_message() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.TopMessage.class, readConsumerMock);

    TestProto3.TopMessage.Builder msg = TestProto3.TopMessage.newBuilder();
    msg.addInnerBuilder().setOne("one").setTwo("two");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("list", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("two".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
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
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("list", 0);

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

    inOrder.verify(readConsumerMock).endField("list", 0);
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
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("list", 0);

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
}
