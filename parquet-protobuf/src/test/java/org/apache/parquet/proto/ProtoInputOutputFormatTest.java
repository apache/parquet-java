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
import org.apache.parquet.proto.test.TestProto3;
import org.apache.parquet.proto.test.TestProtobuf;
import org.apache.parquet.proto.test.TestProtobuf.FirstCustomClassMessage;
import org.apache.parquet.proto.test.TestProtobuf.SecondCustomClassMessage;
import org.apache.parquet.proto.utils.ReadUsingMR;
import org.apache.parquet.proto.utils.WriteUsingMR;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class ProtoInputOutputFormatTest {

  /**
   * Writes Protocol Buffer using first MR job, reads written file using second
   * job and compares input and output.
   */
  @Test
  public void testInputOutput() throws Exception {
    TestProtobuf.IOFormatMessage input;
    {
      TestProtobuf.IOFormatMessage.Builder msg = TestProtobuf.IOFormatMessage.newBuilder();
      msg.setOptionalDouble(666);
      msg.addRepeatedString("Msg1");
      msg.addRepeatedString("Msg2");
      msg.getMsgBuilder().setSomeId(323);
      input = msg.build();
    }

    List<Message> result = runMRJobs(input);

    assertEquals(1, result.size());
    TestProtobuf.IOFormatMessage output = (TestProtobuf.IOFormatMessage) result.get(0);

    assertEquals(666, output.getOptionalDouble(), 0.00001);
    assertEquals(323, output.getMsg().getSomeId());
    assertEquals("Msg1", output.getRepeatedString(0));
    assertEquals("Msg2", output.getRepeatedString(1));

    assertEquals(input, output);
  }

  @Test
  public void testProto3InputOutput() throws Exception {
    TestProto3.IOFormatMessage input;
    {
      TestProto3.IOFormatMessage.Builder msg = TestProto3.IOFormatMessage.newBuilder();
      msg.setOptionalDouble(666);
      msg.addRepeatedString("Msg1");
      msg.addRepeatedString("Msg2");
      msg.getMsgBuilder().setSomeId(323);
      input = msg.build();
    }

    List<Message> result = runMRJobs(input);

    assertEquals(1, result.size());
    TestProto3.IOFormatMessage output = (TestProto3.IOFormatMessage) result.get(0);

    assertEquals(666, output.getOptionalDouble(), 0.00001);
    assertEquals(323, output.getMsg().getSomeId());
    assertEquals("Msg1", output.getRepeatedString(0));
    assertEquals("Msg2", output.getRepeatedString(1));

    assertEquals(input, output);
  }

  /**
   * Writes data to file then reads them again with projection. Only requested
   * data should be read.
   */
  @Test
  public void testProjection() throws Exception {

    TestProtobuf.Document.Builder writtenDocument = TestProtobuf.Document.newBuilder();
    writtenDocument.setDocId(12345);
    writtenDocument.addNameBuilder().setUrl("http://goout.cz/");

    Path outputPath = new WriteUsingMR().write(writtenDocument.build());

    // lets prepare reading with schema
    ReadUsingMR reader = new ReadUsingMR();

    String projection = "message Document {required int64 DocId; }";
    reader.setRequestedProjection(projection);
    List<Message> output = reader.read(outputPath);
    TestProtobuf.Document readDocument = (TestProtobuf.Document) output.get(0);

    // test that only requested fields were deserialized
    assertTrue(readDocument.hasDocId());
    assertTrue("Found data outside projection.", readDocument.getNameCount() == 0);
  }

  @Test
  public void testProto3Projection() throws Exception {

    TestProto3.Document.Builder writtenDocument = TestProto3.Document.newBuilder();
    writtenDocument.setDocId(12345);
    writtenDocument.addNameBuilder().setUrl("http://goout.cz/");

    Path outputPath = new WriteUsingMR().write(writtenDocument.build());

    // lets prepare reading with schema
    ReadUsingMR reader = new ReadUsingMR();

    String projection = "message Document {optional int64 DocId; }";
    reader.setRequestedProjection(projection);
    List<Message> output = reader.read(outputPath);
    TestProto3.Document readDocument = (TestProto3.Document) output.get(0);

    // test that only requested fields were deserialized
    assertTrue(readDocument.getDocId() == 12345);
    assertTrue(readDocument.getNameCount() == 0);
    assertTrue("Found data outside projection.", readDocument.getNameCount() == 0);
  }

  /**
   * When user specified protobuffer class in configuration, It should replace
   * class specified in header.
   */
  @Test
  public void testCustomProtoClass() throws Exception {
    FirstCustomClassMessage.Builder inputMessage;
    inputMessage = FirstCustomClassMessage.newBuilder();
    inputMessage.setString("writtenString");

    Path outputPath = new WriteUsingMR().write(new Message[] { inputMessage.build() });
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = SecondCustomClassMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(1, result.size());
    Message msg = result.get(0);
    assertFalse("Class from header returned.", msg instanceof FirstCustomClassMessage);
    assertTrue("Custom class was not used", msg instanceof SecondCustomClassMessage);

    String stringValue;
    stringValue = ((SecondCustomClassMessage) msg).getString();
    assertEquals("writtenString", stringValue);
  }

  @Test
  public void testProto3CustomProtoClass() throws Exception {
    TestProto3.FirstCustomClassMessage.Builder inputMessage;
    inputMessage = TestProto3.FirstCustomClassMessage.newBuilder();
    inputMessage.setString("writtenString");

    Path outputPath = new WriteUsingMR().write(new Message[] { inputMessage.build() });
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = TestProto3.SecondCustomClassMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(1, result.size());
    Message msg = result.get(0);
    assertFalse("Class from header returned.", msg instanceof TestProto3.FirstCustomClassMessage);
    assertTrue("Custom class was not used", msg instanceof TestProto3.SecondCustomClassMessage);

    String stringValue;
    stringValue = ((TestProto3.SecondCustomClassMessage) msg).getString();
    assertEquals("writtenString", stringValue);
  }

  @Test
  public void testRepeatedIntMessageClass() throws Exception {
    TestProtobuf.RepeatedIntMessage msgEmpty = TestProtobuf.RepeatedIntMessage.newBuilder().build();
    TestProtobuf.RepeatedIntMessage msgNonEmpty = TestProtobuf.RepeatedIntMessage.newBuilder().addRepeatedInt(1)
        .addRepeatedInt(2).build();

    Path outputPath = new WriteUsingMR().write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = TestProtobuf.RepeatedIntMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  @Test
  public void testRepeatedIntMessageClassSchemaCompliant() throws Exception {
    TestProtobuf.RepeatedIntMessage msgEmpty = TestProtobuf.RepeatedIntMessage.newBuilder().build();
    TestProtobuf.RepeatedIntMessage msgNonEmpty = TestProtobuf.RepeatedIntMessage.newBuilder().addRepeatedInt(1)
        .addRepeatedInt(2).build();

    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);

    Path outputPath = new WriteUsingMR(conf).write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = TestProtobuf.RepeatedIntMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  @Test
  public void testMapIntMessageClass() throws Exception {
    TestProtobuf.MapIntMessage msgEmpty = TestProtobuf.MapIntMessage.newBuilder().build();
    TestProtobuf.MapIntMessage msgNonEmpty = TestProtobuf.MapIntMessage.newBuilder().putMapInt(1, 123).putMapInt(2, 234)
        .build();

    Path outputPath = new WriteUsingMR().write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = TestProtobuf.MapIntMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  @Test
  public void testMapIntMessageClassSchemaCompliant() throws Exception {
    TestProtobuf.MapIntMessage msgEmpty = TestProtobuf.MapIntMessage.newBuilder().build();
    TestProtobuf.MapIntMessage msgNonEmpty = TestProtobuf.MapIntMessage.newBuilder().putMapInt(1, 123).putMapInt(2, 234)
        .build();

    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);

    Path outputPath = new WriteUsingMR(conf).write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR(conf);
    String customClass = TestProtobuf.MapIntMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  @Test
  public void testRepeatedInnerMessageClass() throws Exception {
    TestProtobuf.RepeatedInnerMessage msgEmpty = TestProtobuf.RepeatedInnerMessage.newBuilder().build();
    TestProtobuf.RepeatedInnerMessage msgNonEmpty = TestProtobuf.RepeatedInnerMessage.newBuilder()
        .addRepeatedInnerMessage(TestProtobuf.InnerMessage.newBuilder().setOne("one").build())
        .addRepeatedInnerMessage(TestProtobuf.InnerMessage.newBuilder().setTwo("two").build()).build();

    Path outputPath = new WriteUsingMR().write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = TestProtobuf.RepeatedInnerMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  @Test
  public void testRepeatedInnerMessageClassSchemaCompliant() throws Exception {
    TestProtobuf.RepeatedInnerMessage msgEmpty = TestProtobuf.RepeatedInnerMessage.newBuilder().build();
    TestProtobuf.RepeatedInnerMessage msgNonEmpty = TestProtobuf.RepeatedInnerMessage.newBuilder()
        .addRepeatedInnerMessage(TestProtobuf.InnerMessage.newBuilder().setOne("one").build())
        .addRepeatedInnerMessage(TestProtobuf.InnerMessage.newBuilder().setTwo("two").build()).build();

    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);

    Path outputPath = new WriteUsingMR(conf).write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR(conf);
    String customClass = TestProtobuf.RepeatedInnerMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  /**
   * Runs job that writes input to file and then job reading data back.
   */
  public static List<Message> runMRJobs(Message... messages) throws Exception {
    Path outputPath = new WriteUsingMR().write(messages);
    List<Message> result = new ReadUsingMR().read(outputPath);
    return result;
  }
}
