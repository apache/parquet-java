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
package parquet.proto;

import com.google.protobuf.Message;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import parquet.Log;
import parquet.proto.test.TestProtobuf;
import parquet.proto.test.TestProtobuf.FirstCustomClassMessage;
import parquet.proto.test.TestProtobuf.SecondCustomClassMessage;
import parquet.proto.utils.ReadUsingMR;
import parquet.proto.utils.WriteUsingMR;

import java.util.List;

import static org.junit.Assert.*;


public class ProtoInputOutputFormatTest {

  private static final Log LOG = Log.getLog(ProtoInputOutputFormatTest.class);

  /**
   * Writes Protocol Buffer using first MR job, reads written file using
   * second job and compares input and output.
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


  /**
   * Writes data to file then reads them again with projection.
   * Only requested data should be read.
   * */
  @Test
  public void testProjection() throws Exception {

    TestProtobuf.Document.Builder writtenDocument = TestProtobuf.Document.newBuilder();
    writtenDocument.setDocId(12345);
    writtenDocument.addNameBuilder().setUrl("http://goout.cz/");

    Path outputPath = new WriteUsingMR().write(writtenDocument.build());

    //lets prepare reading with schema
    ReadUsingMR reader = new ReadUsingMR();

    String projection = "message Document {required int64 DocId; }";
    reader.setRequestedProjection(projection);
    List<Message> output = reader.read(outputPath);
    TestProtobuf.Document readDocument = (TestProtobuf.Document) output.get(0);


    //test that only requested fields were deserialized
    assertTrue(readDocument.hasDocId());
    assertTrue("Found data outside projection.", readDocument.getNameCount() == 0);
  }

  /**
   * When user specified protobuffer class in configuration,
   * It should replace class specified in header.
   * */
  @Test
  public void testCustomProtoClass() throws Exception {
    FirstCustomClassMessage.Builder inputMessage;
    inputMessage = FirstCustomClassMessage.newBuilder();
    inputMessage.setString("writtenString");

    Path outputPath = new WriteUsingMR().write(new Message[]{inputMessage.build()});
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = SecondCustomClassMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(1, result.size());
    Message msg = result.get(0);
    assertFalse("Class from header returned.",
            msg instanceof FirstCustomClassMessage);
    assertTrue("Custom class was not used",
            msg instanceof SecondCustomClassMessage);

    String stringValue;
    stringValue = ((SecondCustomClassMessage) msg).getString();
    assertEquals("writtenString", stringValue);
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
