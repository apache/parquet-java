/**
 * Copyright 2013 Lukas Nalezenec
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.proto;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.After;
import org.junit.Test;
import parquet.Log;
import parquet.protobuf.test.TestProtobuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;


public class ProtoInputOutputFormatTest {


  private static final Log LOG = Log.getLog(ProtoInputOutputFormatTest.class);

  private static List<Message> inputMessages;
  private static List<Message> outputMessages;


  /**
   * Writes protobuffer using first MR job, reads written file using
   * second job and compares input and output.
   * */
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

    List<Message> result = runMRJobs(TestProtobuf.IOFormatMessage.class, input);

    assertEquals(1, result.size());
    TestProtobuf.IOFormatMessage output = (TestProtobuf.IOFormatMessage) result.get(0);

    assertEquals(666, output.getOptionalDouble(), 0.00001);
    assertEquals(323, output.getMsg().getSomeId());
    assertEquals("Msg1", output.getRepeatedString(0));
    assertEquals("Msg2", output.getRepeatedString(1));

    assertEquals(input, output);

  }

  public static class WritingMapper extends Mapper<LongWritable, Text, Void, Message> {

    public void run(Context context) throws IOException,InterruptedException {
      if (inputMessages == null || inputMessages.size() == 0) {
        throw new RuntimeException("No mock data given");
      } else {
        for (Message msg : inputMessages) {
          context.write(null, msg);
          LOG.debug("Reading msg from mock writing mapper" + msg);
        }
      }
    }
  }

  public static class ReadingMapper extends Mapper<Void, MessageOrBuilder, LongWritable, Message> {
    protected void map(Void key, MessageOrBuilder value, Context context) throws IOException ,InterruptedException {
      Message clone = ((Message.Builder) value).build();
      outputMessages.add(clone);
    }
  }

  /**
   * Runs job that writes input to file and then job reading data back.
   * */
  public static synchronized List<Message> runMRJobs(Class<? extends Message> pbClass, Message... messages) throws Exception {
    final Configuration conf = new Configuration();
    final Path inputPath = new Path("src/test/java/parquet/proto/ProtoInputOutputFormatTest.java");
    final Path parquetPath = TestUtils.someTemporaryFilePath();

    inputMessages = new ArrayList<Message>();

    for (Message m: messages) {
      inputMessages.add(m);
    }

    inputMessages = Collections.unmodifiableList(inputMessages);

    {
      final Job job = new Job(conf, "write");

      // input not really used
      TextInputFormat.addInputPath(job, inputPath);
      job.setInputFormatClass(TextInputFormat.class);

      job.setMapperClass(WritingMapper.class);
      job.setNumReduceTasks(0);

      job.setOutputFormatClass(ProtoParquetOutputFormat.class);
      ProtoParquetOutputFormat.setOutputPath(job, parquetPath);
      ProtoParquetOutputFormat.setProtobufferClass(job, pbClass);

      waitForJob(job);
    }
    inputMessages = null;
    outputMessages = new ArrayList<Message>();
    {
      final Job job = new Job(conf, "read");
      job.setInputFormatClass(ProtoParquetInputFormat.class);
      ProtoParquetInputFormat.setInputPaths(job, parquetPath);

      job.setMapperClass(ReadingMapper.class);
      job.setNumReduceTasks(0);

      job.setOutputFormatClass(NullOutputFormat.class);

      waitForJob(job);
    }

    List<Message> result = Collections.unmodifiableList(outputMessages);
    outputMessages = null;
    return result;
  }


  @After
  public void tearDown() throws Exception {
    inputMessages = null;
    outputMessages = null;
  }


  private static void waitForJob(Job job) throws Exception {
    job.submit();
    while (!job.isComplete()) {
      LOG.debug("waiting for job " + job.getJobName());
      sleep(100);
    }
    LOG.info("status for job " + job.getJobName() + ": " + (job.isSuccessful() ? "SUCCESS" : "FAILURE"));
    if (!job.isSuccessful()) {
      throw new RuntimeException("job failed " + job.getJobName());
    }
  }
}
