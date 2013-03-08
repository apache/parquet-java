/**
 * Copyright 2012 Twitter, Inc.
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
package parquet.hadoop.thrift;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Test;

import parquet.Log;
import parquet.example.data.Group;
import parquet.hadoop.example.ExampleInputFormat;

import com.twitter.data.proto.tutorial.thrift.AddressBook;
import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;

public class TestThriftToParquetFileWriter {
  private static final Log LOG = Log
      .getLog(TestThriftToParquetFileWriter.class);

  @Test
  public void testWriteFile() throws IOException, InterruptedException, TException {
    final Path fileToCreate = new Path("target/test/TestThriftToParquetFileWriter/file.parquet");
    Configuration conf = new Configuration();
    final FileSystem fs = fileToCreate.getFileSystem(conf);
    if (fs.exists(fileToCreate)) {
      fs.delete(fileToCreate, true);
    }
    TProtocolFactory protocolFactory = new TCompactProtocol.Factory();
    TaskAttemptID taskId = new TaskAttemptID("local", 0, true, 0, 0);
    ThriftToParquetFileWriter w = new ThriftToParquetFileWriter(fileToCreate, new TaskAttemptContext(conf, taskId), protocolFactory, AddressBook.class);

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final TProtocol protocol = protocolFactory.getProtocol(new TIOStreamTransport(baos));

    final AddressBook a = new AddressBook(
        Arrays.asList(
            new Person(
                new Name("Bob", "Roberts"),
                0,
                "bob.roberts@example.com",
                Arrays.asList(new PhoneNumber("1234567890")))));

    a.write(protocol);

    w.write(new BytesWritable(baos.toByteArray()));

    w.close();

    ExampleInputFormat exampleInputFormat = new ExampleInputFormat();
    Job job = new Job();
    ExampleInputFormat.addInputPath(job, fileToCreate);
    final JobID jobID = new JobID("local", 1);
    List<InputSplit> splits = exampleInputFormat.getSplits(new JobContext(job.getConfiguration(), jobID));
    int i = 0;
    for (InputSplit split : splits) {
      LOG.info(split);
      TaskAttemptContext taskAttemptContext = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID(new TaskID(jobID, true, i), 0));
      final RecordReader<Void, Group> reader = exampleInputFormat.createRecordReader(split, taskAttemptContext);
      reader.initialize(split, taskAttemptContext);
      while (reader.nextKeyValue()) {
        final Group v = reader.getCurrentValue();
        assertEquals(a.persons.size(), v.getFieldRepetitionCount("persons"));
        assertEquals(a.persons.get(0).email, v.getGroup("persons", 0).getGroup(0, 0).getString("email", 0));
        // just some sanity check, we're testing the various layers somewhere else
        ++i;
      }
    }
    assertEquals("read 1 record", 1, i);

  }

}
