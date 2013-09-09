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
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Test;

import parquet.Log;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.util.ContextUtil;

import com.twitter.data.proto.tutorial.thrift.AddressBook;
import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;

public class TestParquetToThriftReadProjection {

  private static final Log LOG = Log.getLog(TestParquetToThriftReadProjection.class);

  @Test
  public void testThriftOptionalFieldsWithReadProjectionUsingParquetSchema() throws Exception {
    // test with projection
    Configuration conf = new Configuration();
    final String readProjectionSchema = "message AddressBook {\n" +
            "  optional group persons {\n" +
            "    repeated group persons_tuple {\n" +
            "      required group name {\n" +
            "        optional binary first_name;\n" +
            "        optional binary last_name;\n" +
            "      }\n" +
            "      optional int32 id;\n" +
            "    }\n" +
            "  }\n" +
            "}";
    conf.set(ReadSupport.PARQUET_READ_SCHEMA, readProjectionSchema);
    shouldDoProjection(conf);
  }

  @Test
  public void testThriftOptionalFieldsWithReadProjectionUsingFilter() throws Exception {
    Configuration conf = new Configuration();
    final String projectionFilterDesc = "persons/{id}";
    conf.set(ThriftReadSupport.THRIFT_COLUMN_FILTER_KEY, projectionFilterDesc);
    shouldDoProjection(conf);
  }

  private void shouldDoProjection(Configuration conf) throws Exception {
    final Path parquetFile = new Path("target/test/TestParquetToThriftReadProjection/file.parquet");
    final FileSystem fs = parquetFile.getFileSystem(conf);
    if (fs.exists(parquetFile)) {
      fs.delete(parquetFile, true);
    }

    //create a test file
    final TProtocolFactory protocolFactory = new TCompactProtocol.Factory();
    final TaskAttemptID taskId = new TaskAttemptID("local", 0, true, 0, 0);
    final ThriftToParquetFileWriter w = new ThriftToParquetFileWriter(parquetFile, new TaskAttemptContext(conf, taskId), protocolFactory, AddressBook.class);
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


    final ParquetThriftInputFormat<AddressBook> parquetThriftInputFormat = new ParquetThriftInputFormat<AddressBook>();
    final Job job = new Job(conf, "read");
    job.setInputFormatClass(ParquetThriftInputFormat.class);
    ParquetThriftInputFormat.setInputPaths(job, parquetFile);
    final JobID jobID = new JobID("local", 1);
    List<InputSplit> splits = parquetThriftInputFormat.getSplits(new JobContext(ContextUtil.getConfiguration(job), jobID));
    AddressBook expected = a.deepCopy();
    for (Person person: expected.getPersons()) {
    	person.unsetEmail();
    	person.unsetPhones();
    }
    AddressBook readValue = null;
    for (InputSplit split : splits) {
      TaskAttemptContext taskAttemptContext = new TaskAttemptContext(ContextUtil.getConfiguration(job), new TaskAttemptID(new TaskID(jobID, true, 1), 0));
      final RecordReader<Void, AddressBook> reader = parquetThriftInputFormat.createRecordReader(split, taskAttemptContext);
      reader.initialize(split, taskAttemptContext);
      if (reader.nextKeyValue()) {
        readValue = reader.getCurrentValue();
        LOG.info(readValue);
      }
    }
    assertEquals(expected, readValue);

  }

}
