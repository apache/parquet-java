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
package org.apache.parquet.hadoop.thrift;

import com.twitter.data.proto.tutorial.thrift.AddressBook;
import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.parquet.thrift.test.compat.MapWithStructValue;
import org.apache.parquet.thrift.test.compat.StructV3;
import org.apache.parquet.thrift.test.compat.StructV4WithExtracStructField;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Test;
import org.apache.parquet.Log;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.thrift.test.*;

import java.io.ByteArrayOutputStream;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class TestParquetToThriftReadWriteAndProjection {

  private static final Log LOG = Log.getLog(TestParquetToThriftReadWriteAndProjection.class);

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
    TBase toWrite = new AddressBook(
            Arrays.asList(
                    new Person(
                            new Name("Bob", "Roberts"),
                            0,
                            "bob.roberts@example.com",
                            Arrays.asList(new PhoneNumber("1234567890")))));

    TBase toRead = new AddressBook(
            Arrays.asList(
                    new Person(
                            new Name("Bob", "Roberts"),
                            0,
                            null,
                            null)));
    shouldDoProjection(conf, toWrite, toRead, AddressBook.class);
  }

  @Test
  public void testPullingInRequiredStructWithFilter() throws Exception {
    final String projectionFilterDesc = "persons/{id};persons/email";
    TBase toWrite = new AddressBook(
            Arrays.asList(
                    new Person(
                            new Name("Bob", "Roberts"),
                            0,
                            "bob.roberts@example.com",
                            Arrays.asList(new PhoneNumber("1234567890")))));

    //Name is a required field, but is projected out. To make the thrift record pass validation, the name field is filled
    //with empty string
    TBase toRead = new AddressBook(
            Arrays.asList(
                    new Person(
                            new Name("", ""),
                            0,
                            "bob.roberts@example.com",
                            null)));
    shouldDoProjectionWithThriftColumnFilter(projectionFilterDesc, toWrite, toRead, AddressBook.class);
  }

  @Test
  public void testReorderdOptionalFields() throws Exception {
    final String projectionFilter = "**";
    StructWithReorderedOptionalFields toWrite = new StructWithReorderedOptionalFields();
    toWrite.setFieldOne(1);
    toWrite.setFieldTwo(2);
    toWrite.setFieldThree(3);
    shouldDoProjectionWithThriftColumnFilter(projectionFilter, toWrite, toWrite, StructWithReorderedOptionalFields.class);
  }

  @Test
  public void testProjectOutOptionalFields() throws Exception {

    final String projectionFilterDesc = "persons/name/*";

    TBase toWrite = new AddressBook(
            Arrays.asList(
                    new Person(
                            new Name("Bob", "Roberts"),
                            0,
                            "bob.roberts@example.com",
                            Arrays.asList(new PhoneNumber("1234567890")))));

    //emails and phones are optional fields that do not match the projection filter
    TBase toRead = new AddressBook(
            Arrays.asList(
                    new Person(
                            new Name("Bob", "Roberts"),
                            0,
                            null,
                            null))
    );

    shouldDoProjectionWithThriftColumnFilter(projectionFilterDesc, toWrite, toRead, AddressBook.class);
  }

  @Test
  public void testPullInRequiredMaps() throws Exception {
    String filter = "name";

    Map<String, String> mapValue = new HashMap<String, String>();
    mapValue.put("a", "1");
    mapValue.put("b", "2");
    RequiredMapFixture toWrite = new RequiredMapFixture(mapValue);
    toWrite.setName("testName");

    RequiredMapFixture toRead = new RequiredMapFixture(new HashMap<String, String>());
    toRead.setName("testName");

    shouldDoProjectionWithThriftColumnFilter(filter, toWrite, toRead, RequiredMapFixture.class);
  }

  @Test
  public void testDropMapValuePrimitive() throws Exception {
    String filter = "mavalue/key";

    Map<String, String> mapValue = new HashMap<String, String>();
    mapValue.put("a", "1");
    mapValue.put("b", "2");
    RequiredMapFixture toWrite = new RequiredMapFixture(mapValue);
    toWrite.setName("testName");

    // for now we expect no value projection to happen
    // because a sentinel value is selected from the value
    Map<String, String> readValue = new HashMap<String, String>();
    readValue.put("a", "1");
    readValue.put("b", "2");

    RequiredMapFixture toRead = new RequiredMapFixture(readValue);

    shouldDoProjectionWithThriftColumnFilter(filter, toWrite, toRead, RequiredMapFixture.class);
  }

  @Test
  public void testDropMapValueStruct() throws Exception {
    String filter = "reqMap/key";

    Map<String, StructV4WithExtracStructField> mapValue = new HashMap<String, StructV4WithExtracStructField>();

    StructV4WithExtracStructField v1 = new StructV4WithExtracStructField();
    StructV3 sv31 = new StructV3();
    sv31.setAge("bar");
    sv31.setGender("f");
    sv31.setName("a name");
    v1.setAge("foo");
    v1.setAddedStruct(sv31);
    v1.setGender("m");
    v1.setName("outer name");

    StructV4WithExtracStructField v2 = new StructV4WithExtracStructField();
    StructV3 sv32 = new StructV3();
    sv32.setAge("bar2");
    sv32.setGender("f2");
    sv32.setName("a name2");
    v2.setAge("foo2");
    v2.setAddedStruct(sv32);
    v2.setGender("m2");
    v2.setName("outer name2");

    mapValue.put("key 1", v1);
    mapValue.put("key 2", v2);
    MapWithStructValue toWrite = new MapWithStructValue(mapValue);

    // for now we expect a sentinel column to be kept
    HashMap<String, StructV4WithExtracStructField> readValue = new HashMap<String, StructV4WithExtracStructField>();
    readValue.put("key 1", new StructV4WithExtracStructField("outer name"));
    readValue.put("key 2", new StructV4WithExtracStructField("outer name2"));

    MapWithStructValue toRead = new MapWithStructValue(readValue);

    shouldDoProjectionWithThriftColumnFilter(filter, toWrite, toRead, MapWithStructValue.class);
  }

  @Test
  public void testPullInRequiredLists() throws Exception {
    String filter = "info";

    RequiredListFixture toWrite = new RequiredListFixture(Arrays.asList(new org.apache.parquet.thrift.test.Name("first_name")));
    toWrite.setInfo("test_info");

    RequiredListFixture toRead = new RequiredListFixture(new ArrayList<org.apache.parquet.thrift.test.Name>());
    toRead.setInfo("test_info");

    shouldDoProjectionWithThriftColumnFilter(filter, toWrite, toRead, RequiredListFixture.class);
  }

  @Test
  public void testPullInRequiredSets() throws Exception {
    String filter = "info";

    RequiredSetFixture toWrite = new RequiredSetFixture(new HashSet<org.apache.parquet.thrift.test.Name>(Arrays.asList(new org.apache.parquet.thrift.test.Name("first_name"))));
    toWrite.setInfo("test_info");

    RequiredSetFixture toRead = new RequiredSetFixture(new HashSet<org.apache.parquet.thrift.test.Name>());
    toRead.setInfo("test_info");

    shouldDoProjectionWithThriftColumnFilter(filter, toWrite, toRead, RequiredSetFixture.class);
  }

  @Test
  public void testPullInPrimitiveValues() throws Exception {
    String filter = "info_string";

    RequiredPrimitiveFixture toWrite = new RequiredPrimitiveFixture(true, (byte)2, (short)3, 4, (long)5, (double)6.0, "7");
    toWrite.setInfo_string("it's info");

    RequiredPrimitiveFixture toRead = new RequiredPrimitiveFixture(false, (byte)0, (short)0, 0, (long)0, (double)0.0, "");
    toRead.setInfo_string("it's info");

    shouldDoProjectionWithThriftColumnFilter(filter, toWrite, toRead, RequiredPrimitiveFixture.class);
  }

  private void shouldDoProjectionWithThriftColumnFilter(String filterDesc, TBase toWrite, TBase toRead, Class<? extends TBase<?, ?>> thriftClass) throws Exception {
    Configuration conf = new Configuration();
    conf.set(ThriftReadSupport.DEPRECATED_THRIFT_COLUMN_FILTER_KEY, filterDesc);
    shouldDoProjection(conf, toWrite, toRead, thriftClass);
  }


  private <T extends TBase<?, ?>> void shouldDoProjection(Configuration conf, T recordToWrite, T exptectedReadResult, Class<? extends TBase<?, ?>> thriftClass) throws Exception {
    final Path parquetFile = new Path("target/test/TestParquetToThriftReadWriteAndProjection/file.parquet");
    final FileSystem fs = parquetFile.getFileSystem(conf);
    if (fs.exists(parquetFile)) {
      fs.delete(parquetFile, true);
    }

    //create a test file
    final TProtocolFactory protocolFactory = new TCompactProtocol.Factory();
    final TaskAttemptID taskId = new TaskAttemptID("local", 0, true, 0, 0);
    final ThriftToParquetFileWriter w = new ThriftToParquetFileWriter(parquetFile, ContextUtil.newTaskAttemptContext(conf, taskId), protocolFactory, thriftClass);
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final TProtocol protocol = protocolFactory.getProtocol(new TIOStreamTransport(baos));

    recordToWrite.write(protocol);
    w.write(new BytesWritable(baos.toByteArray()));
    w.close();


    final ParquetThriftInputFormat<T> parquetThriftInputFormat = new ParquetThriftInputFormat<T>();
    final Job job = new Job(conf, "read");
    job.setInputFormatClass(ParquetThriftInputFormat.class);
    ParquetThriftInputFormat.setInputPaths(job, parquetFile);
    final JobID jobID = new JobID("local", 1);
    List<InputSplit> splits = parquetThriftInputFormat.getSplits(ContextUtil.newJobContext(ContextUtil.getConfiguration(job), jobID));
    T readValue = null;
    for (InputSplit split : splits) {
      TaskAttemptContext taskAttemptContext = ContextUtil.newTaskAttemptContext(ContextUtil.getConfiguration(job), new TaskAttemptID(new TaskID(jobID, true, 1), 0));
      final RecordReader<Void, T> reader = parquetThriftInputFormat.createRecordReader(split, taskAttemptContext);
      reader.initialize(split, taskAttemptContext);
      if (reader.nextKeyValue()) {
        readValue = reader.getCurrentValue();
        LOG.info(readValue);
      }
    }
    assertEquals(exptectedReadResult, readValue);

  }

}
