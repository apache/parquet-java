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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import parquet.thrift.test.TestListsInMap;

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
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Test;

import parquet.Log;
import parquet.example.data.Group;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.metadata.ParquetMetadata;

import com.twitter.data.proto.tutorial.thrift.AddressBook;
import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;
import com.twitter.elephantbird.thrift.test.TestListInMap;
import com.twitter.elephantbird.thrift.test.TestMapInList;

import parquet.hadoop.util.ContextUtil;
import parquet.schema.MessageType;

public class TestThriftToParquetFileWriter {
  private static final Log LOG = Log
      .getLog(TestThriftToParquetFileWriter.class);

  @Test
  public void testWriteFile() throws IOException, InterruptedException, TException {
    final AddressBook a = new AddressBook(
        Arrays.asList(
            new Person(
                new Name("Bob", "Roberts"),
                0,
                "bob.roberts@example.com",
                Arrays.asList(new PhoneNumber("1234567890")))));

    final Path fileToCreate = createFile(a);

    ParquetReader<Group> reader = createRecordReader(fileToCreate);

    Group g = null;
    int i = 0;
    while((g = reader.read()) != null) {
      assertEquals(a.persons.size(), g.getFieldRepetitionCount("persons"));
      assertEquals(a.persons.get(0).email, g.getGroup("persons", 0).getGroup(0, 0).getString("email", 0));
      // just some sanity check, we're testing the various layers somewhere else
      ++i;
    }
    assertEquals("read 1 record", 1, i);

  }

  @Test
  public void testWriteFileListOfMap() throws IOException, InterruptedException, TException {
    Map<String, String> map1 = new HashMap<String,String>();
    map1.put("key11", "value11");
    map1.put("key12", "value12");
    Map<String, String> map2 = new HashMap<String,String>();
    map2.put("key21", "value21");
    final TestMapInList listMap = new TestMapInList("listmap",
        Arrays.asList(map1, map2));

    final Path fileToCreate = createFile(listMap);

    ParquetReader<Group> reader = createRecordReader(fileToCreate);

    Group g = null;
    while((g = reader.read()) != null) {
      assertEquals(listMap.names.size(), 
          g.getGroup("names", 0).getFieldRepetitionCount("names_tuple"));
      assertEquals(listMap.names.get(0).size(), 
          g.getGroup("names", 0).getGroup("names_tuple", 0).getFieldRepetitionCount("map"));
      assertEquals(listMap.names.get(1).size(), 
          g.getGroup("names", 0).getGroup("names_tuple", 1).getFieldRepetitionCount("map"));
    }
  }

  @Test
  public void testWriteFileMapOfList() throws IOException, InterruptedException, TException {
    Map<String, List<String>> map = new HashMap<String,List<String>>();
    map.put("key", Arrays.asList("val1","val2"));
    final TestListInMap mapList = new TestListInMap("maplist", map);
    final Path fileToCreate = createFile(mapList);

    ParquetReader<Group> reader = createRecordReader(fileToCreate);

    Group g = null;
    while((g = reader.read()) != null) {
      assertEquals("key", 
          g.getGroup("names", 0).getGroup("map",0).getBinary("key", 0).toStringUsingUTF8());
      assertEquals(map.get("key").size(), 
          g.getGroup("names", 0).getGroup("map",0).getGroup("value", 0).getFieldRepetitionCount(0));
    }
  }

  @Test
  public void testWriteFileMapOfLists() throws IOException, InterruptedException, TException {
    Map<List<String>, List<String>> map = new HashMap<List<String>,List<String>>();
    map.put(Arrays.asList("key1","key2"), Arrays.asList("val1","val2"));
    final TestListsInMap mapList = new TestListsInMap("maplists", map);
    final Path fileToCreate = createFile(mapList);

    ParquetReader<Group> reader = createRecordReader(fileToCreate);

    Group g = null;
    while((g = reader.read()) != null) {
      assertEquals("key1", 
          g.getGroup("names", 0).getGroup("map",0).getGroup("key", 0).getBinary("key_tuple", 0).toStringUsingUTF8());
      assertEquals("key2", 
          g.getGroup("names", 0).getGroup("map",0).getGroup("key", 0).getBinary("key_tuple", 1).toStringUsingUTF8());
      assertEquals("val1", 
          g.getGroup("names", 0).getGroup("map",0).getGroup("value", 0).getBinary("value_tuple", 0).toStringUsingUTF8());
      assertEquals("val2", 
          g.getGroup("names", 0).getGroup("map",0).getGroup("value", 0).getBinary("value_tuple", 1).toStringUsingUTF8());
    }
  }

  private ParquetReader<Group> createRecordReader(Path parquetFilePath) throws IOException {
    Configuration configuration = new Configuration(true);

    GroupReadSupport readSupport = new GroupReadSupport();
    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
    MessageType schema = readFooter.getFileMetaData().getSchema();

    readSupport.init(configuration, null, schema);
    return new ParquetReader<Group>(parquetFilePath, readSupport);
  }

  private <T extends TBase<?,?>> Path createFile(T tObj) throws IOException, InterruptedException, TException  {
    final Path fileToCreate = new Path("target/test/TestThriftToParquetFileWriter/"+tObj.getClass()+".parquet");
    LOG.info("File created: " + fileToCreate.toString());
    Configuration conf = new Configuration();
    final FileSystem fs = fileToCreate.getFileSystem(conf);
    if (fs.exists(fileToCreate)) {
      fs.delete(fileToCreate, true);

    }
    TProtocolFactory protocolFactory = new TCompactProtocol.Factory();
    TaskAttemptID taskId = new TaskAttemptID("local", 0, true, 0, 0);
    ThriftToParquetFileWriter w = new ThriftToParquetFileWriter(fileToCreate, new TaskAttemptContext(conf, taskId), protocolFactory, (Class<? extends TBase<?, ?>>) tObj.getClass());

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final TProtocol protocol = protocolFactory.getProtocol(new TIOStreamTransport(baos));

    tObj.write(protocol);

    w.write(new BytesWritable(baos.toByteArray()));

    w.close();

    return fileToCreate;
  }
}
