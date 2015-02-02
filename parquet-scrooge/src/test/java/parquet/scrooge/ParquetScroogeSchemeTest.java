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
package parquet.scrooge;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Test;
import parquet.cascading.ParquetValueScheme.Config;
import parquet.hadoop.thrift.ThriftToParquetFileWriter;
import parquet.hadoop.util.ContextUtil;
import parquet.scrooge.test.TestPersonWithAllInformation;
import parquet.thrift.test.Address;
import parquet.thrift.test.Phone;
import parquet.thrift.test.RequiredPrimitiveFixture;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Write data in thrift, read in scrooge
 *
 * @author Tianshuo Deng
 */
public class ParquetScroogeSchemeTest {

  public static final String PARQUET_PATH = "target/test/TestParquetToThriftReadProjection/file.parquet";
  public static final String TXT_OUTPUT_PATH = "target/test/TestParquetToThriftReadProjection/output.txt";

  @Test
  public void testWritePrimitveThriftReadScrooge() throws Exception {
    RequiredPrimitiveFixture toWrite = new RequiredPrimitiveFixture(true, (byte)2, (short)3, 4, (long)5, 6.0, "7");
    toWrite.setInfo_string("it's info");
    verifyScroogeRead(thriftRecords(toWrite), parquet.scrooge.test.RequiredPrimitiveFixture.class, "RequiredPrimitiveFixture(true,2,3,4,5,6.0,7,Some(it's info))\n", "**");
  }

  @Test
  public void testNestedReadingInScrooge() throws Exception {
    Map<String, parquet.thrift.test.Phone> phoneMap = new HashMap<String, Phone>();
    phoneMap.put("key1", new parquet.thrift.test.Phone("111", "222"));
    parquet.thrift.test.TestPersonWithAllInformation toWrite = new parquet.thrift.test.TestPersonWithAllInformation(new parquet.thrift.test.Name("first"), new Address("my_street", "my_zip"), phoneMap);
    toWrite.setInfo("my_info");

    String expected = "TestPersonWithAllInformation(Name(first,None),None,Address(my_street,my_zip),None,Some(my_info),Map(key1 -> Phone(111,222)),None,None)\n";
    verifyScroogeRead(thriftRecords(toWrite), TestPersonWithAllInformation.class, expected, "**");

    String expectedProjected = "TestPersonWithAllInformation(Name(first,None),None,Address(my_street,my_zip),None,Some(my_info),Map(),None,None)\n";
    verifyScroogeRead(thriftRecords(toWrite), TestPersonWithAllInformation.class, expectedProjected, "address/*;info;name/first_name");
  }

  private static class ObjectToStringFunction extends BaseOperation implements Function {
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
      Object record = functionCall.getArguments().getObject(0);
      Tuple result = new Tuple();
      result.add(record.toString());
      functionCall.getOutputCollector().add(result);
    }
  }

  public <T> void verifyScroogeRead(List<TBase> recordsToWrite, Class<T> readClass, String expectedStr, String projectionFilter) throws Exception {
    Configuration conf = new Configuration();
    deleteIfExist(PARQUET_PATH);
    deleteIfExist(TXT_OUTPUT_PATH);
    final Path parquetFile = new Path(PARQUET_PATH);
    writeParquetFile(recordsToWrite, conf, parquetFile);

    Scheme sourceScheme = new ParquetScroogeScheme(new Config().withRecordClass(readClass).withProjectionString(projectionFilter));
    Tap source = new Hfs(sourceScheme, PARQUET_PATH);

    Scheme sinkScheme = new TextLine(new Fields("first", "last"));
    Tap sink = new Hfs(sinkScheme, TXT_OUTPUT_PATH);

    Pipe assembly = new Pipe("namecp");
    assembly = new Each(assembly, new ObjectToStringFunction());
    Flow flow = new HadoopFlowConnector().connect("namecp", source, sink, assembly);

    flow.complete();
    String result = FileUtils.readFileToString(new File(TXT_OUTPUT_PATH + "/part-00000"));
    assertEquals(expectedStr, result);
  }

  private void writeParquetFile(List<TBase> recordsToWrite, Configuration conf, Path parquetFile) throws IOException, InterruptedException, org.apache.thrift.TException {
    //create a test file
    final TProtocolFactory protocolFactory = new TCompactProtocol.Factory();
    final TaskAttemptID taskId = new TaskAttemptID("local", 0, true, 0, 0);
    Class writeClass = recordsToWrite.get(0).getClass();
    final ThriftToParquetFileWriter w = new ThriftToParquetFileWriter(parquetFile, ContextUtil.newTaskAttemptContext(conf, taskId), protocolFactory, writeClass);
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final TProtocol protocol = protocolFactory.getProtocol(new TIOStreamTransport(baos));
    for (TBase recordToWrite : recordsToWrite) {
      recordToWrite.write(protocol);
    }
    w.write(new BytesWritable(baos.toByteArray()));
    w.close();
  }

  private List<TBase> thriftRecords(TBase... records) {
    List<TBase> result = new ArrayList<TBase>();
    for (TBase record : records) {
      result.add(record);
    }
    return result;
  }

  private void deleteIfExist(String path) throws IOException {
    Path p = new Path(path);
    Configuration conf = new Configuration();
    final FileSystem fs = p.getFileSystem(conf);
    if (fs.exists(p)) {
      fs.delete(p, true);
    }
  }
}
