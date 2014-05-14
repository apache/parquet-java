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
import cascading.tuple.TupleEntry;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Test;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.thrift.ParquetThriftInputFormat;
import parquet.hadoop.thrift.ThriftReadSupport;
import parquet.hadoop.thrift.ThriftToParquetFileWriter;
import parquet.hadoop.util.ContextUtil;
import parquet.scrooge.test.TestPersonWithAllInformation;
import parquet.thrift.test.Address;
import parquet.thrift.test.Phone;
import parquet.thrift.test.RequiredPrimitiveFixture;
import parquet.scrooge.test.Name;
import parquet.scrooge.test.Name$;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import scala.Option;

import static org.junit.Assert.assertEquals;

/**
 * Write data in thrift, read in scrooge
 *
 * @author Tianshuo Deng
 */
public class ParquetScroogeSchemeTest {
  @Test
  public void testWritePrimitveThriftReadScrooge() throws Exception {
    RequiredPrimitiveFixture toWrite = new RequiredPrimitiveFixture(true, (byte) 2, (short) 3, 4, (long) 5, (double) 6.0, "7");
    toWrite.setInfo_string("it's info");
    verifyScroogeRead(toWrite, parquet.scrooge.test.RequiredPrimitiveFixture.class, "RequiredPrimitiveFixture(true,2,3,4,5,6.0,7,Some(it's info))","**");
  }

  @Test
  public void testNestedReadingInScrooge() throws Exception {
    Map<String, parquet.thrift.test.Phone> phoneMap = new HashMap<String, Phone>();
    phoneMap.put("key1", new parquet.thrift.test.Phone("111", "222"));
    parquet.thrift.test.TestPersonWithAllInformation toWrite = new parquet.thrift.test.TestPersonWithAllInformation(new parquet.thrift.test.Name("first"), new Address("my_street", "my_zip"), phoneMap);
    toWrite.setInfo("my_info");
    String expected = "TestPersonWithAllInformation(Name(first,None),None,Address(my_street,my_zip),None,Some(my_info),Map(key1 -> Phone(111,222)),None,None)";
    verifyScroogeRead(toWrite, TestPersonWithAllInformation.class, expected,"**");
    String expectedProjected = "TestPersonWithAllInformation(Name(first,None),None,Address(my_street,my_zip),None,Some(my_info),Map(),None,None)";
    verifyScroogeRead(toWrite, TestPersonWithAllInformation.class, expectedProjected,"address/*;info;name/first_name");
  }

  public <T> void verifyScroogeRead(TBase recordToWrite, Class<T> readClass, String expectedStr, String projectionFilter) throws Exception {
    Configuration conf = new Configuration();
    conf.set("parquet.thrift.converter.class", ScroogeRecordConverter.class.getName());
    conf.set(ThriftReadSupport.THRIFT_READ_CLASS_KEY, readClass.getName());
    conf.set(ThriftReadSupport.THRIFT_COLUMN_FILTER_KEY, projectionFilter);

    final Path parquetFile = new Path("target/test/TestParquetScroogeScheme/file.parquet");
    final FileSystem fs = parquetFile.getFileSystem(conf);
    if (fs.exists(parquetFile)) {
      fs.delete(parquetFile, true);
    }

    //create a test file
    final TProtocolFactory protocolFactory = new TCompactProtocol.Factory();
    final TaskAttemptID taskId = new TaskAttemptID("local", 0, true, 0, 0);
    Class writeClass = recordToWrite.getClass();
    final ThriftToParquetFileWriter w = new ThriftToParquetFileWriter(parquetFile, ContextUtil.newTaskAttemptContext(conf, taskId), protocolFactory, writeClass);
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final TProtocol protocol = protocolFactory.getProtocol(new TIOStreamTransport(baos));

    recordToWrite.write(protocol);
    w.write(new BytesWritable(baos.toByteArray()));
    w.close();

    final ParquetScroogeInputFormat<T> parquetScroogeInputFormat = new ParquetScroogeInputFormat<T>();
    final Job job = new Job(conf, "read");
    job.setInputFormatClass(ParquetThriftInputFormat.class);
    ParquetThriftInputFormat.setInputPaths(job, parquetFile);
    final JobID jobID = new JobID("local", 1);
    List<InputSplit> splits = parquetScroogeInputFormat.getSplits(ContextUtil.newJobContext(ContextUtil.getConfiguration(job), jobID));
    T readValue = null;
    for (InputSplit split : splits) {
      TaskAttemptContext taskAttemptContext = ContextUtil.newTaskAttemptContext(ContextUtil.getConfiguration(job), new TaskAttemptID(new TaskID(jobID, true, 1), 0));
      final RecordReader<Void, T> reader = parquetScroogeInputFormat.createRecordReader(split, taskAttemptContext);
      reader.initialize(split, taskAttemptContext);
      if (reader.nextKeyValue()) {
        readValue = reader.getCurrentValue();
      }
    }
    assertEquals(expectedStr, readValue.toString());
  }

  final String txtInputPath = "src/test/resources/names.txt";
  final String parquetOutputPath = "target/test/ParquetScroogeScheme/names-parquet-out";
  final String txtOutputPath = "target/test/ParquetScroogeScheme/names-txt-out";

  @Test
  public void testWriteThenRead() throws Exception {
    doWrite();
    doRead();
  }

  private void doWrite() throws Exception {
    Path path = new Path(parquetOutputPath);
    final FileSystem fs = path.getFileSystem(new Configuration());
    if (fs.exists(path)) fs.delete(path, true);

    Scheme sourceScheme = new TextLine( new Fields( "first", "last" ) );
    Tap source = new Hfs(sourceScheme, txtInputPath);

    Scheme sinkScheme = new ParquetScroogeScheme<Name>(Name.class);
    Tap sink = new Hfs(sinkScheme, parquetOutputPath);

    Pipe assembly = new Pipe( "namecp" );
    assembly = new Each(assembly, new PackThriftFunction());
    Flow flow  = new HadoopFlowConnector().connect("namecp", source, sink, assembly);

    flow.complete();
  }

  private void doRead() throws Exception {
    Path path = new Path(txtOutputPath);
    final FileSystem fs = path.getFileSystem(new Configuration());
    if (fs.exists(path)) fs.delete(path, true);

    Scheme sourceScheme = new ParquetScroogeScheme<Name>(Name.class);
    Tap source = new Hfs(sourceScheme, parquetOutputPath);

    Scheme sinkScheme = new TextLine(new Fields("first", "last"));
    Tap sink = new Hfs(sinkScheme, txtOutputPath);

    Pipe assembly = new Pipe( "namecp" );
    assembly = new Each(assembly, new UnpackThriftFunction());
    Flow flow  = new HadoopFlowConnector().connect("namecp", source, sink, assembly);

    flow.complete();
    String result = FileUtils.readFileToString(new File(txtOutputPath+"/part-00000"));
    assertEquals("0\tAlice\tPractice\n15\tBob\tHope\n24\tCharlie\tHorse\n", result);
  }

  private static class PackThriftFunction extends BaseOperation implements Function {
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
      TupleEntry arguments = functionCall.getArguments();
      Tuple result = new Tuple();

      Name name = Name$.MODULE$.apply(arguments.getString(0), Option.apply(arguments.getString(1)));

      result.add(name);
      functionCall.getOutputCollector().add(result);
    }
  }

  private static class UnpackThriftFunction extends BaseOperation implements Function {
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
      TupleEntry arguments = functionCall.getArguments();
      Tuple result = new Tuple();

      Name name = (Name) arguments.getObject(0);
      result.add(name.firstName());
      result.add(name.lastName().get());
      functionCall.getOutputCollector().add(result);
    }
  }
}
