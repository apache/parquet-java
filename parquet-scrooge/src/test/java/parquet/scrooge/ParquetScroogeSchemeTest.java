package parquet.scrooge;

import com.twitter.data.proto.tutorial.thrift.AddressBook;
import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;
import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.scrooge.ThriftStructCodec;
import com.twitter.scrooge.ThriftStructField;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.record.meta.TypeID;
import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Test;
import parquet.Log;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.thrift.ParquetThriftInputFormat;
import parquet.hadoop.thrift.TestParquetToThriftReadProjection;
import parquet.hadoop.thrift.ThriftReadSupport;
import parquet.hadoop.thrift.ThriftToParquetFileWriter;
import parquet.hadoop.util.ContextUtil;
import parquet.scrooge.test.TestPersonWithAllInformation;
import parquet.thrift.struct.ThriftType;
import parquet.thrift.struct.ThriftTypeID;
import parquet.thrift.test.*;
import scala.collection.JavaConversions;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Write data in thrift, read in scrooge
 */
public class ParquetScroogeSchemeTest {

  @Test
  public void testTraverse() throws Exception{
    new ScroogeSchemaConverter().convert(TestPersonWithAllInformation.class);
//    traverseStruct(parquet.scrooge.test.RequiredPrimitiveFixture.class.getName());
  }

  @Test
  public void testNestedReadingInScrooge() throws Exception{
    Map<String,parquet.thrift.test.Phone> phoneMap=new HashMap<String,Phone>();
    phoneMap.put("key1",new parquet.thrift.test.Phone("111","222"));
    parquet.thrift.test.TestPersonWithAllInformation toWrite= new parquet.thrift.test.TestPersonWithAllInformation(new parquet.thrift.test.Name("first"),new Address("my_street","my_zip"),"my_info",phoneMap);
    String expected = "TestPersonWithAllInformation(Name(first,None),None,Address(my_street,my_zip),None,my_info,Map(key1 -> Phone(111,222)),None,None)";
    verifyScroogeRead(toWrite,TestPersonWithAllInformation.class, expected);
  }


  @Test
  public void testWritePrimitveThriftReadScrooge() throws Exception{
    RequiredPrimitiveFixture toWrite= new RequiredPrimitiveFixture(true,(byte)2,(short)3,4,(long)5,(double)6.0,"7");
    toWrite.setInfo_string("it's info");
    verifyScroogeRead(toWrite,parquet.scrooge.test.RequiredPrimitiveFixture.class,"RequiredPrimitiveFixture(true,2,3,4,5,6.0,7,Some(it's info))");
  }

  public <T> void verifyScroogeRead(TBase recordToWrite, Class<T> readClass, String expectedStr) throws Exception{
    Configuration conf = new Configuration();
    conf.set("parquet.thrift.converter.class",ScroogeRecordConverter.class.getCanonicalName());
    conf.set(ThriftReadSupport.THRIFT_READ_CLASS_KEY,readClass.getName());

    final Path parquetFile = new Path("target/test/TestParquetToThriftReadProjection/file.parquet");
    final FileSystem fs = parquetFile.getFileSystem(conf);
    if (fs.exists(parquetFile)) {
      fs.delete(parquetFile, true);
    }

    //create a test file
    final TProtocolFactory protocolFactory = new TCompactProtocol.Factory();
    final TaskAttemptID taskId = new TaskAttemptID("local", 0, true, 0, 0);
    Class writeClass=recordToWrite.getClass();
    final ThriftToParquetFileWriter w = new ThriftToParquetFileWriter(parquetFile, new TaskAttemptContext(conf, taskId), protocolFactory, writeClass);
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
    List<InputSplit> splits = parquetThriftInputFormat.getSplits(new JobContext(ContextUtil.getConfiguration(job), jobID));
    T readValue = null;
    for (InputSplit split : splits) {
      TaskAttemptContext taskAttemptContext = new TaskAttemptContext(ContextUtil.getConfiguration(job), new TaskAttemptID(new TaskID(jobID, true, 1), 0));
      final RecordReader<Void, T> reader = parquetThriftInputFormat.createRecordReader(split, taskAttemptContext);
      reader.initialize(split, taskAttemptContext);
      if (reader.nextKeyValue()) {
        readValue = reader.getCurrentValue();
      }
    }
   assertEquals(expectedStr,readValue.toString());
  }


}
