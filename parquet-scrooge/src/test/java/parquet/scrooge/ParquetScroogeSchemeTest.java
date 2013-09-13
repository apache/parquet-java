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
import parquet.thrift.test.RequiredListFixture;
import parquet.thrift.test.RequiredMapFixture;
import parquet.thrift.test.RequiredPrimitiveFixture;
import parquet.thrift.test.RequiredSetFixture;
import scala.collection.JavaConversions;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: tdeng
 * Date: 9/12/13
 * Time: 12:21 PM
 * To change this template use File | Settings | File Templates.
 */
public class ParquetScroogeSchemeTest {
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
    TBase toWrite=new AddressBook(
            Arrays.asList(
                    new Person(
                            new Name("Bob", "Roberts"),
                            0,
                            "bob.roberts@example.com",
                            Arrays.asList(new PhoneNumber("1234567890")))));

    TBase toRead=new AddressBook(
            Arrays.asList(
                    new Person(
                            new Name("Bob", "Roberts"),
                            0,
                            null,
                            null)));
    shouldDoProjection(conf,toWrite,toRead,AddressBook.class);
  }

  @Test
  public void testPullingInRequiredStructWithFilter() throws Exception {
    final String projectionFilterDesc = "persons/{id};persons/email";
    TBase toWrite=new AddressBook(
            Arrays.asList(
                    new Person(
                            new Name("Bob", "Roberts"),
                            0,
                            "bob.roberts@example.com",
                            Arrays.asList(new PhoneNumber("1234567890")))));

    TBase toRead=new AddressBook(
            Arrays.asList(
                    new Person(
                            new Name("", ""),
                            0,
                            "bob.roberts@example.com",
                            null)));
    shouldDoProjectionWithThriftColumnFilter(projectionFilterDesc,toWrite,toRead,AddressBook.class);
  }

  @Test
  public void testNotPullInOptionalFields() throws Exception {
    final String projectionFilterDesc = "nomatch";
    TBase toWrite=new AddressBook(
            Arrays.asList(
                    new Person(
                            new Name("Bob", "Roberts"),
                            0,
                            "bob.roberts@example.com",
                            Arrays.asList(new PhoneNumber("1234567890")))));

    TBase toRead=new AddressBook();
    shouldDoProjectionWithThriftColumnFilter(projectionFilterDesc, toWrite, toRead,AddressBook.class);
  }

  @Test
  public void testPullInRequiredMaps() throws Exception{
    String filter="name";

    Map<String,String> mapValue=new HashMap<String,String>();
    mapValue.put("a","1");
    mapValue.put("b","2");
    RequiredMapFixture toWrite= new RequiredMapFixture(mapValue);
    toWrite.setName("testName");

    RequiredMapFixture toRead=new RequiredMapFixture(new HashMap<String,String>());
    toRead.setName("testName");

    shouldDoProjectionWithThriftColumnFilter(filter,toWrite,toRead,RequiredMapFixture.class);
  }

  @Test
  public void testPullInRequiredLists() throws Exception{
    String filter="info";

    RequiredListFixture toWrite=new RequiredListFixture(Arrays.asList(new parquet.thrift.test.Name("first_name")));
    toWrite.setInfo("test_info");

    RequiredListFixture toRead=new RequiredListFixture(new ArrayList<parquet.thrift.test.Name>());
    toRead.setInfo("test_info");

    shouldDoProjectionWithThriftColumnFilter(filter,toWrite,toRead,RequiredListFixture.class);
  }

  @Test
  public void testPullInRequiredSets() throws Exception{
    String filter="info";

    RequiredSetFixture toWrite=new RequiredSetFixture(new HashSet<parquet.thrift.test.Name>(Arrays.asList(new parquet.thrift.test.Name("first_name"))));
    toWrite.setInfo("test_info");

    RequiredSetFixture toRead=new RequiredSetFixture(new HashSet<parquet.thrift.test.Name>());
    toRead.setInfo("test_info");

    shouldDoProjectionWithThriftColumnFilter(filter,toWrite,toRead,RequiredSetFixture.class);
  }

  @Test
  public void testPullInPrimitiveValues() throws Exception{
    String filter="info_string";

    RequiredPrimitiveFixture toWrite= new RequiredPrimitiveFixture(true,(byte)2,(short)3,4,(long)5,(double)6.0,"7");
    toWrite.setInfo_string("it's info");

    RequiredPrimitiveFixture toRead= new RequiredPrimitiveFixture(false,(byte)0,(short)0,0,(long)0,(double)0.0,"");
    toRead.setInfo_string("it's info");

    shouldDoProjectionWithThriftColumnFilter(filter,toWrite,toRead,RequiredPrimitiveFixture.class);
  }


  @Test
  public void testTraverse() throws Exception{
    new ScroogeSchemaConverter().convert(TestPersonWithAllInformation.class);
//    traverseStruct(parquet.scrooge.test.RequiredPrimitiveFixture.class.getName());
  }

  @Test
  public void testScroogeRead() throws Exception{
//    Class<?> companionClass=Class.forName(parquet.scrooge.test.RequiredPrimitiveFixture.class.getName()+"$");
    Class<?> companionClass=Class.forName(parquet.scrooge.test.TestPersonWithRequiredPhone.class.getName()+"$");
    ThriftStructCodec cObject=(ThriftStructCodec<?>)companionClass.getField("MODULE$").get(null);

    Iterable<ThriftStructField> ss = JavaConversions.asIterable(cObject.metaData().fields());
    for(ThriftStructField f:ss){
      System.out.println(f.tfield().name);
      String fieldName=f.tfield().name;
      short fieldId=f.tfield().id;
      byte thriftType=f.tfield().type;
      System.out.println("ho");
    }




    Configuration conf = new Configuration();
    conf.set(ThriftReadSupport.THRIFT_COLUMN_FILTER_KEY, "**");
    conf.set("parquet.thrift.converter.class",ScroogeRecordConverter.class.getCanonicalName());
    conf.set(ThriftReadSupport.THRIFT_READ_CLASS_KEY,"parquet.scrooge.test.RequiredPrimitiveFixture");

    RequiredPrimitiveFixture recordToWrite= new RequiredPrimitiveFixture(true,(byte)2,(short)3,4,(long)5,(double)6.0,"7");

    final Path parquetFile = new Path("target/test/TestParquetToThriftReadProjection/file.parquet");
    final FileSystem fs = parquetFile.getFileSystem(conf);
    if (fs.exists(parquetFile)) {
      fs.delete(parquetFile, true);
    }

    //create a test file
    final TProtocolFactory protocolFactory = new TCompactProtocol.Factory();
    final TaskAttemptID taskId = new TaskAttemptID("local", 0, true, 0, 0);
    final ThriftToParquetFileWriter w = new ThriftToParquetFileWriter(parquetFile, new TaskAttemptContext(conf, taskId), protocolFactory, RequiredPrimitiveFixture.class);
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final TProtocol protocol = protocolFactory.getProtocol(new TIOStreamTransport(baos));

    recordToWrite.write(protocol);
    w.write(new BytesWritable(baos.toByteArray()));
    w.close();

    //S could be TBASE or Scrooge
    final ParquetThriftInputFormat<parquet.scrooge.test.RequiredPrimitiveFixture> parquetThriftInputFormat = new ParquetThriftInputFormat<parquet.scrooge.test.RequiredPrimitiveFixture>();
    final Job job = new Job(conf, "read");
    job.setInputFormatClass(ParquetThriftInputFormat.class);
    ParquetThriftInputFormat.setInputPaths(job, parquetFile);
    final JobID jobID = new JobID("local", 1);
    List<InputSplit> splits = parquetThriftInputFormat.getSplits(new JobContext(ContextUtil.getConfiguration(job), jobID));
    parquet.scrooge.test.RequiredPrimitiveFixture readValue = null;
    for (InputSplit split : splits) {
      TaskAttemptContext taskAttemptContext = new TaskAttemptContext(ContextUtil.getConfiguration(job), new TaskAttemptID(new TaskID(jobID, true, 1), 0));
      final RecordReader<Void, parquet.scrooge.test.RequiredPrimitiveFixture> reader = parquetThriftInputFormat.createRecordReader(split, taskAttemptContext);
      reader.initialize(split, taskAttemptContext);
      if (reader.nextKeyValue()) {
        readValue = reader.getCurrentValue();
//        LOG.info(readValue);
      }
    }
    System.out.println(readValue);



  }

  private void shouldDoProjectionWithThriftColumnFilter(String filterDesc,TBase toWrite, TBase toRead,Class<? extends TBase<?,?>> thriftClass) throws Exception {
    Configuration conf = new Configuration();
    conf.set(ThriftReadSupport.THRIFT_COLUMN_FILTER_KEY, filterDesc);
    shouldDoProjection(conf,toWrite,toRead,thriftClass);
  }


  private <T extends TBase<?,?>,S> void shouldDoProjection(Configuration conf,T recordToWrite,S exptectedReadResult, Class<? extends TBase<?,?>> thriftClass) throws Exception {
    final Path parquetFile = new Path("target/test/TestParquetToThriftReadProjection/file.parquet");
    final FileSystem fs = parquetFile.getFileSystem(conf);
    if (fs.exists(parquetFile)) {
      fs.delete(parquetFile, true);
    }

    //create a test file
    final TProtocolFactory protocolFactory = new TCompactProtocol.Factory();
    final TaskAttemptID taskId = new TaskAttemptID("local", 0, true, 0, 0);
    final ThriftToParquetFileWriter w = new ThriftToParquetFileWriter(parquetFile, new TaskAttemptContext(conf, taskId), protocolFactory, thriftClass);
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final TProtocol protocol = protocolFactory.getProtocol(new TIOStreamTransport(baos));

    recordToWrite.write(protocol);
    w.write(new BytesWritable(baos.toByteArray()));
    w.close();

    //S could be TBASE or Scrooge
    final ParquetThriftInputFormat<S> parquetThriftInputFormat = new ParquetThriftInputFormat<S>();
    final Job job = new Job(conf, "read");
    job.setInputFormatClass(ParquetThriftInputFormat.class);
    ParquetThriftInputFormat.setInputPaths(job, parquetFile);
    final JobID jobID = new JobID("local", 1);
    List<InputSplit> splits = parquetThriftInputFormat.getSplits(new JobContext(ContextUtil.getConfiguration(job), jobID));
    S readValue = null;
    for (InputSplit split : splits) {
      TaskAttemptContext taskAttemptContext = new TaskAttemptContext(ContextUtil.getConfiguration(job), new TaskAttemptID(new TaskID(jobID, true, 1), 0));
      final RecordReader<Void, S> reader = parquetThriftInputFormat.createRecordReader(split, taskAttemptContext);
      reader.initialize(split, taskAttemptContext);
      if (reader.nextKeyValue()) {
        readValue = reader.getCurrentValue();
//        LOG.info(readValue);
      }
    }
    assertEquals(exptectedReadResult, readValue);

  }

}
