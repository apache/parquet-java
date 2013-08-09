package parquet.cascading;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.tap.Tap;
import com.twitter.data.proto.tutorial.thrift.AddressBook;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.thrift.ThriftReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;
import parquet.thrift.TBaseRecordConverter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestParquetTBaseScheme {

  @Test
  public void testGetRecordMaterializer() throws Exception {
    ThriftReadSupport<AddressBook> readSupport = new ThriftReadSupport<AddressBook>();
    ParquetTBaseScheme tBaseScheme = new ParquetTBaseScheme();
    String descriptorKey = "thrift.descriptor";
    String mockDescriptor = "{\"id\":\"STRUCT\"}";
    Map<String, String> metaData = new HashMap<String, String>();
    metaData.put(descriptorKey, mockDescriptor);
    FlowProcess<JobConf> flowProcess = mock(FlowProcess.class);
    Tap<JobConf, RecordReader, OutputCollector> tap = mock(Tap.class);
    JobConf jobConf = new JobConf();
    jobConf.set(ThriftReadSupport.THRIFT_READ_CLASS_KEY, AddressBook.class.getName());
    MessageType fileSchema = new MessageType("Test");
    ReadSupport.ReadContext readContext = new ReadSupport.ReadContext(fileSchema);

    //Mock the method call by cascading
    tBaseScheme.sourceConfInit(flowProcess, tap, jobConf);

    RecordMaterializer recordMaterializer = readSupport.prepareForRead(jobConf, metaData, fileSchema, readContext);
    assertNotNull(recordMaterializer);
    assertEquals(TBaseRecordConverter.class, recordMaterializer.getClass());
  }

  @Test
  public void testNotSupportSink(){
    ParquetTBaseScheme<AddressBook> scheme=new ParquetTBaseScheme<AddressBook>();
    assertFalse(scheme.isSink());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testNotSinkConfInit(){
    ParquetTBaseScheme<AddressBook> scheme=new ParquetTBaseScheme<AddressBook>();
    scheme.sinkConfInit(mock(FlowProcess.class),
                        mock(Tap.class),
                        mock(JobConf.class));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testNotSink() throws IOException {
    ParquetTBaseScheme<AddressBook> scheme=new ParquetTBaseScheme<AddressBook>();
    scheme.sink(mock(FlowProcess.class),mock(SinkCall.class));
  }
}
