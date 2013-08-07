package parquet.cascading;

import cascading.flow.FlowProcess;
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
import java.util.HashMap;
import java.util.Map;

public class ParquetTBaseSchemeTest {

  @Test
  public void testGetRecordMaterializer() throws Exception {
    ThriftReadSupport<String> readSupport = new ThriftReadSupport<String>();
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
}
