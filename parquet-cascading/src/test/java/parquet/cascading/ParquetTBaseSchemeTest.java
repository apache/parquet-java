package parquet.cascading;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import com.twitter.data.proto.tutorial.thrift.AddressBook;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Test;
import static org.junit.Assert.*;
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
    ThriftReadSupport<String> mySupport = new ThriftReadSupport<String>();
    ParquetTBaseScheme tBaseScheme = new ParquetTBaseScheme();
    String descriptorKey = "thrift.descriptor";
    String mockDescriptor = "{\"id\":\"STRUCT\"}";
    FlowProcess<JobConf> fp = null;
    Tap<JobConf, RecordReader, OutputCollector> tap = null;
    JobConf jobConf = new JobConf();
    Map<String, String> metaData = new HashMap<String, String>();
    jobConf.set(ThriftReadSupport.THRIFT_READ_CLASS_KEY, AddressBook.class.getName());
    metaData.put(descriptorKey, mockDescriptor);
    MessageType fileSchema = new MessageType("Test");
    ReadSupport.ReadContext readContext = new ReadSupport.ReadContext(fileSchema);

    //Mock the method call by cascading
    tBaseScheme.sourceConfInit(fp, tap, jobConf);

    RecordMaterializer recordMaterializer = mySupport.prepareForRead(jobConf, metaData, fileSchema, readContext);
    assertNotNull(recordMaterializer);
    assertEquals(TBaseRecordConverter.class, recordMaterializer.getClass());
  }
}
