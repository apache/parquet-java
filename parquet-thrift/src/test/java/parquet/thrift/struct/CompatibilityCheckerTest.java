package parquet.thrift.struct;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import parquet.thrift.ThriftSchemaConverter;

import java.io.File;

import static junit.framework.Assert.assertEquals;


public class CompatibilityCheckerTest {

  @Test
  public void testSaveJSON() throws Exception{
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(parquet.thrift.test.TestPersonWithRequiredPhone.class);
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(new File("oh_yeah.json"),expected);
    ThriftType readResult=mapper.readValue(new File("oh_yeah.json"),ThriftType.StructType.class);
    assertEquals(expected,readResult);
  }
  @Test
  public void testReadJson() throws Exception{
    ObjectMapper mapper = new ObjectMapper();
    mapper.readValue(new File("oh_yeah.json"),ThriftType.StringType.class);
  }
}
