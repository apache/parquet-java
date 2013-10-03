package parquet.thrift.struct;

import org.apache.thrift.TBase;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import parquet.thrift.ThriftSchemaConverter;
import parquet.thrift.test.compat.*;

import java.io.File;

import static junit.framework.Assert.assertEquals;


public class CompatibilityCheckerTest {

  @Test
  public void testSaveJSON() throws Exception{
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(parquet.thrift.test.TestPersonWithRequiredPhone.class);
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(new File("oh_yeah.json"),expected);
    ThriftType readResult=mapper.readValue(new File("oh_yeah.json"),ThriftType.StructType.class);
    assertEquals(expected.toJSON(),readResult.toJSON());
  }
  @Test
  public void testReadJson() throws Exception{
    ObjectMapper mapper = new ObjectMapper();
    mapper.readValue(new File("oh_yeah.json"),ThriftType.StructType.class);
  }

  @Test
  public void testAddOptionalField(){
    verifyCompatible(StructV1.class,StructV2.class,true);
  }

  @Test
  public void testRemoveOptionalField(){
    verifyCompatible(StructV2.class,StructV1.class,false);
  }

  @Test
  public void testRenameField(){
    verifyCompatible(StructV1.class,RenameStructV1.class,false);
  }

  @Test
  public void testTypeChange(){
    verifyCompatible(StructV1.class,TypeChangeStructV1.class,false);
  }

  @Test
  public void testReuirementChange(){
    //required can become optional or default
    verifyCompatible(StructV1.class,OptionalStructV1.class,true);
    verifyCompatible(StructV1.class,DefaultStructV1.class,true);

    //optional/deafult can not become required
    verifyCompatible(OptionalStructV1.class,StructV1.class,false);
    verifyCompatible(DefaultStructV1.class,StructV1.class,false);
  }
  private ThriftType.StructType struct(Class thriftClass){
    return new ThriftSchemaConverter().toStructType(thriftClass);
  }

  private void verifyCompatible(Class oldClass, Class newClass, boolean expectCompatible){
    CompatibilityChecker checker=new CompatibilityChecker();
    CompatibilityReport report=checker.checkCompatibility(struct(oldClass),struct(newClass));
    assertEquals(expectCompatible,report.isCompatible());
    System.out.println(report.messages);
  }
}
