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

  @Test
  public void testAddRequiredField(){
    verifyCompatible(StructV1.class,AddRequiredStructV1.class,false);
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
