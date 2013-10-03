package parquet.thrift.struct;

import org.apache.thrift.TBase;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import parquet.thrift.ThriftSchemaConverter;
import parquet.thrift.test.compat.*;

import java.io.File;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;


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
    CompatibilityChecker checker=new CompatibilityChecker();
    CompatibilityReport report = checker.checkCompatibility(struct(StructV1.class), struct(StructV2.class));
    assertTrue(report.isCompatible);
    System.out.println(report.messages);
  }

  @Test
  public void testRemoveOptionalField(){
    CompatibilityChecker checker=new CompatibilityChecker();
    CompatibilityReport report=checker.checkCompatibility(struct(StructV2.class),struct(StructV1.class));
    assertFalse(report.isCompatible());
    System.out.println(report.messages);
  }

  @Test
  public void testRenameField(){
    CompatibilityChecker checker=new CompatibilityChecker();
    CompatibilityReport report=checker.checkCompatibility(struct(StructV1.class),struct(RenameStructV1.class));
    assertFalse(report.isCompatible());
    System.out.println(report.messages);
  }

  @Test
  public void testTypeChange(){
    CompatibilityChecker checker=new CompatibilityChecker();
    CompatibilityReport report=checker.checkCompatibility(struct(StructV1.class),struct(TypeChangeStructV1.class));
    assertFalse(report.isCompatible());
    System.out.println(report.messages);
  }

  @Test
  public void testReuirementChange(){
    //required can become optional
    CompatibilityChecker checker=new CompatibilityChecker();
    CompatibilityReport report=checker.checkCompatibility(struct(StructV1.class),struct(OptionalStructV1.class));
    assertTrue(report.isCompatible());
    System.out.println(report.messages);

    //optional can not become required
    report=checker.checkCompatibility(struct(OptionalStructV1.class),struct(StructV1.class));
    assertFalse(report.isCompatible());
    System.out.println(report.messages);
  }
  private ThriftType.StructType struct(Class thriftClass){
    return new ThriftSchemaConverter().toStructType(thriftClass);
  }
}
