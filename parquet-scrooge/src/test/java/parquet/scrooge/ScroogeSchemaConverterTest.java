package parquet.scrooge;

import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import parquet.scrooge.test.*;
import parquet.scrooge.test.Address;
import parquet.scrooge.test.AddressWithStreetWithDefaultRequirement;
import parquet.scrooge.test.Phone;
import parquet.scrooge.test.TestFieldOfEnum;
import parquet.scrooge.test.TestListPrimitive;
import parquet.scrooge.test.TestMapComplex;
import parquet.scrooge.test.TestMapPrimitiveKey;
import parquet.scrooge.test.TestMapPrimitiveValue;
import parquet.scrooge.test.TestOptionalMap;
import parquet.scrooge.test.TestPersonWithAllInformation;
import parquet.scrooge.test.TestSetPrimitive;
import parquet.thrift.ThriftSchemaConverter;
import parquet.thrift.struct.ThriftType;
import parquet.thrift.test.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;

import static junit.framework.Assert.assertEquals;

/**
 * Test convert scrooge schema to Parquet Schema
 */
public class ScroogeSchemaConverterTest {
  @Test
  public void testConvertPrimitiveMapKey() throws Exception{
    ThriftType.StructType scroogeMap = new ScroogeSchemaConverter().convert(TestMapPrimitiveKey.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(parquet.thrift.test.TestMapPrimitiveKey.class);
    assertEquals(expected,scroogeMap);

  }

  @Test
  public void testConvertPrimitiveMapValue() throws Exception{
    ThriftType.StructType scroogeMap = new ScroogeSchemaConverter().convert(TestMapPrimitiveValue.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(parquet.thrift.test.TestMapPrimitiveValue.class);
    assertEquals(expected,scroogeMap);
  }

  @Test
  public void testConvertPrimitiveList() throws Exception{
    ThriftType.StructType scroogeList = new ScroogeSchemaConverter().convert(TestListPrimitive.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(parquet.thrift.test.TestListPrimitive.class);
    assertEquals(expected, scroogeList);
  }

  @Test
     public void testConvertPrimitiveSet() throws Exception{
    ThriftType.StructType scroogeList = new ScroogeSchemaConverter().convert(TestSetPrimitive.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(parquet.thrift.test.TestSetPrimitive.class);
    assertEquals(expected, scroogeList);
  }

  @Test
  public void testConvertEnum() throws Exception{
    ThriftType.StructType scroogeList = new ScroogeSchemaConverter().convert(TestFieldOfEnum.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(parquet.thrift.test.TestFieldOfEnum.class);
    assertEquals(expected, scroogeList);
  }

  @Test
  public void testMapComplex() throws Exception{
    ThriftType.StructType scroogePerson = new ScroogeSchemaConverter().convert(TestMapComplex.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(parquet.thrift.test.TestMapComplex.class);
    assertEquals(expected, scroogePerson);
  }

  @Test
  public void testConvertStruct() throws Exception{
    ThriftType.StructType scroogePerson = new ScroogeSchemaConverter().convert(TestPersonWithAllInformation.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(parquet.thrift.test.TestPersonWithAllInformation.class);
    assertEquals(expected, scroogePerson);
  }

/**
 * TODO: DEFAULT requirement can not be identified, since scrooge does not store the requirement type in generated class
 * Current solution uses reflection based on following rules:
 * if the getter returns option, then it's optional, otherwise it's required
 */
  @Test
  public void testDefaultFields() throws Exception{
    ThriftType.StructType scroogePerson = new ScroogeSchemaConverter().convert(AddressWithStreetWithDefaultRequirement.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(parquet.thrift.test.AddressWithStreetWithDefaultRequirement.class);
//    assertEquals(expected.toJSON(), scroogePerson.toJSON());
  }

  @Test
  public void testConvertOptionalPrimitiveMap() throws Exception{
    ThriftType.StructType scroogeMap = new ScroogeSchemaConverter().convert(TestOptionalMap.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(parquet.thrift.test.TestOptionalMap.class);
    assertEquals(expected,scroogeMap);
  }
}
