package parquet.scrooge;

import org.junit.Test;
import parquet.scrooge.test.TestMap;
import parquet.thrift.ThriftSchemaConverter;
import parquet.thrift.struct.ThriftType;

import static junit.framework.Assert.assertEquals;

/**
 * Test convert scrooge schema to Parquet Schema
 */
public class ScroogeSchemaConverterTest {
  @Test
  public void testConvertPrimitiveMapKey() throws Exception{
    ThriftType.StructType scroogeMap = new ScroogeSchemaConverter().convert(TestMap.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(parquet.thrift.test.TestMap.class);
    assertEquals(expected.toJSON(),scroogeMap.toJSON());
  }
}
