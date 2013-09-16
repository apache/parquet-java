package parquet.scrooge;

import org.junit.Test;
import parquet.scrooge.test.TestMap;
import parquet.scrooge.test.TestPersonWithAllInformation;

/**
 * Test convert scrooge schema to Parquet Schema
 */
public class ScroogeSchemaConverterTest {
  @Test
  public void testTraverse() throws Exception{
    new ScroogeSchemaConverter().convert(TestMap.class);
//    traverseStruct(parquet.scrooge.test.RequiredPrimitiveFixture.class.getName());
  }
}
