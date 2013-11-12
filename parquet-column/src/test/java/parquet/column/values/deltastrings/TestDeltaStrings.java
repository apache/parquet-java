package parquet.column.values.deltastrings;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import parquet.io.api.Binary;

public class TestDeltaStrings {
  
  @Test
  public void testSerialization () throws IOException {
    DeltaStringValuesWriter writer = new DeltaStringValuesWriter(64*1024);
    
    Binary b1 = Binary.fromString("parquet-format");
    Binary b2 = Binary.fromString("parquet-mr");
    Binary b3 = Binary.fromString("parquet");
    
    writer.writeBytes(b1);
    writer.writeBytes(b2);
    writer.writeBytes(b3);
    
    DeltaStringValuesReader reader = new DeltaStringValuesReader();
    reader.initFromPage(3, writer.getBytes().toByteArray(), 0);
    
    Assert.assertEquals(reader.readBytes(), b1);
    Assert.assertEquals(reader.readBytes(), b2);
    Assert.assertEquals(reader.readBytes(), b3);
  }

}
