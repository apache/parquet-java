package parquet.column.values.delta;

import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
public class DeltaBinaryPackingValuesWriterTest {
  @Test
  public void shouldWriteWhenDataIsAlignedWithBlock() throws IOException {
    int blockSize=128;
//    int miniBlockSize=32;
    int dataSize=blockSize*5;
//    int[] data=new int[dataSize];
//    generateRandomInteger(data);
    DeltaBinaryPackingValuesWriter writer=new DeltaBinaryPackingValuesWriter(blockSize,4,100);
    for(int i=0;i<blockSize*5;i++){
      writer.writeInteger(i*32);
    }
    System.out.println(writer.getBytes());
    DeltaBinaryPackingValuesReader reader=new DeltaBinaryPackingValuesReader();
    reader.initFromPage(100,writer.getBytes().toByteArray(),0);

    for(int i=0;i<blockSize*5;i++){
      assertEquals(reader.readInteger(),i*32);
//      System.out.println(reader.readInteger());
    }
  }

//  private void generateRandomInteger(int[] data) {
//    Random random = new Random();
//    for(int i=0;i<data.length;i++){
//      data[i]=random.nextInt(100);
//    }
//  }


}
