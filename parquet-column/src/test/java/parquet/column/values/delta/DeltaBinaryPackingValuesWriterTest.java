package parquet.column.values.delta;

import org.junit.Test;
import parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
public class DeltaBinaryPackingValuesWriterTest {
  @Test
  public void shouldWriteWhenDataIsAlignedWithBlock() throws IOException {
    int blockSize=128;
//    int miniBlockSize=32;
//    int dataSize=blockSize*5;
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

  @Test
  public void shouldWriteWhenDataIs0() throws IOException {
    int blockSize=128;
//    int miniBlockSize=32;
//    int dataSize=blockSize*5;
//    int[] data=new int[dataSize];
//    generateRandomInteger(data);
    DeltaBinaryPackingValuesWriter writer=new DeltaBinaryPackingValuesWriter(blockSize,4,100);
    for(int i=0;i<blockSize*5;i++){
      writer.writeInteger(i*5);
    }

    for(int i=0;i<blockSize;i++)
      writer.writeInteger(0);

    System.out.println(writer.getBytes());
    DeltaBinaryPackingValuesReader reader=new DeltaBinaryPackingValuesReader();
    reader.initFromPage(100,writer.getBytes().toByteArray(),0);

    for(int i=0;i<blockSize*5;i++){
      assertEquals(reader.readInteger(),i*5);
//      System.out.println(reader.readInteger());
    }

    for(int i=0;i<blockSize;i++)
      assertEquals(reader.readInteger(),0);
  }



  @Test
  public void shouldReadWriteWhenDataIsNotAlignedWithBlock() throws IOException {
    int blockSize=128;
//    int miniBlockSize=32;
//    int dataSize=blockSize*5;
//    int[] data=new int[dataSize];
//    generateRandomInteger(data);
    DeltaBinaryPackingValuesWriter writer=new DeltaBinaryPackingValuesWriter(blockSize,4,100);
    for(int i=0;i<blockSize*5+1;i++){
      writer.writeInteger(i*32);
    }
    System.out.println(writer.getBytes());
    DeltaBinaryPackingValuesReader reader=new DeltaBinaryPackingValuesReader();
    reader.initFromPage(100,writer.getBytes().toByteArray(),0);

    for(int i=0;i<blockSize*5+1;i++){
      assertEquals(reader.readInteger(),i*32);
//      System.out.println(reader.readInteger());
    }
  }

  @Test
  public void shouldThrowExceptionWhenReadMoreThanWritten() throws IOException {
    int blockSize=128;
//    int miniBlockSize=32;
//    int dataSize=blockSize*5;
//    int[] data=new int[dataSize];
//    generateRandomInteger(data);
    DeltaBinaryPackingValuesWriter writer=new DeltaBinaryPackingValuesWriter(blockSize,4,100);
    for(int i=0;i<blockSize*5;i++){
      writer.writeInteger(i*32);
    }
    DeltaBinaryPackingValuesReader reader=new DeltaBinaryPackingValuesReader();
    reader.initFromPage(100,writer.getBytes().toByteArray(),0);
    for(int i=0;i<blockSize*5;i++){
      assertEquals(reader.readInteger(),i*32);
    }

    try{
      reader.readInteger();
    } catch(ParquetDecodingException e){
      assertEquals("no more value to read, total value count is 640" , e.getMessage());
    }

  }
  //TODO test for 0

  //TODO test for not aligned data count

  private void generateRandomInteger(int[] data) {
    Random random = new Random();
    for(int i=0;i<data.length;i++){
      data[i]=random.nextInt(100);
    }
  }


}
