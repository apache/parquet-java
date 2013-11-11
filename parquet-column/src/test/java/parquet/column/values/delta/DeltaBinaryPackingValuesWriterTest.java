package parquet.column.values.delta;

import org.junit.Before;
import org.junit.Test;
import parquet.bytes.BytesInput;
import parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class DeltaBinaryPackingValuesWriterTest {
  DeltaBinaryPackingValuesReader reader;
  private int blockSize;
  private int miniBlockNum;
  private DeltaBinaryPackingValuesWriter writer;

  @Before
  public void setUp() {
    blockSize = 128;
    miniBlockNum = 4;
    writer = new DeltaBinaryPackingValuesWriter(blockSize, miniBlockNum, 100);
  }

  @Test(expected = AssertionError.class)
  public void miniBlockSizeShouldBeMultipleOf8() {
    new DeltaBinaryPackingValuesWriter(1281, 4, 100);
    new DeltaBinaryPackingValuesWriter(128, 3, 100);
  }

  /* When data size is multiple of Block*/
  @Test
  public void shouldWriteWhenDataIsAlignedWithBlock() throws IOException {
    int[] data = new int[5 * blockSize];
    for (int i = 0; i < blockSize * 5; i++) {
      data[i] = i * 32;
    }
    shouldReadAndWrite(data);
  }

  @Test
  public void shouldWriteAndReadWhenBlockIsNotFullyWritten() throws IOException {
    int[] data = new int[blockSize - 3];
    for (int i = 0; i < data.length; i++) {
      data[i] = i * 32;
    }
    shouldReadAndWrite(data);
  }

  @Test
  public void shouldWriteAndReadWhenAMiniBlockIsNotFullyWritten() throws IOException {
    int miniBlockSize = blockSize / miniBlockNum;
    int[] data = new int[miniBlockSize - 3];
    for (int i = 0; i < data.length; i++) {
      data[i] = i * 32;
    }
    shouldReadAndWrite(data);
  }

  @Test
  public void shouldWriteNegativeDeltas() throws IOException {
    int[] data = new int[blockSize];
    for (int i = 0; i < data.length; i++) {
      data[i] = 10 - i * 32;
    }
    shouldReadAndWrite(data);
  }

  @Test
  public void shouldWriteAndReadWhenDeltaIs0() throws IOException {
    int[] data = new int[2 * blockSize];
    for (int i = 0; i < blockSize * 1; i++) {
      data[i] = i * 32;
    }
    shouldReadAndWrite(data);
  }

  @Test
  public void shouldWriteWhenDeltaIs0ForEachBlock() throws IOException {
    int blockSize = 128;
    int[] data = new int[5 * blockSize];
    for (int i = 0; i < blockSize * 5; i++) {
      data[i] = i / blockSize;
    }
    shouldReadAndWrite(data);
  }

  @Test
  public void shouldReadWriteWhenDataIsNotAlignedWithBlock() throws IOException {
    int[] data = new int[5 * blockSize + 1];
    for (int i = 0; i < data.length; i++) {
      data[i] = i * 32;
    }
    shouldReadAndWrite(data);
  }

  @Test
  public void shouldReturnCorrectOffsetAfterInitialization() throws IOException {
    int[] data = new int[2 * blockSize + 1];
    for (int i = 0; i < data.length; i++) {
      data[i] = i * 32;
    }
    writeData(data);

    reader = new DeltaBinaryPackingValuesReader();
    BytesInput bytes = writer.getBytes();
    byte[] valueContent = bytes.toByteArray();
    byte[] pageContent = new byte[valueContent.length * 10];
    int contentOffsetInPage = 33;
    System.arraycopy(valueContent, 0, pageContent, contentOffsetInPage, valueContent.length);

    //offset should be correct
    int offset = reader.initFromPage(100, pageContent, contentOffsetInPage);
    assertEquals(valueContent.length, offset);
    //should be able to read data correclty
    for (int i : data) {
      assertEquals(i, reader.readInteger());
    }
  }

  @Test
  public void shouldThrowExceptionWhenReadMoreThanWritten() throws IOException {
    int[] data = new int[5 * blockSize + 1];
    for (int i = 0; i < data.length; i++) {
      data[i] = i * 32;
    }
    shouldReadAndWrite(data);
    try {
      reader.readInteger();
    } catch (ParquetDecodingException e) {
      assertEquals("no more value to read, total value count is " + data.length, e.getMessage());
    }

  }

  @Test
  public void readingPerfTest() throws IOException {
    int[] data = new int[1000 * blockSize];
    for (int i = 0; i < data.length; i++) {
      data[i] = i * 3;
    }

    writeData(data);

    for (int i = 0; i < 1000; i++) {
      System.out.print("<");
      long startTime = System.nanoTime();

      reader = new DeltaBinaryPackingValuesReader();
      reader.initFromPage(100, writer.getBytes().toByteArray(), 0);
      for (int j = 0; j < data.length; j++)
        reader.readInteger();

      long endTime = System.nanoTime();
      System.out.println(">time consumed " + (endTime - startTime));
    }
  }

  @Test
  public void writingPerfTest() throws IOException {
    int[] data = new int[1000 * blockSize];
    for (int i = 0; i < data.length; i++) {
      data[i] = i * 3;
    }

//    ValuesWriter writer=new RunLengthBitPackingHybridValuesWriter(32,100);
    double avg = 0.0;
    for (int i = 0; i < 1000; i++) {
      System.out.print("<");
      writer.reset();
      long startTime = System.nanoTime();
      writeData(data);
      long endTime = System.nanoTime();
      long duration = endTime - startTime;
      avg += (double) duration / 1000;

      System.out.println(">time consumed " + duration);
    }

    System.out.println("average value is " + avg);

  }

  @Test
  public void shouldSkip() throws IOException {
    int[] data = new int[5 * blockSize + 1];
    for (int i = 0; i < data.length; i++) {
      data[i] = i * 32;
    }
    writeData(data);
    reader = new DeltaBinaryPackingValuesReader();
    reader.initFromPage(100, writer.getBytes().toByteArray(), 0);
    for (int i = 0; i < data.length; i++) {
      if (i % 3 == 0) {
        reader.skip();
      } else {
        assertEquals(i * 32, reader.readInteger());
      }
    }
  }

  @Test
  public void shouldReset() throws IOException {
    shouldReadWriteWhenDataIsNotAlignedWithBlock();
    int[] data = new int[5 * blockSize];
    for (int i = 0; i < blockSize * 5; i++) {
      data[i] = i * 2;
    }
    writer.reset();
    shouldReadAndWrite(data);
  }

  @Test
  public void randomDataTest() throws IOException {
    int maxSize = 10000;
    int[] data = new int[maxSize];

    Random sizeRandom = new Random();
//    int size= sizeRandom.nextInt(maxSize);
    int size = blockSize * 10;
    Random numberRandom = new Random();
    for (int i = 0; i < size; i++) {
      data[i] = numberRandom.nextInt();
    }
    shouldReadAndWrite(data, size);
  }

  private void shouldReadAndWrite(int[] data) throws IOException {
    shouldReadAndWrite(data, data.length);
  }

  private void shouldReadAndWrite(int[] data, int length) throws IOException {

    writeData(data, length);
    reader = new DeltaBinaryPackingValuesReader();
    byte[] page = writer.getBytes().toByteArray();
    int miniBlockSize = blockSize / miniBlockNum;
    //storage overhead is
//      System.out.println("estimate overhead is " +(1.0/miniBlockSize + (4.0/blockSize)+ ((4.0*miniBlockSize)/length)));
    double estimatedSize = 4 * 4 //blockHeader
            + 4 * length //data
            + ((double) length / miniBlockSize) //bitWidth of mini blocks
            + ((4.0 * length) / blockSize)//min delta for each block
            + 4 * miniBlockSize;
    System.out.println("estimate size is " + estimatedSize);
    System.out.println("page size is " + page.length);
    System.out.printf("avg length of value is %f, data size is %d\n", (double) page.length / length, length);
    reader.initFromPage(100, page, 0);

    for (int i = 0; i < length; i++) {
      assertEquals(data[i], reader.readInteger());
    }
  }

  private void writeData(int[] data) {
    writeData(data, data.length);
  }

  private void writeData(int[] data, int length) {
    for (int i = 0; i < length; i++) {
      writer.writeInteger(data[i]);
    }
  }

}
