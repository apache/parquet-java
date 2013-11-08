package parquet.column.values.delta;

import org.junit.Before;
import org.junit.Test;
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
    new DeltaBinaryPackingValuesWriter(128, 3, 100);
  }

  @Test
  public void shouldWriteWhenDataIsAlignedWithBlock() throws IOException {
    int[] data = new int[5 * blockSize];
    for (int i = 0; i < blockSize * 5; i++) {
      data[i] = i * 32;
    }
    shouldReadAndWrite(data);
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
  public void shoulReadWriteDataSmallerThanABlock() throws IOException {
    int[] data = new int[blockSize - 3];
    for (int i = 0; i < data.length; i++) {
      data[i] = i * 32;
    }
    shouldReadAndWrite(data);
  }

  @Test
  public void shouldReadDataSmallerThanAMiniBlock() throws IOException {
    int miniBlockSize = blockSize / miniBlockNum;
    int[] data = new int[miniBlockSize - 3];
    for (int i = 0; i < data.length; i++) {
      data[i] = i * 32;
    }
    shouldReadAndWrite(data);
  }

  @Test
  public void shouldWriteWhenDataIs0() throws IOException {
    int[] data = new int[6 * blockSize];
    for (int i = 0; i < blockSize * 5; i++) {
      data[i] = i * 32;
    }
    shouldReadAndWrite(data);
  }

  @Test
  public void shouldWriteWhenDeltaIs0ForEachBlock() throws IOException {
    int blockSize = 128;
    DeltaBinaryPackingValuesWriter writer = new DeltaBinaryPackingValuesWriter(blockSize, 4, 100);

    int[] data = new int[5 * blockSize];
    for (int i = 0; i < blockSize * 5; i++) {
      data[i] = i / blockSize;
    }
    shouldReadAndWrite(data);
  }

  @Test
  public void shouldReadWriteWhenDataIsNotAlignedWithBlock() throws IOException {

    int[] data = new int[5 * blockSize + 1];

    for (int i = 0; i < blockSize * 5 + 1; i++) {
      data[i] = i * 32;
    }
    shouldReadAndWrite(data);
  }

  @Test
  public void shouldThrowExceptionWhenReadMoreThanWritten() throws IOException {
    int[] data = new int[5 * blockSize];
    for (int i = 0; i < blockSize * 5; i++) {
      data[i] = i * 32;
    }
    shouldReadAndWrite(data);
    try {
      reader.readInteger();
    } catch (ParquetDecodingException e) {
      assertEquals("no more value to read, total value count is 640", e.getMessage());
    }

  }

  @Test
  public void perfTest() throws IOException {
    int[] data = new int[1000 * blockSize];
    for (int i = 0; i < data.length; i++) {
      data[i] = i * 3;
    }

    for (int i = 0; i < 1000; i++) {
      System.out.print("<");
      long startTime = System.nanoTime();

      shouldReadAndWrite(data);
      writer.reset();
      long endTime = System.nanoTime();
      System.out.println(">time consumed " + (endTime - startTime));
    }
  }

  private void shouldReadAndWrite(int[] data) throws IOException {

    for (int i : data) {
      writer.writeInteger(i);
    }

    reader = new DeltaBinaryPackingValuesReader();
    reader.initFromPage(100, writer.getBytes().toByteArray(), 0);

    for (int i : data) {
      assertEquals(i, reader.readInteger());
    }
  }

  private void generateRandomInteger(int[] data) {
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(100);
    }
  }


}
