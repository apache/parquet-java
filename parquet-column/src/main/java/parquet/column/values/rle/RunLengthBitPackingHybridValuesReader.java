package parquet.column.values.rle;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import parquet.Preconditions;
import parquet.column.values.ValuesReader;

/**
 * This ValuesReader does all the reading in {@link #initFromPage}
 * and stores the values in an in memory buffer, which is less than ideal.
 *
 * @author Alex Levenson
 */
public class RunLengthBitPackingHybridValuesReader extends ValuesReader {
  private final int bitWidth;

  private int[][] values;
  private int currentSlab;
  private int currentValueInSlab;

  public RunLengthBitPackingHybridValuesReader(int bitWidth) {
    this.bitWidth = bitWidth;
  }

  @Override
  public int initFromPage(long valueCount, byte[] page, int offset) throws IOException {
    PosReportingByteArrayInputStream in =
        new PosReportingByteArrayInputStream(page, offset, page.length);

    RunLengthBitPackingHybridDecoder decoder = new RunLengthBitPackingHybridDecoder(bitWidth, in);

    if (valueCount <= 0) {
      // readInteger() will never be called,
      // there is no data to read
      return offset;
    }

    // ceil(valueCount / Integer.MAX_VALUE)
    long numSlabsL = 1 + ((valueCount - 1L) / Integer.MAX_VALUE);

    // safe cast to to int
    // TODO: is this necessary?
    Preconditions.checkArgument(numSlabsL <= Integer.MAX_VALUE,
        String.format("valueCount (%d) is too large", valueCount));
    int numSlabs = (int) numSlabsL;

    // make the slabs array
    values = new int[numSlabs][];

    // all the slabs except the last one are of size Integer.MAX_VALUE
    for (int i = 0; i < numSlabs - 1; i++) {
      values[i] = new int[Integer.MAX_VALUE];
    }

    // create the last slab
    if (valueCount % Integer.MAX_VALUE != 0) {
      // this slab can be smaller
      values[numSlabs - 1] = new int[(int) (valueCount % Integer.MAX_VALUE)];
    } else {
      // it just so happens that the last slab also needs to be
      // Integer.MAX_VALUE
      values[numSlabs - 1] = new int[Integer.MAX_VALUE];
    }

    currentSlab = 0;
    currentValueInSlab = 0;

    // decode all the values
    for (long i = 0; i < valueCount; i++) {
      if (currentValueInSlab > values[currentSlab].length) {
        ++currentSlab;
        currentValueInSlab = 0;
      }
      values[currentSlab][currentValueInSlab] = decoder.readInt();
      ++currentValueInSlab;
    }

    currentSlab = 0;
    currentValueInSlab = 0;

    // in has been keeping track of its position in page
    return in.getPos();
  }

  @Override
  public int readInteger() {
    if (currentValueInSlab > values[currentSlab].length) {
      ++currentSlab;
      currentValueInSlab = 0;
    }
    int ret = values[currentSlab][currentValueInSlab];
    ++currentValueInSlab;
    return ret;
  }

  /**
   * A regular {@link ByteArrayInputStream}
   * that exposes its position in buf
   */
  static class PosReportingByteArrayInputStream extends ByteArrayInputStream {
    public PosReportingByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    public PosReportingByteArrayInputStream(byte[] buf, int offset, int length) {
      super(buf, offset, length);
    }

    public int getPos() {
      return pos;
    }
  }
}
