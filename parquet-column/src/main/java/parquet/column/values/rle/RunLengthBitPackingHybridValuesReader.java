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

  private int[] values;
  private int valueIndex;

  public RunLengthBitPackingHybridValuesReader(int bitWidth) {
    this.bitWidth = bitWidth;
  }

  @Override
  public int initFromPage(long valueCountL, byte[] page, int offset) throws IOException {
    // TODO: we are assuming valueCount < Integer.MAX_VALUE
    //       we should address this here and elsewhere
    Preconditions.checkArgument(valueCountL < Integer.MAX_VALUE,
      String.format("valueCount (%d) cannot be greater than Integer.MAX_VALUE", valueCountL));

    int valueCount = (int) valueCountL;

    PosReportingByteArrayInputStream in =
        new PosReportingByteArrayInputStream(page, offset, page.length);

    RunLengthBitPackingHybridDecoder decoder = new RunLengthBitPackingHybridDecoder(bitWidth, in);

    if (valueCount <= 0) {
      // readInteger() will never be called,
      // there is no data to read
      return offset;
    }

    values = new int[valueCount];

    // decode all the values
    for (int i = 0; i < valueCount; i++) {
      values[i] = decoder.readInt();
    }

    valueIndex = 0;
    // in has been keeping track of its position in page
    return in.getPos();
  }

  @Override
  public int readInteger() {
    int ret = values[valueIndex];
    valueIndex++;
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
