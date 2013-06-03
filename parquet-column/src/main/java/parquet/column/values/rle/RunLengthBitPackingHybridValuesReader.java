package parquet.column.values.rle;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import parquet.column.values.ValuesReader;
import parquet.io.ParquetDecodingException;

/**
 * @author Alex Levenson
 */
public class RunLengthBitPackingHybridValuesReader extends ValuesReader {
  private final int bitWidth;
  private RunLengthBitPackingHybridDecoder decoder;

  public RunLengthBitPackingHybridValuesReader(int bitWidth) {
    this.bitWidth = bitWidth;
  }

  @Override
  public int initFromPage(long valueCount, byte[] page, int offset) throws IOException {
    InputStream in = new ByteArrayInputStream(page, offset, page.length - offset);
    decoder = new RunLengthBitPackingHybridDecoder(bitWidth, in);
    return page.length;
  }

  @Override
  public int readInteger() {
    try {
      return decoder.readInt();
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }
}
