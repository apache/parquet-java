package parquet.column.values.rle;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import parquet.Ints;
import parquet.Preconditions;
import parquet.bytes.BytesUtils;
import parquet.column.values.ValuesReader;
import parquet.io.ParquetDecodingException;

/**
 * This ValuesReader does all the reading in {@link #initFromPage}
 * and stores the values in an in memory buffer, which is less than ideal.
 *
 * @author Alex Levenson
 */
public class RunLengthBitPackingHybridValuesReader extends ValuesReader {
  private final int bitWidth;
  private RunLengthBitPackingHybridDecoder decoder;

  public RunLengthBitPackingHybridValuesReader(int bitWidth) {
    this.bitWidth = bitWidth;
  }

  @Override
  public int initFromPage(long valueCountL, byte[] page, int offset) throws IOException {
    // TODO: we are assuming valueCount < Integer.MAX_VALUE
    //       we should address this here and elsewhere
    int valueCount = Ints.checkedCast(valueCountL);

    if (valueCount <= 0) {
      // readInteger() will never be called,
      // there is no data to read
      return offset;
    }

    ByteArrayInputStream in = new ByteArrayInputStream(page, offset, page.length);
    int length = BytesUtils.readIntLittleEndian(in);

    decoder = new RunLengthBitPackingHybridDecoder(bitWidth, in);

    // 4 is for the length which is stored as 4 bytes little endian
    return offset + length + 4;
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
