package parquet.column.values.plain;

import static parquet.Log.DEBUG;

import java.io.IOException;

import parquet.Log;
import parquet.bytes.BytesUtils;
import parquet.column.values.ValuesReader;
import parquet.io.ParquetDecodingException;
import parquet.io.api.Binary;

public class BinaryPlainValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(BinaryPlainValuesReader.class);
  private byte[] in;
  private int offset;

  @Override
  public Binary readBytes() {
    try {
      int length = BytesUtils.readIntLittleEndian(in, offset);
      int start = offset + 4;
      offset = start + length;
      return Binary.fromByteArray(in, start, length);
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read bytes at offset " + offset, e);
    }
  }

  @Override
  public int initFromPage(long valueCount, byte[] in, int offset)
      throws IOException {
    if (DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (in.length - offset));
    this.in = in;
    this.offset = offset;
    return in.length;
  }

}
