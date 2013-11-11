package parquet.column.values.deltalengthbytearray;

import static parquet.Log.DEBUG;

import java.io.IOException;

import parquet.Log;
import parquet.column.values.ValuesReader;
import parquet.column.values.plain.PlainValuesReader;
import parquet.io.api.Binary;

public class DeltaLengthByteArrayValuesReader extends ValuesReader {

  private static final Log LOG = Log.getLog(DeltaLengthByteArrayValuesReader.class);
  private ValuesReader lengthReader;
  private byte[] in;
  private int offset;

  public DeltaLengthByteArrayValuesReader() {
    this.lengthReader = new PlainValuesReader.IntegerPlainValuesReader();
  }

  @Override
  public void initFromPage(int valueCount, byte[] in, int offset)
      throws IOException {
    if (DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (in.length - offset));
    lengthReader.initFromPage(valueCount, in, offset);
    offset = lengthReader.getNextOffset();
    this.in = in;
    this.offset = offset;
  }

  @Override
  public Binary readBytes() {
    int length = lengthReader.readInteger();
    int start = offset;
    offset = start + length;
    return Binary.fromByteArray(in, start, length);
  }

  @Override
  public void skip() {
    int length = lengthReader.readInteger();
    offset = offset + length;
  }
}
