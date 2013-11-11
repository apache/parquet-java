package parquet.column.values.deltastrings;

import java.io.IOException;

import parquet.column.values.ValuesReader;
import parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesReader;
import parquet.column.values.plain.PlainValuesReader;
import parquet.io.api.Binary;

public class DeltaStringValuesReader extends ValuesReader {

  private ValuesReader prefixLengthReader;
  private ValuesReader suffixReader;

  private byte[] previous = new byte[0];

  public DeltaStringValuesReader() {
    this.prefixLengthReader = new PlainValuesReader.IntegerPlainValuesReader();
    this.suffixReader = new DeltaLengthByteArrayValuesReader();
    this.previous = new byte[0];
  }

  @Override
  public void initFromPage(int valueCount, byte[] page, int offset)
      throws IOException {
    prefixLengthReader.initFromPage(valueCount, page, offset);
    int next = prefixLengthReader.getNextOffset();
    suffixReader.initFromPage(valueCount, page, next);	
  }

  @Override
  public void skip() {
    prefixLengthReader.skip();
    suffixReader.skip();
  }

  @Override
  public Binary readBytes() {
    int prefixLength = prefixLengthReader.readInteger();
    Binary suffix = suffixReader.readBytes();
    int length = prefixLength + suffix.length();
    byte[] out = new byte[length];
    System.arraycopy(previous, 0, out, 0, prefixLength);
    System.arraycopy(suffix.getBytes(), 0, out, prefixLength, suffix.length());
    previous = out;
    return Binary.fromByteArray(out);
  }
}
