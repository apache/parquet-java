package parquet.column.values.plain;

import parquet.io.api.Int96;

public class Int96PlainValuesReader extends FixedLenByteArrayPlainValuesReader {
  public Int96PlainValuesReader() {
    super(12);
  }

  @Override
  public Int96 readInt96() {
    return Int96.fromByteBuffer(readByteBuffer());
  }
}
