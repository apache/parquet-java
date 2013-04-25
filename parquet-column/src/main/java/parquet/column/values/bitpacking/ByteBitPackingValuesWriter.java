package parquet.column.values.bitpacking;

import static parquet.column.Encoding.BIT_PACKED;

import java.io.IOException;

import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.io.ParquetEncodingException;

public class ByteBitPackingValuesWriter extends ValuesWriter {

  private ByteBasedBitPackingEncoder encoder;
  private final int bitWidth;

  public ByteBitPackingValuesWriter(int bound) {
    this.bitWidth = BytesUtils.getWidthFromMaxInt(bound);
    this.encoder = new ByteBasedBitPackingEncoder(bitWidth);
  }

  @Override
  public void writeInteger(int v) {
    try {
      this.encoder.writeInt(v);
    } catch (IOException e) {
      throw new ParquetEncodingException(e);
    }
  }

  @Override
  public Encoding getEncoding() {
    return BIT_PACKED;
  }

  @Override
  public BytesInput getBytes() {
    try {
      return encoder.toBytes();
    } catch (IOException e) {
      throw new ParquetEncodingException(e);
    }
  }

  @Override
  public void reset() {
    encoder = new ByteBasedBitPackingEncoder(bitWidth);
  }

  @Override
  public long getBufferedSize() {
    return encoder.getBufferSize();
  }

  @Override
  public long getAllocatedSize() {
    return encoder.getAllocatedSize();
  }

}
