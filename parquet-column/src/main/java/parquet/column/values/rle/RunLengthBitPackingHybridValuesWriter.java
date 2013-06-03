package parquet.column.values.rle;

import java.io.IOException;

import parquet.bytes.BytesInput;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.io.ParquetEncodingException;

/**
 * @author Alex Levenson
 */
public class RunLengthBitPackingHybridValuesWriter extends ValuesWriter {
  private final RunLengthBitPackingHybridEncoder encoder;

  public RunLengthBitPackingHybridValuesWriter(int bitWidth, int initialCapacity) {
    this.encoder = new RunLengthBitPackingHybridEncoder(bitWidth, initialCapacity);
  }

  @Override
  public void writeInteger(int v) {
    try {
      encoder.writeInt(v);
    } catch (IOException e) {
      throw new ParquetEncodingException(e);
    }
  }

  @Override
  public long getBufferedSize() {
    return encoder.getBufferedSize();
  }

  @Override
  public long getAllocatedSize() {
    return encoder.getAllocatedSize();
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
  public Encoding getEncoding() {
    return Encoding.RLE;
  }

  @Override
  public void reset() {
    encoder.reset();
  }

  @Override
  public String memUsageString(String prefix) {
    return String.format("%s RunLengthBitPackingHybrid %d bytes", prefix, getAllocatedSize());
  }
}
