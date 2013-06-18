package parquet.column.values.rle;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import parquet.Ints;
import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.io.ParquetEncodingException;

/**
 * @author Alex Levenson
 */
public class RunLengthBitPackingHybridValuesWriter extends ValuesWriter {
  private final RunLengthBitPackingHybridEncoder encoder;
  private final ByteArrayOutputStream length;

  public RunLengthBitPackingHybridValuesWriter(int bitWidth, int initialCapacity) {
    this.encoder = new RunLengthBitPackingHybridEncoder(bitWidth, initialCapacity);
    this.length = new ByteArrayOutputStream(4);
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
      // prepend the length of the column
      BytesInput rle = encoder.toBytes();
      BytesUtils.writeIntLittleEndian(length, Ints.checkedCast(rle.size()));
      return BytesInput.concat(BytesInput.from(length.toByteArray()), rle);
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
    length.reset();
  }

  @Override
  public String memUsageString(String prefix) {
    return String.format("%s RunLengthBitPackingHybrid %d bytes", prefix, getAllocatedSize());
  }
}
