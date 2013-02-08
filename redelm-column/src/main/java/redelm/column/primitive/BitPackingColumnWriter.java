package redelm.column.primitive;

import static redelm.bytes.BytesUtils.getWidthFromMaxInt;
import static redelm.column.primitive.BitPacking.getBitPackingWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import redelm.bytes.BytesInput;
import redelm.column.primitive.BitPacking.BitPackingWriter;

public class BitPackingColumnWriter extends PrimitiveColumnWriter {

  private ByteArrayOutputStream out;
  private BitPackingWriter bitPackingWriter;
  private int bitsPerValue;

  public BitPackingColumnWriter(int bound) {
    this.bitsPerValue = getWidthFromMaxInt(bound);
    this.out = new ByteArrayOutputStream();
    init();
  }

  private void init() {
    this.bitPackingWriter = getBitPackingWriter(bitsPerValue, out);
  }

  public void writeInteger(int v) {
    try {
      bitPackingWriter.write(v);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long getMemSize() {
    return out.size();
  }

  @Override
  public BytesInput getBytes() {
    try {
      this.bitPackingWriter.finish();
      return BytesInput.from(out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void reset() {
    out.reset();
    init();
  }


}
