package redelm.column.primitive;

import static redelm.bytes.BytesUtils.getWidthFromMaxInt;
import static redelm.column.primitive.BitPacking.getBitPackingReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import redelm.column.primitive.BitPacking.BitPackingReader;

public class BitPackingColumnReader extends PrimitiveColumnReader {

  private ByteArrayInputStream in;
  private BitPackingReader bitPackingReader;
  private final int bitsPerValue;

  public BitPackingColumnReader(int bound) {
    this.bitsPerValue = getWidthFromMaxInt(bound);
  }

  public int readInteger() {
    try {
      return bitPackingReader.read();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int initFromPage(long valueCount, byte[] in, int offset) throws IOException {
    // TODO: int vs long
    int effectiveBitLength = (int)valueCount * bitsPerValue;
    int length = effectiveBitLength / 8 + (effectiveBitLength % 8 == 0 ? 0 : 1); // ceil
    this.in = new ByteArrayInputStream(in, offset, length);
    this.bitPackingReader = getBitPackingReader(bitsPerValue, this.in, valueCount);
    return offset + length;
  }

}
