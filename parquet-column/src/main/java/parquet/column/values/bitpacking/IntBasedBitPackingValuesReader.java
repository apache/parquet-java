package parquet.column.values.bitpacking;

import static parquet.bytes.BytesUtils.getWidthFromMaxInt;

import java.io.IOException;
import java.nio.ByteBuffer;

import parquet.Log;
import parquet.column.values.ValuesReader;

public class IntBasedBitPackingValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(IntBasedBitPackingValuesReader.class);

  private final int bitsPerValue;
  private final IntPacker packer;
  private final int[] decoded = new int[32];
  private int decodedSize;
  private int[] encoded;
  private int encodedPosition = 0;

  /**
   * @param bound the maximum value stored by this column
   */
  public IntBasedBitPackingValuesReader(int bound) {
    this.bitsPerValue = getWidthFromMaxInt(bound);
    packer = LemireBitPackingBE.getPacker(bitsPerValue);
    decode();
  }

  private void decode() {
    decodedSize = 32;
    packer.unpack32Values(encoded, 0, decoded, encodedPosition);
    encodedPosition += bitsPerValue;
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.values.ValuesReader#readInteger()
   */
  @Override
  public int readInteger() {
    if (decodedSize == 0) {
      decode();
    }
    -- decodedSize;
    return decoded[decodedSize];
  }

  @Override
  public int initFromPage(long valueCount, byte[] page, int offset)
      throws IOException {
    // TODO: int vs long
    int effectiveBitLength = (int)valueCount * bitsPerValue;
    // TODO: maybe ((effectiveBitLength - 1) / 8 + 1) here? has fewer conditionals and divides
    int length = effectiveBitLength / 8 + (effectiveBitLength % 8 == 0 ? 0 : 1); // ceil
    int intLength = (int)(valueCount + 63) / 64 * bitsPerValue; // We write bitsPerValue ints for every 64 values (rounded up to the next 64th value)
    this.encoded = new int[intLength];
    ByteBuffer.wrap(page, offset, (length + 3) / 4).asIntBuffer().get(encoded);
    if (Log.DEBUG) LOG.debug("reading " + length + " bytes for " + valueCount + " values of size " + bitsPerValue + " bits." );
    return offset + length;
  }

}
