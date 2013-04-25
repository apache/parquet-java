package parquet.column.values.bitpacking;

import java.io.IOException;
import java.util.Arrays;

import parquet.Log;
import parquet.bytes.BytesUtils;
import parquet.column.values.ValuesReader;

public class ByteBitPackingValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(ByteBitPackingValuesReader.class);

  private final int bitWidth;
  private final BytePacker packer;
  private final int[] decoded = new int[8];
  private int decodedPosition = 7;
  private byte[] encoded;
  private int encodedPos;

  public ByteBitPackingValuesReader(int bound) {
    this.bitWidth = BytesUtils.getWidthFromMaxInt(bound);
    this.packer = ByteBitPacking.getPacker(bitWidth);
  }

  @Override
  public int readInteger() {
    ++ decodedPosition;
    if (decodedPosition == decoded.length) {
      if (encodedPos + bitWidth > encoded.length) {
        System.out.println(encodedPos + " + " + bitWidth + " > " + encoded.length);
        packer.unpack8Values(Arrays.copyOfRange(encoded, encodedPos, encodedPos + bitWidth), 0, decoded, 0);
      } else {
        packer.unpack8Values(encoded, encodedPos, decoded, 0);
      }
      encodedPos += bitWidth;
      decodedPosition = 0;
    }
    return decoded[decodedPosition];
  }

  @Override
  public int initFromPage(long valueCount, byte[] page, int offset)
      throws IOException {
    // TODO: int vs long
    int effectiveBitLength = (int)valueCount * bitWidth;
    int length = BytesUtils.paddedByteCountFromBits(effectiveBitLength); // ceil
    if (Log.DEBUG) LOG.debug("reading " + length + " bytes for " + valueCount + " values of size " + bitWidth + " bits." );
    this.encoded = page;
    this.encodedPos = offset;
    this.decodedPosition = 7;
    return offset + length;
  }

}
