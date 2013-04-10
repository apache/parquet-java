package parquet.column.values.rle;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.column.values.bitpacking.BitPacking;

/**
 * Simple RLE Encoder that does only bitpacking
 *
 * @author Julien Le Dem
 *
 */
public class RLESimpleEncoder {

  private final int bitWidth;
  private final BitPacking.BitPackingWriter out;
  private final ByteArrayOutputStream buffer;

  public RLESimpleEncoder(int bitWidth) {
    this.bitWidth = bitWidth;
    buffer = new ByteArrayOutputStream(64 * 1024);
    out = BitPacking.getBitPackingWriter(bitWidth, buffer);
  }

  public void writeInt(int value) throws IOException {
    out.write(value);
  }

  public BytesInput toBytes() throws IOException {
    out.finish();
    final BytesInput bytes = BytesInput.from(buffer);
    ByteArrayOutputStream size = new ByteArrayOutputStream(4);
    int header = (int)(bytes.size()/bitWidth) << 1 | 1; // TODO: fix int vs long
    BytesUtils.writeUnsignedVarInt(header, size);
    return BytesInput.fromSequence(BytesInput.from(size), bytes);
  }

}
