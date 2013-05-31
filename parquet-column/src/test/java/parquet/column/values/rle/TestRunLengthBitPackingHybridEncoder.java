package parquet.column.values.rle;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import parquet.bytes.BytesUtils;
import parquet.column.values.bitpacking.ByteBitPacking;
import parquet.column.values.bitpacking.BytePacker;

import static org.junit.Assert.assertEquals;

/**
 * @author Alex Levenson
 */
public class TestRunLengthBitPackingHybridEncoder {

  @Test
  public void testRLEOnly() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(3, 5);
    for (int i = 0; i < 100; i++) {
      encoder.writeInt(4);
    }
    for (int i = 0; i < 100; i++) {
      encoder.writeInt(5);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = 100 << 1 = 200
    assertEquals(200, BytesUtils.readUnsignedVarInt(is));
    // payload = 4
    assertEquals(4, BytesUtils.readIntLittleEndianOnOneByte(is));

    // header = 100 << 1 = 200
    assertEquals(200, BytesUtils.readUnsignedVarInt(is));
    // payload = 5
    assertEquals(5, BytesUtils.readIntLittleEndianOnOneByte(is));

    // end of stream
    assertEquals(-1, is.read());
  }

  @Test
  public void testBitPackingOnly() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(3, 5);

    for (int i = 0; i < 100; i++) {
      encoder.writeInt(i % 3);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = ((104/8) << 1) | 1 = 27
    assertEquals(27, BytesUtils.readUnsignedVarInt(is));

    List<Integer> values = unpack(3, 104, is);

    for (int i = 0; i < 100; i++) {
      assertEquals(i % 3, (int) values.get(i));
    }
  }

  private List<Integer> unpack(int bitWidth, int numValues, ByteArrayInputStream is)
      throws Exception {

    BytePacker packer = ByteBitPacking.getPacker(bitWidth);
    int[] unpacked = new int[8];
    byte[] next8Values = new byte[bitWidth];

    List<Integer> values = new ArrayList<Integer>(numValues);

    while(values.size() < numValues) {
      for (int i = 0; i < bitWidth; i++) {
        next8Values[i] = (byte) is.read();
      }

      packer.unpack8Values(next8Values, 0, unpacked, 0);

      for (int v = 0; v < 8; v++) {
        values.add(unpacked[v]);
      }
    }

    return values;
  }

  @Test
  public void testBitPackingOverflow() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(3, 5);

    for (int i = 0; i < 1000; i++) {
      encoder.writeInt(i % 3);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // 504 is the max number of values in a bit packed run
    // that still has a header of 1 byte
    // header = ((504/8) << 1) | 1 = 127
    assertEquals(127, BytesUtils.readUnsignedVarInt(is));
    List<Integer> values = unpack(3, 504, is);

    for (int i = 0; i < 504; i++) {
      assertEquals(i % 3, (int) values.get(i));
    }

    // there should now be 496 values in another bit-packed run
    // header = ((496/8) << 1) | 1 = 125
    assertEquals(125, BytesUtils.readUnsignedVarInt(is));
    values = unpack(3, 496, is);
    for (int i = 0; i < 496; i++) {
      assertEquals((i + 504) % 3, (int) values.get(i));
    }
  }

}
