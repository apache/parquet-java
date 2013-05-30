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
      encoder.writeInt(i % 2);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = ((104/8) << 1) | 1 = 27
    assertEquals(27, BytesUtils.readUnsignedVarInt(is));

    BytePacker packer = ByteBitPacking.getPacker(3);
    int[] unpacked = new int[8];
    byte[] next8Values = new byte[3];

    List<Integer> values = new ArrayList<Integer>(104);

    int valueCounter = 0;

    while(values.size() < 104) {
      for (int i = 0; i < 3; i++) {
        next8Values[i] = (byte) is.read();
      }

      packer.unpack8Values(next8Values, 0, unpacked, 0);

      for (int v = 0; v < 8; v++) {
        values.add(unpacked[v]);
      }
    }

    for (int i = 0; i < 100; i++) {
      assertEquals(i % 2, (int) values.get(i));
    }

  }

}
