package parquet.column.values.bitpacking;

import org.junit.Test;

public class TestByteBasedBitPackingEncoder {

  @Test
  public void testSlabBoundary() {
    for (int i = 0; i < 32; i++) {
      final ByteBasedBitPackingEncoder encoder = new ByteBasedBitPackingEncoder(i, Packer.BIG_ENDIAN);
      // make sure to write more than a slab
      for (int j = 0; j < 64 * 1024 * 32 + 10; j++) {
        try {
          encoder.writeInt(j);
        } catch (Exception e) {
          throw new RuntimeException(i + ": error writing " + j, e);
        }
      }
    }
  }

}
