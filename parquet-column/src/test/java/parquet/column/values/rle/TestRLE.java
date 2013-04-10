package parquet.column.values.rle;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class TestRLE {

  @Test
  public void testOne() throws IOException {
    int[] in = {0,0,1,0,1,1,1,0,1,0,1,1,1};
    verify(in, 1);
  }

  @Test
  public void testTwo() throws IOException {
    int[] in = {0,0,1,0,2,2,1,0,3,0,1,1,1};
    verify(in, 2);
  }

  private void verify(int[] in, int width) throws IOException {
    final RLESimpleEncoder rleSimpleEncoder = new RLESimpleEncoder(width);
    for (int i : in) {
      rleSimpleEncoder.writeInt(i);
    }
    final RLEDecoder rleDecoder = new RLEDecoder(width, new ByteArrayInputStream(rleSimpleEncoder.toBytes().toByteArray()));
    for (int i = 0; i < in.length; i++) {
      Assert.assertEquals(in[i], rleDecoder.readInt());
    }
  }
}
