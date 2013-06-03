package parquet.column.values.rle;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Alex Levenson
 */
public class RunLengthBitPackingHybridIntegrationTest {

  @Test
  public void integrationTest() throws Exception {
    for (int i = 0; i <= 32; i++) {
      doIntegrationTest(i);
    }
  }

  private void doIntegrationTest(int bitWidth) throws Exception {
    long modValue = 1L << bitWidth;

    RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(bitWidth, 1000);

    for (int i = 0; i < 100; i++) {
      encoder.writeInt((int) (i % modValue));
    }

    for (int i = 0; i < 100; i++) {
      encoder.writeInt((int) (77 % modValue));
    }

    for (int i = 0; i < 100; i++) {
      encoder.writeInt((int) (88 % modValue));
    }

    for (int i = 0; i < 1000; i++) {
      encoder.writeInt((int) (i % modValue));
      encoder.writeInt((int) (i % modValue));
      encoder.writeInt((int) (i % modValue));
    }

    for (int i = 0; i < 1000; i++) {
      encoder.writeInt((int) (17 % modValue));
    }

    InputStream in = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    RunLengthBitPackingHybridDecoder decoder = new RunLengthBitPackingHybridDecoder(bitWidth, in);

    for (int i = 0; i < 100; i++) {
      assertEquals(i % modValue, decoder.readInt());
    }

    for (int i = 0; i < 100; i++) {
      assertEquals(77 % modValue, decoder.readInt());
    }

    for (int i = 0; i < 100; i++) {
      assertEquals(88 % modValue, decoder.readInt());
    }

    for (int i = 0; i < 1000; i++) {
      assertEquals(i % modValue, decoder.readInt());
      assertEquals(i % modValue, decoder.readInt());
      assertEquals(i % modValue, decoder.readInt());
    }

    for (int i = 0; i < 1000; i++) {
      assertEquals(17 % modValue, decoder.readInt());
    }
  }
}
