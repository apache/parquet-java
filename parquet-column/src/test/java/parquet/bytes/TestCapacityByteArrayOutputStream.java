package parquet.bytes;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;

public class TestCapacityByteArrayOutputStream {

  @Test
  public void testWrite() throws Throwable {
    CapacityByteArrayOutputStream capacityByteArrayOutputStream = new CapacityByteArrayOutputStream(10);
    for (int i = 0; i < 54; i++) {
      capacityByteArrayOutputStream.write(i);
      assertEquals(i + 1, capacityByteArrayOutputStream.size());
    }
    final byte[] byteArray = BytesInput.from(capacityByteArrayOutputStream).toByteArray();
    assertEquals(54, byteArray.length);
    for (int i = 0; i < 54; i++) {
      assertEquals(i, byteArray[i]);
    }

  }

  @Test
  public void testWriteArray() throws Throwable {
    CapacityByteArrayOutputStream capacityByteArrayOutputStream = new CapacityByteArrayOutputStream(10);
    for (int i = 0; i < 23; i++) {
      byte[] toWrite = { (byte)(i * 3), (byte)(i * 3 + 1), (byte)(i * 3 + 2)};
      capacityByteArrayOutputStream.write(toWrite);
      assertEquals((i + 1) * 3, capacityByteArrayOutputStream.size());
    }
    final byte[] byteArray = BytesInput.from(capacityByteArrayOutputStream).toByteArray();
    assertEquals(23 * 3, byteArray.length);
    for (int i = 0; i < 23 * 3; i++) {
      assertEquals(i, byteArray[i]);
    }

  }

  @Test
  public void testWriteArrayAndInt() throws Throwable {
    CapacityByteArrayOutputStream capacityByteArrayOutputStream = new CapacityByteArrayOutputStream(10);
    for (int i = 0; i < 23; i++) {
      byte[] toWrite = { (byte)(i * 3), (byte)(i * 3 + 1)};
      capacityByteArrayOutputStream.write(toWrite);
      capacityByteArrayOutputStream.write((byte)(i * 3 + 2));
      assertEquals((i + 1) * 3, capacityByteArrayOutputStream.size());
    }
    final byte[] byteArray = BytesInput.from(capacityByteArrayOutputStream).toByteArray();
    assertEquals(23 * 3, byteArray.length);
    for (int i = 0; i < 23 * 3; i++) {
      assertEquals(i, byteArray[i]);
    }

  }

  @Test
  public void testReset() throws Throwable {
    CapacityByteArrayOutputStream capacityByteArrayOutputStream = new CapacityByteArrayOutputStream(10);
    for (int i = 0; i < 54; i++) {
      capacityByteArrayOutputStream.write(i);
      assertEquals(i + 1, capacityByteArrayOutputStream.size());
    }
    capacityByteArrayOutputStream.reset();
    for (int i = 0; i < 54; i++) {
      capacityByteArrayOutputStream.write(54 + i);
      assertEquals(i + 1, capacityByteArrayOutputStream.size());
    }
    final byte[] byteArray = BytesInput.from(capacityByteArrayOutputStream).toByteArray();
    assertEquals(54, byteArray.length);
    for (int i = 0; i < 54; i++) {
      assertEquals(i + " in " + Arrays.toString(byteArray) ,54 + i, byteArray[i]);
    }

  }
}
