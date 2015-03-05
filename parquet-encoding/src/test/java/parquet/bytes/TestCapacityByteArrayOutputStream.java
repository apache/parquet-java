/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package parquet.bytes;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

public class TestCapacityByteArrayOutputStream {

  @Test
  public void testWrite() throws Throwable {
    CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(10);
    final int expectedSize = 54;
    for (int i = 0; i < expectedSize; i++) {
      capacityByteArrayOutputStream.write(i);
      assertEquals(i + 1, capacityByteArrayOutputStream.size());
    }
    validate(capacityByteArrayOutputStream, expectedSize);
  }

  @Test
  public void testWriteArray() throws Throwable {
    CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(10);
    int v = 23;
    writeArraysOf3(capacityByteArrayOutputStream, v);
    validate(capacityByteArrayOutputStream, v * 3);
  }

  @Test
  public void testWriteArrayAndInt() throws Throwable {
    CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(10);
    for (int i = 0; i < 23; i++) {
      byte[] toWrite = { (byte)(i * 3), (byte)(i * 3 + 1)};
      capacityByteArrayOutputStream.write(toWrite);
      capacityByteArrayOutputStream.write((byte)(i * 3 + 2));
      assertEquals((i + 1) * 3, capacityByteArrayOutputStream.size());
    }
    validate(capacityByteArrayOutputStream, 23 * 3);

  }

  protected CapacityByteArrayOutputStream newCapacityBAOS(int initialSize) {
    return new CapacityByteArrayOutputStream(10, 1000000);
  }

  @Test
  public void testReset() throws Throwable {
    CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(10);
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

  @Test
  public void testWriteArrayBiggerThanSlab() throws Throwable {
    CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(10);
    int v = 23;
    writeArraysOf3(capacityByteArrayOutputStream, v);
    int n = v * 3;
    byte[] toWrite = { // bigger than 2 slabs of size of 10
        (byte)n, (byte)(n + 1), (byte)(n + 2), (byte)(n + 3), (byte)(n + 4), (byte)(n + 5),
        (byte)(n + 6), (byte)(n + 7), (byte)(n + 8), (byte)(n + 9), (byte)(n + 10),
        (byte)(n + 11), (byte)(n + 12), (byte)(n + 13), (byte)(n + 14), (byte)(n + 15),
        (byte)(n + 16), (byte)(n + 17), (byte)(n + 18), (byte)(n + 19), (byte)(n + 20)};
    capacityByteArrayOutputStream.write(toWrite);
    n = n + toWrite.length;
    assertEquals(n, capacityByteArrayOutputStream.size());
    validate(capacityByteArrayOutputStream, n);
    capacityByteArrayOutputStream.reset();
    // check it works after reset too
    capacityByteArrayOutputStream.write(toWrite);
    assertEquals(toWrite.length, capacityByteArrayOutputStream.size());
    byte[] byteArray = BytesInput.from(capacityByteArrayOutputStream).toByteArray();
    assertEquals(toWrite.length, byteArray.length);
    for (int i = 0; i < toWrite.length; i++) {
      assertEquals(toWrite[i], byteArray[i]);
    }
  }

  @Test
  public void testWriteArrayManySlabs() throws Throwable {
    CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(10);
    int it = 500;
    int v = 23;
    for (int j = 0; j < it; j++) {
      for (int i = 0; i < v; i++) {
        byte[] toWrite = { (byte)(i * 3), (byte)(i * 3 + 1), (byte)(i * 3 + 2)};
        capacityByteArrayOutputStream.write(toWrite);
        assertEquals((i + 1) * 3 + v * 3 * j, capacityByteArrayOutputStream.size());
      }
    }
    byte[] byteArray = BytesInput.from(capacityByteArrayOutputStream).toByteArray();
    assertEquals(v * 3 * it, byteArray.length);
    for (int i = 0; i < v * 3 * it; i++) {
      assertEquals(i % (v * 3), byteArray[i]);
    }
    // verifying we have not created 500 * 23 / 10 slabs
    assertTrue("slab count: " + capacityByteArrayOutputStream.getSlabCount(),capacityByteArrayOutputStream.getSlabCount() <= 20);
    capacityByteArrayOutputStream.reset();
    writeArraysOf3(capacityByteArrayOutputStream, v);
    validate(capacityByteArrayOutputStream, v * 3);
    // verifying we use less slabs now
    assertTrue("slab count: " + capacityByteArrayOutputStream.getSlabCount(),capacityByteArrayOutputStream.getSlabCount() <= 2);
  }

  @Test
  public void testReplaceByte() throws Throwable {
    // test replace the first value
    {
      CapacityByteArrayOutputStream cbaos = newCapacityBAOS(5);
      cbaos.write(10);
      assertEquals(0, cbaos.getCurrentIndex());
      cbaos.setByte(0, (byte) 7);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      cbaos.writeTo(baos);
      assertEquals(7, baos.toByteArray()[0]);
    }

    // test replace value in the first slab
    {
      CapacityByteArrayOutputStream cbaos = newCapacityBAOS(5);
      cbaos.write(10);
      cbaos.write(13);
      cbaos.write(15);
      cbaos.write(17);
      assertEquals(3, cbaos.getCurrentIndex());
      cbaos.write(19);
      cbaos.setByte(3, (byte) 7);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      cbaos.writeTo(baos);
      assertArrayEquals(new byte[]{10, 13, 15, 7, 19}, baos.toByteArray());
    }

    // test replace in *not* the first slab
    {
      CapacityByteArrayOutputStream cbaos = newCapacityBAOS(5);

      // advance part way through the 3rd slab
      for (int i = 0; i < 12; i++) {
        cbaos.write(100 + i);
      }
      assertEquals(11, cbaos.getCurrentIndex());

      cbaos.setByte(6, (byte) 7);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      cbaos.writeTo(baos);
      assertArrayEquals(
        new byte[]{100, 101, 102, 103, 104, 105, 7, 107, 108, 109, 110, 111},
        baos.toByteArray());
    }

    // test replace last value of a slab
    {
      CapacityByteArrayOutputStream cbaos = newCapacityBAOS(5);

      // advance part way through the 3rd slab
      for (int i = 0; i < 12; i++) {
        cbaos.write(100 + i);
      }
      assertEquals(11, cbaos.getCurrentIndex());

      cbaos.setByte(9, (byte) 7);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      cbaos.writeTo(baos);
      assertArrayEquals(
        new byte[]{100, 101, 102, 103, 104, 105, 106, 107, 108, 7, 110, 111},
        baos.toByteArray());
    }

    // test replace last value
    {
      CapacityByteArrayOutputStream cbaos = newCapacityBAOS(5);

      // advance part way through the 3rd slab
      for (int i = 0; i < 12; i++) {
        cbaos.write(100 + i);
      }
      assertEquals(11, cbaos.getCurrentIndex());

      cbaos.setByte(11, (byte) 7);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      cbaos.writeTo(baos);
      assertArrayEquals(
        new byte[]{100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 7},
        baos.toByteArray());
    }

  }

  private void writeArraysOf3(CapacityByteArrayOutputStream capacityByteArrayOutputStream, int n)
      throws IOException {
    for (int i = 0; i < n; i++) {
      byte[] toWrite = { (byte)(i * 3), (byte)(i * 3 + 1), (byte)(i * 3 + 2)};
      capacityByteArrayOutputStream.write(toWrite);
      assertEquals((i + 1) * 3, capacityByteArrayOutputStream.size());
    }
  }

  private void validate(
      CapacityByteArrayOutputStream capacityByteArrayOutputStream,
      final int expectedSize) throws IOException {
    final byte[] byteArray = BytesInput.from(capacityByteArrayOutputStream).toByteArray();
    assertEquals(expectedSize, byteArray.length);
    for (int i = 0; i < expectedSize; i++) {
      assertEquals(i, byteArray[i]);
    }
  }
}
