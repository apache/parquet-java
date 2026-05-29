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
package org.apache.parquet.bytes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.lang.reflect.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for integer overflow handling in {@link CapacityByteArrayOutputStream#addSlab(int)}.
 * Verifies the fix for PARQUET-3261.
 */
public class TestCapacityByteArrayOutputStreamOverflow {

  private TrackingByteBufferAllocator allocator;

  @Before
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
  }

  @After
  public void closeAllocator() {
    allocator.close();
  }

  /**
   * Regression test for PARQUET-3261: bytesAllocated overflow in addSlab().
   * Simulates near-overflow by setting bytesAllocated via reflection, then verifying
   * that addSlab caps the slab size instead of throwing ArithmeticException.
   */
  @Test
  public void testAddSlabCapsSlabSizeNearIntegerMaxValue() throws Exception {
    int slabSize = 1024;
    try (CapacityByteArrayOutputStream cbaos =
        new CapacityByteArrayOutputStream(slabSize, Integer.MAX_VALUE, allocator)) {
      // Write initial data to set up internal state
      byte[] data = new byte[slabSize];
      cbaos.write(data, 0, data.length);

      // Simulate near-overflow by setting bytesAllocated close to Integer.MAX_VALUE
      Field bytesAllocatedField = CapacityByteArrayOutputStream.class.getDeclaredField("bytesAllocated");
      bytesAllocatedField.setAccessible(true);
      bytesAllocatedField.setInt(cbaos, Integer.MAX_VALUE - 100);

      // Writing 1 byte triggers addSlab with minimumSize=1.
      // Without the fix, the doubling strategy would compute nextSlabSize = bytesUsed (1024),
      // and bytesAllocated + 1024 would overflow. With the fix, nextSlabSize is capped to 100.
      cbaos.write(1);
      assertEquals(slabSize + 1, cbaos.size());
    }
  }

  /**
   * Verify that a true overflow (bytesAllocated + minimumSize > Integer.MAX_VALUE)
   * still throws OutOfMemoryError.
   */
  @Test
  public void testAddSlabThrowsOOMOnTrueOverflow() throws Exception {
    int slabSize = 1024;
    try (CapacityByteArrayOutputStream cbaos =
        new CapacityByteArrayOutputStream(slabSize, Integer.MAX_VALUE, allocator)) {
      byte[] data = new byte[slabSize];
      cbaos.write(data, 0, data.length);

      // Set bytesAllocated so that even minimumSize=200 would overflow
      Field bytesAllocatedField = CapacityByteArrayOutputStream.class.getDeclaredField("bytesAllocated");
      bytesAllocatedField.setAccessible(true);
      bytesAllocatedField.setInt(cbaos, Integer.MAX_VALUE - 50);

      // Writing 200 bytes requires minimumSize=200, but only 50 bytes remain.
      // The addExact(bytesAllocated, minimumSize) check should throw OOM.
      byte[] tooLarge = new byte[200];
      assertThrows(OutOfMemoryError.class, () -> cbaos.write(tooLarge, 0, tooLarge.length));
    }
  }
}
