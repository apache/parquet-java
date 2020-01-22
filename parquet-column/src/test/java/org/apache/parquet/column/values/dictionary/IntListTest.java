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
package org.apache.parquet.column.values.dictionary;

import org.junit.Test;

import junit.framework.Assert;

public class IntListTest {

  /**
   * Test IntList of fairly small size (< INITIAL_SLAB_SIZE), this tests a single
   * slab being created
   */
  @Test
  public void testSmallList() {
    int testSize = IntList.INITIAL_SLAB_SIZE - 100;
    doTestIntList(testSize, IntList.INITIAL_SLAB_SIZE);
  }

  /**
   * Test IntList > INITIAL_SLAB_SIZE so that we have multiple slabs being created
   */
  @Test
  public void testListGreaterThanInitialSlabSize() {
    int testSize = IntList.INITIAL_SLAB_SIZE + 100;
    doTestIntList(testSize, IntList.INITIAL_SLAB_SIZE * 2);
  }

  /**
   * Test IntList of a fairly large size (> MAX_SLAB_SIZE) so that we have
   * multiple slabs created of varying sizes
   */
  @Test
  public void testListGreaterThanMaxSlabSize() {
    int testSize = IntList.MAX_SLAB_SIZE * 4 + 100;
    doTestIntList(testSize, IntList.MAX_SLAB_SIZE);
  }

  private void doTestIntList(int testSize, int expectedSlabSize) {
    IntList testList = new IntList();
    populateList(testList, testSize);

    verifyIteratorResults(testSize, testList);

    // confirm the size of the current slab
    Assert.assertEquals(expectedSlabSize, testList.getCurrentSlabSize());
  }

  private void populateList(IntList testList, int size) {
    for (int i = 0; i < size; i++) {
      testList.add(i);
    }
  }

  private void verifyIteratorResults(int testSize, IntList testList) {
    IntList.IntIterator iterator = testList.iterator();
    int expected = 0;

    while (iterator.hasNext()) {
      int val = iterator.next();
      Assert.assertEquals(expected, val);
      expected++;
    }

    // ensure we have the correct final value of expected
    Assert.assertEquals(testSize, expected);
  }
}
