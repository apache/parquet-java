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
package org.apache.parquet.internal.column.columnindex;

import static org.junit.Assert.assertArrayEquals;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.Arrays;
import java.util.PrimitiveIterator;
import org.junit.Test;

/**
 * Unit test for {@link IndexIterator}.
 */
public class TestIndexIterator {
  @Test
  public void testAll() {
    assertEquals(IndexIterator.all(10), 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  public void testFilter() {
    assertEquals(IndexIterator.filter(30, value -> value % 3 == 0), 0, 3, 6, 9, 12, 15, 18, 21, 24, 27);
  }

  @Test
  public void testFilterTranslate() {
    assertEquals(IndexIterator.filterTranslate(20, value -> value < 5, Math::negateExact), 0, -1, -2, -3, -4);
  }

  @Test
  public void testRangeTranslate() {
    assertEquals(IndexIterator.rangeTranslate(11, 18, i -> i - 10), 1, 2, 3, 4, 5, 6, 7, 8);
  }

  static void assertEquals(PrimitiveIterator.OfInt actualIt, int... expectedValues) {
    IntList actualList = new IntArrayList();
    actualIt.forEachRemaining((int value) -> actualList.add(value));
    int[] actualValues = actualList.toIntArray();
    assertArrayEquals(
        "ExpectedValues: " + Arrays.toString(expectedValues) + " ActualValues: "
            + Arrays.toString(actualValues),
        expectedValues,
        actualValues);
  }
}
