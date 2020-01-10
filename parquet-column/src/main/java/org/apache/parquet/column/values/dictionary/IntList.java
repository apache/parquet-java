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

import java.util.ArrayList;
import java.util.List;

/**
 * An append-only integer list
 * avoids autoboxing and buffer resizing
 */
public class IntList {

  static final int MAX_SLAB_SIZE = 64 * 1024;
  static final int INITIAL_SLAB_SIZE = 4 * 1024;

  // Double slab size till we reach the max slab size. At that point we just add slabs of size
  // MAX_SLAB_SIZE. This ensures we don't allocate very large slabs from the start if we don't have
  // too much data.
  private int currentSlabSize = INITIAL_SLAB_SIZE;

  /**
   * Visible for testing to verify the current slab size
   */
  int getCurrentSlabSize() {
    return currentSlabSize;
  }

  /**
   * to iterate on the content of the list
   * not an actual iterator to avoid autoboxing
   */
  public static class IntIterator {

    private final int[][] slabs;
    private final int count;

    private int current;
    private int currentRow;
    private int currentCol;

    /**
     * slabs will be iterated in order up to the provided count
     * as the last slab may not be full
     * @param slabs contain the ints
     * @param count count of ints
     */
    public IntIterator(int[][] slabs, int count) {
      this.slabs = slabs;
      this.count = count;
    }

    /**
     * @return whether there is a next value
     */
    public boolean hasNext() {
      return current < count;
    }

    /**
     * @return the next int
     */
    public int next() {
      final int result = slabs[currentRow][currentCol];
      incrementPosition();
      return result;
    }

    private void incrementPosition() {
      current++;
      currentCol++;
      if (currentCol >= slabs[currentRow].length) {
        currentCol = 0;
        currentRow++;
      }
    }
  }

  private List<int[]> slabs = new ArrayList<>();

  // Lazy initialize currentSlab only when needed to save on memory in cases where items might
  // not be added
  private int[] currentSlab;
  private int currentSlabPos;

  private void allocateSlab() {
    currentSlab = new int[currentSlabSize];
    currentSlabPos = 0;
  }

  // Double slab size up to the MAX_SLAB_SIZE limit
  private void updateCurrentSlabSize() {
    if (currentSlabSize < MAX_SLAB_SIZE) {
      currentSlabSize *= 2;
      if (currentSlabSize > MAX_SLAB_SIZE) {
        currentSlabSize = MAX_SLAB_SIZE;
      }
    }
  }

  /**
   * @param i value to append to the end of the list
   */
  public void add(int i) {
    if (currentSlab == null) {
      allocateSlab();
    } else if (currentSlabPos == currentSlab.length) {
      slabs.add(currentSlab);
      updateCurrentSlabSize();
      allocateSlab();
    }

    currentSlab[currentSlabPos] = i;
    ++ currentSlabPos;
  }

  /**
   * (not an actual Iterable)
   * @return an IntIterator on the content
   */
  public IntIterator iterator() {
    if (currentSlab == null) {
      allocateSlab();
    }

    int[][] itSlabs = slabs.toArray(new int[slabs.size() + 1][]);
    itSlabs[slabs.size()] = currentSlab;
    return new IntIterator(itSlabs, size());
  }

  /**
   * @return the current size of the list
   */
  public int size() {
    int size = currentSlabPos;
    for (int [] slab : slabs) {
      size += slab.length;
    }

    return size;
  }

}
