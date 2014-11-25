/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.hadoop;

import parquet.Log;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * Implements a memory manager that keeps a global context of how many Parquet
 * writers there are and manages the memory between them. For use cases with
 * dynamic partitions, it is easy to end up with many writers in the same task.
 * By managing the size of each allocation, we try to cut down the size of each
 * allocation and keep the task from running out of memory.
 *
 * This class balances the allocation size of each writer by resize them averagely.
 * When the sum of each writer's allocation size  is less than total memory pool,
 * keep them original value.
 * When the sum exceeds, decrease each writer's allocation size by a ratio.
 */
public class MemoryManager {
  private static final Log LOG = Log.getLog(MemoryManager.class);
  public static final float DEFAULT_MEMORY_POOL_RATIO = 0.95f;
  private static float memoryPoolRatio = -1f;
  private static boolean isRatioConfigured = false;

  private final long totalMemoryPool;
  private final Map<InternalParquetRecordWriter, Long> writerList = new
      HashMap<InternalParquetRecordWriter, Long>();

  public MemoryManager() {
    float ratio;
    if (memoryPoolRatio > 0 && memoryPoolRatio <= 1) {
      ratio = memoryPoolRatio;
    } else {
      ratio = DEFAULT_MEMORY_POOL_RATIO;
    }

    totalMemoryPool = Math.round(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax
        () * ratio);
    LOG.debug(String.format("Allocated total memory pool is: %,d", totalMemoryPool));
  }

  /**
   * Add a new writer and its memory allocation to the memory manager.
   * @param writer the new created writer
   * @param allocation the requested buffer size
   */
  synchronized void addWriter(InternalParquetRecordWriter writer, Long allocation) {
    Long oldValue = writerList.get(writer);
    if (oldValue == null) {
      writerList.put(writer, allocation);
    } else {
      throw new IllegalArgumentException("[BUG] The Parquet Memory Manager should not add an " +
          "instance of InternalParquetRecordWriter more than once. The Manager already contains " +
          "the writer: " + writer);
    }
    updateAllocation();
  }

  /**
   * Remove the given writer from the memory manager.
   * @param writer the writer that has been closed
   */
  synchronized void removeWriter(InternalParquetRecordWriter writer) {
    if (writerList.containsKey(writer)) {
      writerList.remove(writer);
    }
    if (!writerList.isEmpty()) {
      updateAllocation();
    }
  }

  /**
   * Update the allocated size of each writer based on the current allocations and pool size.
   */
  private void updateAllocation() {
    long totalAllocations = 0;
    double scale;
    for (Long allocation : writerList.values()) {
      totalAllocations += allocation;
    }
    if (totalAllocations <= totalMemoryPool) {
      scale = 1.0;
    } else {
      scale = (double) totalMemoryPool / totalAllocations;
    }

    for (Map.Entry<InternalParquetRecordWriter, Long> entry : writerList.entrySet()) {
      long newSize = (long) Math.floor(entry.getValue() * scale);
      entry.getKey().setRowGroupSizeThreshold(newSize);
      LOG.debug(String.format("Adjust block size from %,d to %,d for writer: %s",
            entry.getValue(), newSize, entry.getKey()));
    }
  }

  /**
   * Get the total memory pool size that is available for writers.
   * @return the number of bytes in the memory pool
   */
  long getTotalMemoryPool() {
    return totalMemoryPool;
  }

  /**
   * Get the writers list
   * @return the writers in this memory manager
   */
  Map<InternalParquetRecordWriter, Long> getWriterList() {
    return writerList;
  }

  /**
   * Set the ratio of memory allocated for all the writers.
   * Different users may have different preferred ratio.
   * @param memoryPoolRatio equal (allocated memory size / JVM total memory size)
   * @return return true if the ratio is set successfully or its value has already been set
   *         return false if the ratio does not equal the already configured memoryPoolRatio
   */
  public static synchronized boolean setMemoryPoolRatio(float ratio) {
    boolean success;

    if (!isRatioConfigured) {
      // This is the first time to configure the ratio
      if (ratio > 0 && ratio <= 1) {
        memoryPoolRatio = ratio;
        isRatioConfigured = true;
        success = true;
      } else {
        throw new IllegalArgumentException("The configured memory pool ratio " + ratio + " is " +
            "not between 0 and 1.");
      }
    } else {
      // The ratio has been configured. It is better to not change it during the task lifecycle.
      if (ratio == memoryPoolRatio) {
        success = true;
      } else {
        success = false;
      }
    }
    return success;
  }

  /**
   * Get the ratio of memory allocated for all the writers.
   * @return the memory pool ratio
   */
  public static synchronized float getMemoryPoolRatio() {
    return memoryPoolRatio;
  }
}
