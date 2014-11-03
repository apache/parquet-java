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
  static final double MEMORY_POOL_PERCENTAGE = 0.5;

  private static MemoryManager memoryManager = null;
  private final long totalMemoryPool;
  private final Map<InternalParquetRecordWriter, Integer> writerList = new
      HashMap<InternalParquetRecordWriter, Integer>();

  private MemoryManager() {
    totalMemoryPool = Math.round(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax
        () * MEMORY_POOL_PERCENTAGE);
    LOG.debug(String.format("Allocated total memory pool is: %,d", totalMemoryPool));
  }

  public static synchronized MemoryManager getMemoryManager() {
    if (memoryManager == null) {
      memoryManager = new MemoryManager();
    }
    return memoryManager;
  }

  /**
   * Add a new writer and its memory allocation to the memory manager.
   * @param writer the new created writer
   * @param allocation the requested buffer size
   */
  synchronized void addWriter(InternalParquetRecordWriter writer, Integer allocation) {
    Integer oldValue = writerList.get(writer);
    if (oldValue == null) {
      writerList.put(writer, allocation);
    } else {
      //This should not happen.
      LOG.warn("The memory manager already contains writer: " + writer);
      return;
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
    int totalAllocations = 0;
    boolean isAllocationExceed;
    for (Integer allocation : writerList.values()) {
      totalAllocations += allocation;
    }
    if (totalAllocations <= totalMemoryPool) {
      isAllocationExceed = false;
    } else {
      isAllocationExceed = true;
    }

    if (!isAllocationExceed) {
      for (Map.Entry<InternalParquetRecordWriter, Integer> entry : writerList.entrySet()) {
        entry.getKey().setRowGroupSize(entry.getValue());
      }
    } else {
      double scale = (double) totalMemoryPool / totalAllocations;
      for (Map.Entry<InternalParquetRecordWriter, Integer> entry : writerList.entrySet()) {
        int newSize = (int) Math.floor(entry.getValue() * scale);
        entry.getKey().setRowGroupSize(newSize);
        LOG.debug(String.format("Adjust block size to %,d for writer: %s", newSize, entry.getKey()));
      }
    }
  }

  /**
   * Get the total memory pool size that is available for writers.
   * @return the number of bytes in the memory pool
   */
  long getTotalMemoryPool() {
    return totalMemoryPool;
  }
}
