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
package parquet.hadoop;

import parquet.Log;
import parquet.ParquetRuntimeException;

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
  static final float DEFAULT_MEMORY_POOL_RATIO = 0.95f;
  static final long DEFAULT_MIN_MEMORY_ALLOCATION = 1 * 1024 * 1024; // 1MB
  private final float memoryPoolRatio;

  private final long totalMemoryPool;
  private final long minMemoryAllocation;
  private final Map<InternalParquetRecordWriter, Long> writerList = new
      HashMap<InternalParquetRecordWriter, Long>();

  public MemoryManager(float ratio, long minAllocation) {
    checkRatio(ratio);

    memoryPoolRatio = ratio;
    minMemoryAllocation = minAllocation;
    totalMemoryPool = Math.round(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax
        () * ratio);
    LOG.debug(String.format("Allocated total memory pool is: %,d", totalMemoryPool));
  }

  private void checkRatio(float ratio) {
    if (ratio <= 0 || ratio > 1) {
      throw new IllegalArgumentException("The configured memory pool ratio " + ratio + " is " +
          "not between 0 and 1.");
    }
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
      LOG.warn(String.format(
          "Total allocation exceeds %.2f%% (%,d bytes) of heap memory\n" +
          "Scaling row group sizes to %.2f%% for %d writers",
          100*memoryPoolRatio, totalMemoryPool, 100*scale, writerList.size()));
    }

    int maxColCount = 0;
    for (InternalParquetRecordWriter w : writerList.keySet()) {
      maxColCount = Math.max(w.getSchema().getColumns().size(), maxColCount);
    }

    for (Map.Entry<InternalParquetRecordWriter, Long> entry : writerList.entrySet()) {
      long newSize = (long) Math.floor(entry.getValue() * scale);
      if(scale < 1.0 && minMemoryAllocation > 0 && newSize < minMemoryAllocation) {
          throw new ParquetRuntimeException(String.format("New Memory allocation %d bytes" +
          " is smaller than the minimum allocation size of %d bytes.",
              newSize, minMemoryAllocation)){};
      }
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
   * Get the ratio of memory allocated for all the writers.
   * @return the memory pool ratio
   */
  float getMemoryPoolRatio() {
    return memoryPoolRatio;
  }
}
