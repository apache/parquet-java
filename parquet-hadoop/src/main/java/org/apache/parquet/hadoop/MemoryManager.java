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
package org.apache.parquet.hadoop;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a memory manager that keeps a global context of how many Parquet
 * writers there are and manages the memory between them. For use cases with
 * dynamic partitions, it is easy to end up with many writers in the same task.
 * By managing the size of each allocation, we try to cut down the size of each
 * allocation and keep the task from running out of memory.
 * <p>
 * This class balances the allocation size of each writer by resize them averagely.
 * When the sum of each writer's allocation size  is less than total memory pool,
 * keep their original value.
 * When the sum exceeds, decrease each writer's allocation size by a ratio.
 */
public class MemoryManager {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryManager.class);
  static final float DEFAULT_MEMORY_POOL_RATIO = 0.95f;
  static final long DEFAULT_MIN_MEMORY_ALLOCATION = 1 * 1024 * 1024; // 1MB
  private final float memoryPoolRatio;

  private final long totalMemoryPool;
  private final long minMemoryAllocation;
  private final Map<InternalParquetRecordWriter<?>, Long> writerList = new HashMap<>();
  // Multiset of the allocations currently in writerList, keyed by allocation size, so the
  // smallest live allocation (the first one that would breach minMemoryAllocation as scale
  // shrinks) can be found in O(log D) instead of scanning all of writerList, where D is the
  // number of distinct allocation sizes in use (typically 1, since writers in a job usually
  // share the same configured row group size).
  private final TreeMap<Long, Integer> allocationCounts = new TreeMap<>();
  private final Map<String, Runnable> callBacks = new HashMap<String, Runnable>();
  private long totalAllocations = 0;
  // Writers read this directly (MemoryManager#getScale) off the write path, so it must stay
  // cheap and lock-free from the reader's side; all writes to it happen under the monitor below.
  private volatile double scale = 1.0;

  public MemoryManager(float ratio, long minAllocation) {
    checkRatio(ratio);

    memoryPoolRatio = ratio;
    minMemoryAllocation = minAllocation;
    totalMemoryPool = Math.round((double)
            ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax()
        * ratio);
    LOG.debug("Allocated total memory pool is: {}", totalMemoryPool);
  }

  private void checkRatio(float ratio) {
    if (ratio <= 0 || ratio > 1) {
      throw new IllegalArgumentException(
          "The configured memory pool ratio " + ratio + " is " + "not between 0 and 1.");
    }
  }

  /**
   * Add a new writer and its memory allocation to the memory manager.
   *
   * @param writer     the new created writer
   * @param allocation the requested buffer size
   */
  synchronized void addWriter(InternalParquetRecordWriter<?> writer, Long allocation) {
    if (writerList.putIfAbsent(writer, allocation) != null) {
      throw new IllegalArgumentException("[BUG] The Parquet Memory Manager should not add an "
          + "instance of InternalParquetRecordWriter more than once. The Manager already contains "
          + "the writer: "
          + writer);
    }
    writer.setMemoryManager(this);
    totalAllocations += allocation;
    allocationCounts.merge(allocation, 1, Integer::sum);
    try {
      recomputeScale();
    } catch (RuntimeException e) {
      // A writer rejected here (e.g. its allocation would scale below minMemoryAllocation) never
      // escapes its constructor, so nothing will ever close it: undo the registration instead of
      // leaking the allocation in this JVM-global manager. A leaked allocation would keep every
      // later writer over-allocated and re-tripping the same exception on this JVM.
      writerList.remove(writer);
      totalAllocations -= allocation;
      allocationCounts.compute(allocation, (key, count) -> (count == null || count <= 1) ? null : count - 1);
      throw e;
    }
  }

  /**
   * Remove the given writer from the memory manager.
   *
   * @param writer the writer that has been closed
   */
  synchronized void removeWriter(InternalParquetRecordWriter<?> writer) {
    Long allocation = writerList.remove(writer);
    if (allocation == null) {
      return;
    }
    totalAllocations -= allocation;
    allocationCounts.compute(allocation, (key, count) -> (count == null || count <= 1) ? null : count - 1);
    if (!writerList.isEmpty()) {
      recomputeScale();
    } else {
      // No live writers are left: reset so a stale shrunken scale cannot be observed or used
      // while the manager sits idle (writers read scale lazily, so it is the sole live state).
      scale = 1.0;
    }
  }

  /**
   * Recompute the scale factor applied to every writer's row group size from the current total
   * allocation and pool size. Writers do not store a pushed threshold; each one reads {@link
   * #getScale()} lazily, so this only has to update the handful of fields below instead of
   * visiting every writer in {@code writerList}.
   */
  private void recomputeScale() {
    double newScale;
    if (totalAllocations <= totalMemoryPool) {
      newScale = 1.0;
    } else {
      newScale = (double) totalMemoryPool / totalAllocations;
      LOG.warn(String.format(
          "Total allocation exceeds %.2f%% (%,d bytes) of heap memory\n"
              + "Scaling row group sizes to %.2f%% for %d writers",
          100 * memoryPoolRatio, totalMemoryPool, 100 * newScale, writerList.size()));
      for (Runnable callBack : callBacks.values()) {
        // we do not really want to start a new thread here.
        callBack.run();
      }
    }

    if (newScale < 1.0 && minMemoryAllocation > 0 && !allocationCounts.isEmpty()) {
      // scale is a single multiplier applied to every writer's allocation, so the smallest
      // live allocation is always the first one to breach minMemoryAllocation.
      long minAllocation = allocationCounts.firstKey();
      long minScaledSize = (long) Math.floor(minAllocation * newScale);
      if (minScaledSize < minMemoryAllocation) {
        throw new ParquetMemoryManagerRuntimeException(String.format(
            "New Memory allocation %d bytes" + " is smaller than the minimum allocation size of %d bytes.",
            minScaledSize, minMemoryAllocation));
      }
    }

    scale = newScale;

    if (LOG.isDebugEnabled()) {
      // Only pay for visiting every writer when a caller has actually opted into DEBUG
      // logging; the scale itself is computed above without ever touching writerList.
      for (Map.Entry<InternalParquetRecordWriter<?>, Long> entry : writerList.entrySet()) {
        LOG.debug(String.format(
            "Adjust block size from %,d to %,d for writer: %s",
            entry.getValue(), (long) Math.floor(entry.getValue() * newScale), entry.getKey()));
      }
    }
  }

  /**
   * Get the total memory pool size that is available for writers.
   *
   * @return the number of bytes in the memory pool
   */
  long getTotalMemoryPool() {
    return totalMemoryPool;
  }

  /**
   * Get the writers list
   *
   * @return the writers in this memory manager
   */
  Map<InternalParquetRecordWriter<?>, Long> getWriterList() {
    return writerList;
  }

  /**
   * Get the ratio of memory allocated for all the writers.
   *
   * @return the memory pool ratio
   */
  float getMemoryPoolRatio() {
    return memoryPoolRatio;
  }

  /**
   * Register callback and deduplicate it if any.
   *
   * @param callBackName the name of callback. It should be identical.
   * @param callBack     the callback passed in from upper layer, such as Hive.
   */
  public void registerScaleCallBack(String callBackName, Runnable callBack) {
    Objects.requireNonNull(callBackName, "callBackName cannot be null");
    Objects.requireNonNull(callBack, "callBack cannot be null");

    if (callBacks.containsKey(callBackName)) {
      throw new IllegalArgumentException(
          "The callBackName " + callBackName + " is duplicated and has been registered already.");
    } else {
      callBacks.put(callBackName, callBack);
    }
  }

  /**
   * Get the registered callbacks.
   *
   * @return
   */
  Map<String, Runnable> getScaleCallBacks() {
    return Collections.unmodifiableMap(callBacks);
  }

  /**
   * Get the internal scale value of MemoryManger
   *
   * @return
   */
  double getScale() {
    return scale;
  }
}
