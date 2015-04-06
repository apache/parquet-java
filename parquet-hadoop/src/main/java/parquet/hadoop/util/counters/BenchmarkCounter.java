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
package parquet.hadoop.util.counters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import parquet.hadoop.util.counters.mapred.MapRedCounterLoader;
import parquet.hadoop.util.counters.mapreduce.MapReduceCounterLoader;

/**
 * Encapsulate counter operations, compatible with Hadoop1/2, mapred/mapreduce API
 *
 * @author Tianshuo Deng
 */
public class BenchmarkCounter {

  private static final String ENABLE_BYTES_READ_COUNTER = "parquet.benchmark.bytes.read";
  private static final String ENABLE_BYTES_TOTAL_COUNTER = "parquet.benchmark.bytes.total";
  private static final String ENABLE_TIME_READ_COUNTER = "parquet.benchmark.time.read";
  private static final String COUNTER_GROUP_NAME = "parquet";
  private static final String BYTES_READ_COUNTER_NAME = "bytesread";
  private static final String BYTES_TOTAL_COUNTER_NAME = "bytestotal";
  private static final String TIME_READ_COUNTER_NAME = "timeread";
  private static ICounter bytesReadCounter = new NullCounter();
  private static ICounter totalBytesCounter = new NullCounter();
  private static ICounter timeCounter = new NullCounter();
  private static CounterLoader counterLoader;

  /**
   * Init counters in hadoop's mapreduce API, support both 1.x and 2.x
   *
   * @param context
   */
  public static void initCounterFromContext(TaskInputOutputContext<?, ?, ?, ?> context) {
    counterLoader = new MapReduceCounterLoader(context);
    loadCounters();
  }

  /**
   * Init counters in hadoop's mapred API, which is used by cascading and Hive.
   *
   * @param reporter
   * @param configuration
   */
  public static void initCounterFromReporter(Reporter reporter, Configuration configuration) {
    counterLoader = new MapRedCounterLoader(reporter, configuration);
    loadCounters();
  }

  private static void loadCounters() {
    bytesReadCounter = getCounterWhenFlagIsSet(COUNTER_GROUP_NAME, BYTES_READ_COUNTER_NAME, ENABLE_BYTES_READ_COUNTER);
    totalBytesCounter = getCounterWhenFlagIsSet(COUNTER_GROUP_NAME, BYTES_TOTAL_COUNTER_NAME, ENABLE_BYTES_TOTAL_COUNTER);
    timeCounter = getCounterWhenFlagIsSet(COUNTER_GROUP_NAME, TIME_READ_COUNTER_NAME, ENABLE_TIME_READ_COUNTER);
  }

  private static ICounter getCounterWhenFlagIsSet(String groupName, String counterName, String counterFlag) {
    return counterLoader.getCounterByNameAndFlag(groupName, counterName, counterFlag);
  }

  public static void incrementTotalBytes(long val) {
    totalBytesCounter.increment(val);
  }

  public static long getTotalBytes() {
    return totalBytesCounter.getCount();
  }

  public static void incrementBytesRead(long val) {
    bytesReadCounter.increment(val);
  }

  public static long getBytesRead() {
    return bytesReadCounter.getCount();
  }

  public static void incrementTime(long val) {
    timeCounter.increment(val);
  }

  public static long getTime() {
    return timeCounter.getCount();
  }

  public static class NullCounter implements ICounter {
    @Override
    public void increment(long val) {
      //do nothing
    }

    @Override
    public long getCount() {
      return 0;
    }
  }
}


