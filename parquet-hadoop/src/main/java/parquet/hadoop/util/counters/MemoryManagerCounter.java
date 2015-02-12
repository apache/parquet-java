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
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import parquet.hadoop.util.counters.mapred.MapRedCounterLoader;
import parquet.hadoop.util.counters.mapreduce.MapReduceCounterLoader;

public class MemoryManagerCounter {

  private static final String ENABLE_BLOCK_DOWNSIZE_COUNTER = "parquet.memory.manager.block.downsize";
  private static final String COUNTER_GROUP_NAME = "parquet";
  private static final String BLOCK_DOWNSIZE_COUNTER_NAME = "blockdownsize";
  private static ICounter blockDownsize = new BenchmarkCounter.NullCounter();

  private static CounterLoader counterLoader;

  public static void initCounterFromContext(TaskAttemptContext context) {
    counterLoader = new MapReduceCounterLoader(context);
    loadCounters();
  }

  public static void initCounterFromReporter(Reporter reporter, Configuration configuration) {
    counterLoader = new MapRedCounterLoader(reporter, configuration);
    loadCounters();
  }

  private static void loadCounters() {
    blockDownsize = counterLoader.getCounterByNameAndFlag(COUNTER_GROUP_NAME, BLOCK_DOWNSIZE_COUNTER_NAME, ENABLE_BLOCK_DOWNSIZE_COUNTER);
  }

  public static void incrementBlockDownsize(long val) {
    blockDownsize.increment(val);
  }

  public static long getBlockDownsize() {
    return blockDownsize.getCount();
  }
}
