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
package org.apache.parquet.hadoop.util.counters.mapreduce;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.apache.parquet.hadoop.util.counters.CounterLoader;
import org.apache.parquet.hadoop.util.counters.ICounter;

/**
 * Concrete factory for counters in mapred API,
 * get a counter using mapreduce API when the corresponding flag is set, otherwise return a NullCounter
 */
public class MapReduceCounterLoader implements CounterLoader {
  private TaskAttemptContext context;

  public MapReduceCounterLoader(TaskAttemptContext context) {
    this.context = context;
  }

  @Override
  public ICounter getCounterByNameAndFlag(String groupName, String counterName, String counterFlag) {
    if (ContextUtil.getConfiguration(context).getBoolean(counterFlag, true)) {
      return new MapReduceCounterAdapter(ContextUtil.getCounter(context, groupName, counterName));
    } else {
      return new BenchmarkCounter.NullCounter();
    }
  }
}
