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
package org.apache.parquet.hadoop.util.counters.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Reporter;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.apache.parquet.hadoop.util.counters.CounterLoader;
import org.apache.parquet.hadoop.util.counters.ICounter;

/**
 * Concrete factory for counters in mapred API,
 * get a counter using mapred API when the corresponding flag is set, otherwise return a NullCounter
 */
public class MapRedCounterLoader implements CounterLoader {
  private Reporter reporter;
  private Configuration conf;

  public MapRedCounterLoader(Reporter reporter, Configuration conf) {
    this.reporter = reporter;
    this.conf = conf;
  }

  @Override
  public ICounter getCounterByNameAndFlag(String groupName, String counterName, String counterFlag) {
    if (conf.getBoolean(counterFlag, true)) {
      Counters.Counter counter = reporter.getCounter(groupName, counterName);
      if (counter != null) {
        return new MapRedCounterAdapter(reporter.getCounter(groupName, counterName));
      }
    }
    return new BenchmarkCounter.NullCounter();
  }
}
