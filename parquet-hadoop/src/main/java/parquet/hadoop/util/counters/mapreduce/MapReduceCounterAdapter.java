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
package parquet.hadoop.util.counters.mapreduce;

import org.apache.hadoop.mapreduce.Counter;
import parquet.hadoop.util.ContextUtil;
import parquet.hadoop.util.counters.ICounter;

/**
 * Adapt a mapreduce counter to ICounter
 * @author Tianshuo Deng
 */
public class MapReduceCounterAdapter implements ICounter {
  private Counter adaptee;

  public MapReduceCounterAdapter(Counter adaptee) {
    this.adaptee = adaptee;
  }

  @Override
  public void increment(long val) {
    ContextUtil.incrementCounter(adaptee, val);
  }

  @Override
  public long getCount() {
    return adaptee.getValue();  //To change body of implemented methods use File | Settings | File Templates.
  }
}
