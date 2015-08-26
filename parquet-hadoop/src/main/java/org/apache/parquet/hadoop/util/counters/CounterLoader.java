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
package org.apache.parquet.hadoop.util.counters;

/**
 * Factory interface for CounterLoaders, will load the counter according to groupName, counterName,
 * and if in the configuration, flag with name counterFlag is false, the counter will not be loaded
 * @author Tianshuo Deng
 */
public interface CounterLoader {
  public ICounter getCounterByNameAndFlag(String groupName, String counterName, String counterFlag);
}
