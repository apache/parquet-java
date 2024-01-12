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

import org.apache.hadoop.classification.InterfaceStability;

/**
 *  A simple interface to pass basic metric values by name to any implementation. Typically, an
 *  implementation of this interface will serve as a bridge to pass metric values on
 *  to the metrics system of a distributed engine (hadoop, spark, etc).
 *  <br>
 *  Development Note: This interface should provide a default implementation for any new metric tracker
 *  added to allow for backward compatibility
 *  <br>
 *   e.g.
 *  <br>
 *  <code>default addMaximum(key, value) { } ; </code>
 */
@InterfaceStability.Unstable
public interface ParquetMetricsCallback {
  void setValueInt(String name, int value);

  void setValueLong(String name, long value);

  void setValueFloat(String name, float value);

  void setValueDouble(String name, double value);

  void setDuration(String name, long value);
}
