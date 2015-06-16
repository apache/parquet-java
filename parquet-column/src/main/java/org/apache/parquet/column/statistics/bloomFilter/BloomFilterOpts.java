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
package org.apache.parquet.column.statistics.bloomFilter;

public class BloomFilterOpts {
  private int numBits;
  private int numHashFunctions;
  private boolean enabled;

  public BloomFilterOpts() {
    this.enabled = false;
  }

  public BloomFilterOpts(int numBits, int numHashFunctions) {
    this.numBits = numBits;
    this.numHashFunctions = numHashFunctions;
    this.enabled = true;
  }

  public int getNumBits() {
    return numBits;
  }

  public void setNumBits(int numBits) {
    this.numBits = numBits;
  }

  public int getNumHashFunctions() {
    return numHashFunctions;
  }

  public void setNumHashFunctions(int numHashFunctions) {
    this.numHashFunctions = numHashFunctions;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }
}