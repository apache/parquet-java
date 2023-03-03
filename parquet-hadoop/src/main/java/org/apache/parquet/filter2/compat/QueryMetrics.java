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

package org.apache.parquet.filter2.compat;

public class QueryMetrics {
  private String skipBloomFilter = "";
  private long skipBloomBlocks = 0;
  private long skipBloomRows = 0;
  private long totalBloomBlocks = 0;

  public String getSkipBloomFilter() {
    return skipBloomFilter;
  }

  public void setSkipBloomFilter(String skipBloomFilter) {
    this.skipBloomFilter = skipBloomFilter;
  }

  public long getSkipBloomBlocks() {
    return skipBloomBlocks;
  }

  public void setSkipBloomBlocks(long skipBloomBlocks) {
    this.skipBloomBlocks = skipBloomBlocks;
  }

  public long getSkipBloomRows() {
    return skipBloomRows;
  }

  public void setSkipBloomRows(long skipBloomRows) {
    this.skipBloomRows = skipBloomRows;
  }

  public long getTotalBloomBlocks() {
    return totalBloomBlocks;
  }

  public void setTotalBloomBlocks(long totalBloomBlocks) {
    this.totalBloomBlocks = totalBloomBlocks;
  }
}
