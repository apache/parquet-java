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

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;

/**
 * Page (v1) holder that holds on to page data and encoding etc.
 */
public class PageV1Holder extends PageHolder {
  private final Statistics statistics;
  private final Encoding rlEncoding;
  private final Encoding dlEncoding;

  public PageV1Holder(int pageIndex, BytesInput bytes, int valueCount, Statistics statistics, Encoding rlEncoding, Encoding dlEncoding, Encoding valuesEncoding) {
    super(pageIndex, bytes, valueCount, valuesEncoding);
    this.statistics = statistics;
    this.rlEncoding = rlEncoding;
    this.dlEncoding = dlEncoding;
  }

  public Statistics getStatistics() {
    return statistics;
  }

  public Encoding getRlEncoding() {
    return rlEncoding;
  }

  public Encoding getDlEncoding() {
    return dlEncoding;
  }

}
