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
public class PageV2Holder extends PageHolder {
  private final int rowCount;
  private final int nullCount;
  private final BytesInput repetitionLevels;
  private final BytesInput definitionLevels;
  private final Statistics<?> statistics;

  public PageV2Holder(int pageIndex, int rowCount, int nullCount, int valueCount, BytesInput repetitionLevels, BytesInput definitionLevels, Encoding dataEncoding, BytesInput data, Statistics<?> statistics) {
    super(pageIndex, data, valueCount, dataEncoding);
    this.rowCount = rowCount;
    this.nullCount = nullCount;
    this.repetitionLevels = repetitionLevels;
    this.definitionLevels = definitionLevels;
    this.statistics = statistics;
  }

  public int getRowCount() {
    return rowCount;
  }

  public int getNullCount() {
    return nullCount;
  }


  public BytesInput getRepetitionLevels() {
    return repetitionLevels;
  }

  public BytesInput getDefinitionLevels() {
    return definitionLevels;
  }

  public Statistics<?> getStatistics() {
    return statistics;
  }
}
