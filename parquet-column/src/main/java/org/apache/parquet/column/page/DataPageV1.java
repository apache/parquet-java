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
package org.apache.parquet.column.page;

import java.util.Optional;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;

public class DataPageV1 extends DataPage {

  private final BytesInput bytes;
  private final Statistics<?> statistics;
  private final Encoding rlEncoding;
  private final Encoding dlEncoding;
  private final Encoding valuesEncoding;
  private final int indexRowCount;

  // We need an additional flag, since we can not use a default value for the crc32
  private final int crc32;
  private final boolean isSetCrc32;

  /**
   * @param bytes the bytes for this page
   * @param valueCount count of values in this page
   * @param uncompressedSize the uncompressed size of the page
   * @param statistics of the page's values (max, min, num_null)
   * @param rlEncoding the repetition level encoding for this page
   * @param dlEncoding the definition level encoding for this page
   * @param valuesEncoding the values encoding for this page
   * @param crc32 the crc32 for this page
   */
  private DataPageV1(BytesInput bytes, int valueCount, int uncompressedSize,
                    Statistics<?> statistics, Encoding rlEncoding, Encoding dlEncoding,
                    Encoding valuesEncoding, int crc32, boolean isSetCrc32) {
    super(Math.toIntExact(bytes.size()), uncompressedSize, valueCount);
    this.bytes = bytes;
    this.statistics = statistics;
    this.rlEncoding = rlEncoding;
    this.dlEncoding = dlEncoding;
    this.valuesEncoding = valuesEncoding;
    this.indexRowCount = -1;
    this.crc32 = crc32;
    this.isSetCrc32 = isSetCrc32;
  }

  /**
   * @param bytes the bytes for this page
   * @param valueCount count of values in this page
   * @param uncompressedSize the uncompressed size of the page
   * @param statistics of the page's values (max, min, num_null)
   * @param rlEncoding the repetition level encoding for this page
   * @param dlEncoding the definition level encoding for this page
   * @param valuesEncoding the values encoding for this page
   */
  public DataPageV1(BytesInput bytes, int valueCount, int uncompressedSize,
                    Statistics<?> statistics, Encoding rlEncoding, Encoding dlEncoding,
                    Encoding valuesEncoding) {
    this(bytes, valueCount, uncompressedSize, statistics, rlEncoding, dlEncoding, valuesEncoding,
      0, false);
  }

  /**
   * @param bytes the bytes for this page
   * @param valueCount count of values in this page
   * @param uncompressedSize the uncompressed size of the page
   * @param statistics of the page's values (max, min, num_null)
   * @param rlEncoding the repetition level encoding for this page
   * @param dlEncoding the definition level encoding for this page
   * @param valuesEncoding the values encoding for this page
   * @param crc32 the crc32 for this page
   */
  public DataPageV1(BytesInput bytes, int valueCount, int uncompressedSize,
                    Statistics<?> statistics, Encoding rlEncoding, Encoding dlEncoding,
                    Encoding valuesEncoding, int crc32) {
    this(bytes, valueCount, uncompressedSize, statistics, rlEncoding, dlEncoding, valuesEncoding,
      crc32, true);
  }

  /**
   * @param bytes the bytes for this page
   * @param valueCount count of values in this page
   * @param uncompressedSize the uncompressed size of the page
   * @param firstRowIndex the index of the first row in this page
   * @param rowCount the number of rows in this page
   * @param statistics of the page's values (max, min, num_null)
   * @param rlEncoding the repetition level encoding for this page
   * @param dlEncoding the definition level encoding for this page
   * @param valuesEncoding the values encoding for this page
   * @param crc32 the crc32 for this page
   */
  private DataPageV1(BytesInput bytes, int valueCount, int uncompressedSize, long firstRowIndex,
                    int rowCount, Statistics<?> statistics, Encoding rlEncoding,
                    Encoding dlEncoding, Encoding valuesEncoding, int crc32, boolean isSetCrc32) {
    super(Math.toIntExact(bytes.size()), uncompressedSize, valueCount, firstRowIndex);
    this.bytes = bytes;
    this.statistics = statistics;
    this.rlEncoding = rlEncoding;
    this.dlEncoding = dlEncoding;
    this.valuesEncoding = valuesEncoding;
    this.indexRowCount = rowCount;
    this.crc32 = crc32;
    this.isSetCrc32 = isSetCrc32;
  }

  /**
   * @param bytes the bytes for this page
   * @param valueCount count of values in this page
   * @param uncompressedSize the uncompressed size of the page
   * @param firstRowIndex the index of the first row in this page
   * @param rowCount the number of rows in this page
   * @param statistics of the page's values (max, min, num_null)
   * @param rlEncoding the repetition level encoding for this page
   * @param dlEncoding the definition level encoding for this page
   * @param valuesEncoding the values encoding for this page
   */
  public DataPageV1(BytesInput bytes, int valueCount, int uncompressedSize, long firstRowIndex,
                    int rowCount, Statistics<?> statistics, Encoding rlEncoding,
                    Encoding dlEncoding, Encoding valuesEncoding) {
    this(bytes, valueCount, uncompressedSize, firstRowIndex, rowCount, statistics, rlEncoding,
      dlEncoding, valuesEncoding, 0, false);
  }

  /**
   * @param bytes the bytes for this page
   * @param valueCount count of values in this page
   * @param uncompressedSize the uncompressed size of the page
   * @param firstRowIndex the index of the first row in this page
   * @param rowCount the number of rows in this page
   * @param statistics of the page's values (max, min, num_null)
   * @param rlEncoding the repetition level encoding for this page
   * @param dlEncoding the definition level encoding for this page
   * @param valuesEncoding the values encoding for this page
   * @param crc32 the crc32 for this page
   */
  public DataPageV1(BytesInput bytes, int valueCount, int uncompressedSize, long firstRowIndex,
                    int rowCount, Statistics<?> statistics, Encoding rlEncoding,
                    Encoding dlEncoding, Encoding valuesEncoding, int crc32) {
    this(bytes, valueCount, uncompressedSize, firstRowIndex, rowCount, statistics, rlEncoding,
      dlEncoding, valuesEncoding, crc32, true);
  }

  /**
   * @return the bytes for the page
   */
  public BytesInput getBytes() {
    return bytes;
  }

  /**
   *
   * @return the statistics for this page (max, min, num_nulls)
   */
  public Statistics<?> getStatistics() {
    return statistics;
  }

  /**
   * @return the definition level encoding for this page
   */
  public Encoding getDlEncoding() {
    return dlEncoding;
  }

  /**
   * @return the repetition level encoding for this page
   */
  public Encoding getRlEncoding() {
    return rlEncoding;
  }

  /**
   * @return the values encoding for this page
   */
  public Encoding getValueEncoding() {
    return valuesEncoding;
  }

  /**
   * @return the crc32 for this page
   */
  public int getCrc32() {
    return crc32;
  }

  /**
   * @return the boolean representing whether the crc32 field is actually set
   */
  public boolean isSetCrc32() {
    return isSetCrc32;
  }

  @Override
  public String toString() {
    return "Page [bytes.size=" + bytes.size() + ", valueCount=" + getValueCount() +
      ", uncompressedSize=" + getUncompressedSize() + (isSetCrc32() ? ", crc=" + getCrc32() : "" )
      + "]";
  }

  @Override
  public <T> T accept(Visitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public Optional<Integer> getIndexRowCount() {
    return indexRowCount < 0 ? Optional.empty() : Optional.of(indexRowCount);
  }
}
