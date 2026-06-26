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
package org.apache.parquet.column.values.fallback;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.RequiresFallback;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.api.Binary;

public class FallbackValuesWriter<I extends ValuesWriter & RequiresFallback, F extends ValuesWriter>
    extends ValuesWriter {

  public static <I extends ValuesWriter & RequiresFallback, F extends ValuesWriter> FallbackValuesWriter<I, F> of(
      I initialWriter, F fallBackWriter) {
    return new FallbackValuesWriter<>(initialWriter, fallBackWriter, /*dictionaryCheckThresholdRawSizeBytes=*/ 0);
  }

  public static <I extends ValuesWriter & RequiresFallback, F extends ValuesWriter> FallbackValuesWriter<I, F> of(
      I initialWriter, F fallBackWriter, long dictionaryCheckThresholdRawSizeBytes) {
    return new FallbackValuesWriter<>(initialWriter, fallBackWriter, dictionaryCheckThresholdRawSizeBytes);
  }

  /**
   * writer to start with
   */
  public final I initialWriter;
  /**
   * fallback
   */
  public final F fallBackWriter;

  private boolean fellBackAlready = false;
  private boolean compressionChecked = false;
  private final long dictionaryCheckThresholdRawSizeBytes;
  /** Accumulates raw bytes across pages (only reset in resetDictionary) so the
   * threshold check works even when individual pages are smaller than the threshold.
   * Overflow is not a concern: a long would require writing over 9.2 exabytes to a single
   * column chunk, which is physically impossible. */
  private long cumulativeRawBytes = 0;
  /** Accumulates dictionary-encoded size across pages (only reset in resetDictionary) so the
   * compression decision compares like-scoped cumulative quantities — the column-chunk
   * dictionary cost is amortized over all pages it covers, not charged against a single page. */
  private long cumulativeEncodedBytes = 0;

  /**
   * writer currently written to
   */
  private ValuesWriter currentWriter;

  private boolean initialUsedAndHadDictionary = false;

  /* size of raw data, even if dictionary is used, it will not have effect on raw data size, it is used to decide
   * if fall back to plain encoding is better by comparing rawDataByteSize with Encoded data size
   * It's also used in getBufferedSize, so the page will be written based on raw data size
   */
  private long rawDataByteSize = 0;

  public FallbackValuesWriter(I initialWriter, F fallBackWriter) {
    this(initialWriter, fallBackWriter, /*dictionaryCheckThresholdRawSizeBytes=*/ 0);
  }

  public FallbackValuesWriter(I initialWriter, F fallBackWriter, long dictionaryCheckThresholdRawSizeBytes) {
    super();
    this.initialWriter = initialWriter;
    this.fallBackWriter = fallBackWriter;
    this.currentWriter = initialWriter;
    this.dictionaryCheckThresholdRawSizeBytes = dictionaryCheckThresholdRawSizeBytes;
  }

  @Override
  public long getBufferedSize() {
    // use raw data size to decide if we want to flush the page
    // so the actual size of the page written could be much more smaller
    // due to dictionary encoding. This prevents page being too big when fallback happens.
    return rawDataByteSize;
  }

  @Override
  public BytesInput getBytes() {
    try {
      cumulativeRawBytes = Math.addExact(cumulativeRawBytes, rawDataByteSize);
    } catch (ArithmeticException e) {
      // overflow, keep the previous value
    }
    if (!fellBackAlready && !compressionChecked && cumulativeRawBytes >= dictionaryCheckThresholdRawSizeBytes) {
      compressionChecked = true;
      BytesInput bytes = initialWriter.getBytes();
      try {
        cumulativeEncodedBytes = Math.addExact(cumulativeEncodedBytes, bytes.size());
      } catch (ArithmeticException e) {
        // overflow, keep the previous value
      }
      // Compare cumulative raw vs cumulative encoded so the column-chunk dictionary
      // (which is itself cumulative) is amortized over all pages it covers, not charged
      // against a single page.
      if (!initialWriter.isCompressionSatisfying(cumulativeRawBytes, cumulativeEncodedBytes)) {
        fallBack();
      } else {
        return bytes;
      }
    }
    BytesInput result = currentWriter.getBytes();
    if (!fellBackAlready && !compressionChecked) {
      // Accumulate dictionary-encoded size for pages flushed before the check fires.
      try {
        cumulativeEncodedBytes = Math.addExact(cumulativeEncodedBytes, result.size());
      } catch (ArithmeticException e) {
        // overflow, keep the previous value
      }
    }
    return result;
  }

  @Override
  public Encoding getEncoding() {
    Encoding encoding = currentWriter.getEncoding();
    if (!fellBackAlready && !initialUsedAndHadDictionary) {
      initialUsedAndHadDictionary = encoding.usesDictionary();
    }
    return encoding;
  }

  @Override
  public void reset() {
    rawDataByteSize = 0;
    currentWriter.reset();
  }

  @Override
  public void close() {
    initialWriter.close();
    fallBackWriter.close();
  }

  @Override
  public DictionaryPage toDictPageAndClose() {
    if (initialUsedAndHadDictionary) {
      return initialWriter.toDictPageAndClose();
    } else {
      return currentWriter.toDictPageAndClose();
    }
  }

  @Override
  public void resetDictionary() {
    if (initialUsedAndHadDictionary) {
      initialWriter.resetDictionary();
    } else {
      currentWriter.resetDictionary();
    }
    currentWriter = initialWriter;
    fellBackAlready = false;
    compressionChecked = false;
    cumulativeRawBytes = 0;
    cumulativeEncodedBytes = 0;
    initialUsedAndHadDictionary = false;
  }

  @Override
  public long getAllocatedSize() {
    return currentWriter.getAllocatedSize();
  }

  @Override
  public String memUsageString(String prefix) {
    return String.format(
        "%s FallbackValuesWriter{\n" + "%s\n" + "%s\n" + "%s}\n",
        prefix,
        initialWriter.memUsageString(prefix + " initial:"),
        fallBackWriter.memUsageString(prefix + " fallback:"),
        prefix);
  }

  private void checkFallback() {
    if (!fellBackAlready && initialWriter.shouldFallBack()) {
      fallBack();
    }
  }

  private void fallBack() {
    fellBackAlready = true;
    initialWriter.fallBackAllValuesTo(fallBackWriter);
    currentWriter = fallBackWriter;
  }

  // passthrough writing the value

  @Override
  public void writeByte(int value) {
    rawDataByteSize += 1;
    currentWriter.writeByte(value);
    checkFallback();
  }

  @Override
  public void writeBytes(Binary v) {
    // for rawdata, length(4 bytes int) is stored, followed by the binary content itself
    rawDataByteSize += v.length() + 4;
    currentWriter.writeBytes(v);
    checkFallback();
  }

  @Override
  public void writeInteger(int v) {
    rawDataByteSize += 4;
    currentWriter.writeInteger(v);
    checkFallback();
  }

  @Override
  public void writeLong(long v) {
    rawDataByteSize += 8;
    currentWriter.writeLong(v);
    checkFallback();
  }

  @Override
  public void writeFloat(float v) {
    rawDataByteSize += 4;
    currentWriter.writeFloat(v);
    checkFallback();
  }

  @Override
  public void writeDouble(double v) {
    rawDataByteSize += 8;
    currentWriter.writeDouble(v);
    checkFallback();
  }
}
