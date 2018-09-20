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
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FallbackValuesWriter<I extends ValuesWriter & RequiresFallback, F extends ValuesWriter> extends ValuesWriter {
  private static final Logger LOG = LoggerFactory.getLogger(FallbackValuesWriter.class);

  public static <I extends ValuesWriter & RequiresFallback, F extends ValuesWriter> FallbackValuesWriter<I, F> of(I initialWriter, F fallBackWriter) {
    return new FallbackValuesWriter<I, F>(initialWriter, fallBackWriter);
  }

  /** writer to start with */
  public final I initialWriter;
  /** fallback */
  public final F fallBackWriter;

  private boolean fellBackAlready = false;

  /** writer currently written to */
  private ValuesWriter currentWriter;

  private boolean initialUsedAndHadDictionary = false;

  /* size of raw data, even if dictionary is used, it will not have effect on raw data size, it is used to decide
   * if fall back to plain encoding is better by comparing rawDataByteSize with Encoded data size
   * It's also used in getBufferedSize, so the page will be written based on raw data size
   * Package-private visibility for testing only.
   */
  long rawDataByteSize = 0;

  /** indicates if this is the first page being processed */
  private boolean firstPage = true;

  public FallbackValuesWriter(I initialWriter, F fallBackWriter) {
    super();
    this.initialWriter = initialWriter;
    this.fallBackWriter = fallBackWriter;
    this.currentWriter = initialWriter;
  }

  final private double UTILIZATION_THRESHOLD = 0.6;

  // When using a dictionary writer with a fallback, it can happen that the
  // dictionary gets full and we have to fall back to the other writer. Since
  // the dictionary-encoded data is typically smaller than the raw data, a
  // fallback could significantly increase the size of the data.
  //
  // There is a logic aiming to ensure that pages and row groups have specific
  // sizes. This size-targeting logic can not cope with the sudden jump in the
  // reported data size that happens at a fallback. Adding one more record to
  // data that is much smaller than the limit could make it suddenly jump above
  // the limit.
  //
  // For this reason, we can not return the dictionary-encoded size here.
  // However, returning the raw size would not be optimal either, since that
  // would feed larger-than-actual sizes into the size-targeting logic, leading
  // to smaller-than-desired pages and row groups.
  //
  // The solution employed here is to return the dictionary-encoded size as long
  // as there is no risk of the dictionary becoming full, but as we reach a
  // threshold in the dictionary utilization, gradually change to returning an
  // estimation closer and closer to the raw data size. This smoothes the
  // transition from the "much smaller than the limit" to the "much larger than
  // the limit", thereby allowing the size-targeting logic to close the current
  // page or row group before it would exceed the limit.
  @Override
  public long getBufferedSize() {
    if (fellBackAlready) {
      return currentWriter.getBufferedSize();
    }
    final long dictEncodedByteSize = initialWriter.getBufferedSize();
    final double utilization = initialWriter.getUtilization();
    long weightedSize;
    if (utilization < UTILIZATION_THRESHOLD) {
      weightedSize = dictEncodedByteSize;
    } else {
      final double weightForRawSize = (utilization - UTILIZATION_THRESHOLD) / (1 - UTILIZATION_THRESHOLD);
      weightedSize = (long) (weightForRawSize * rawDataByteSize + (1 - weightForRawSize) * dictEncodedByteSize);
    }
    LOG.debug("utilization = {}, dictEncodedByteSize = {}, rawDataByteSize = {}, weightedSize = {}",
      utilization, dictEncodedByteSize, rawDataByteSize, weightedSize);
    return weightedSize;
  }

  @Override
  public BytesInput getBytes() {
    if (!fellBackAlready && firstPage) {
      // we use the first page to decide if we're going to use this encoding
      BytesInput bytes = initialWriter.getBytes();
      if (!initialWriter.isCompressionSatisfying(rawDataByteSize, bytes.size())) {
        fallBack();
      } else {
        return bytes;
      }
    }
    return currentWriter.getBytes();
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
    firstPage = false;
    currentWriter.reset();
  }

  @Override
  public void close() {
    initialWriter.close();
    fallBackWriter.close();
  }

  public DictionaryPage toDictPageAndClose() {
    if (initialUsedAndHadDictionary) {
      return initialWriter.toDictPageAndClose();
    } else {
      return currentWriter.toDictPageAndClose();
    }
  }

  public void resetDictionary() {
    if (initialUsedAndHadDictionary) {
      initialWriter.resetDictionary();
    } else {
      currentWriter.resetDictionary();
    }
    currentWriter = initialWriter;
    fellBackAlready = false;
    initialUsedAndHadDictionary = false;
    firstPage = true;
  }

  @Override
  public long getAllocatedSize() {
    return currentWriter.getAllocatedSize();
  }

  @Override
  public String memUsageString(String prefix) {
    return String.format(
        "%s FallbackValuesWriter{\n"
          + "%s\n"
          + "%s\n"
        + "%s}\n",
        prefix,
        initialWriter.memUsageString(prefix + " initial:"),
        fallBackWriter.memUsageString(prefix + " fallback:"),
        prefix
        );
  }

  private void checkFallback() {
    if (!fellBackAlready && initialWriter.shouldFallBack()) {
      fallBack();
    }
  }

  private void fallBack() {
    LOG.debug("Falling back");
    fellBackAlready = true;
    initialWriter.fallBackAllValuesTo(fallBackWriter);
    currentWriter = fallBackWriter;
  }

  // passthrough writing the value

  public void writeByte(int value) {
    rawDataByteSize += 1;
    currentWriter.writeByte(value);
    checkFallback();
  }

  public void writeBytes(Binary v) {
    //for rawdata, length(4 bytes int) is stored, followed by the binary content itself
    rawDataByteSize += v.length() + 4;
    currentWriter.writeBytes(v);
    checkFallback();
  }

  public void writeInteger(int v) {
    rawDataByteSize += 4;
    currentWriter.writeInteger(v);
    checkFallback();
  }

  public void writeLong(long v) {
    rawDataByteSize += 8;
    currentWriter.writeLong(v);
    checkFallback();
  }

  public void writeFloat(float v) {
    rawDataByteSize += 4;
    currentWriter.writeFloat(v);
    checkFallback();
  }

  public void writeDouble(double v) {
    rawDataByteSize += 8;
    currentWriter.writeDouble(v);
    checkFallback();
  }

}
