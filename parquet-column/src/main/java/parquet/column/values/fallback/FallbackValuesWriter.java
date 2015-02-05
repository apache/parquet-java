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
package parquet.column.values.fallback;

import parquet.bytes.BytesInput;
import parquet.column.Encoding;
import parquet.column.page.DictionaryPage;
import parquet.column.values.RequiresFallback;
import parquet.column.values.ValuesWriter;
import parquet.io.api.Binary;

public class FallbackValuesWriter<I extends ValuesWriter & RequiresFallback, F extends ValuesWriter> extends ValuesWriter {

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
   */
  private long rawDataByteSize = 0;

  /** indicates if this is the first page being processed */
  private boolean firstPage = true;

  public FallbackValuesWriter(I initialWriter, F fallBackWriter) {
    super();
    this.initialWriter = initialWriter;
    this.fallBackWriter = fallBackWriter;
    this.currentWriter = initialWriter;
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

  public DictionaryPage createDictionaryPage() {
    if (initialUsedAndHadDictionary) {
      return initialWriter.createDictionaryPage();
    } else {
      return currentWriter.createDictionaryPage();
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
