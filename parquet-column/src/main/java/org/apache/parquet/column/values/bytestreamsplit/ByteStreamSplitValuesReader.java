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
package org.apache.parquet.column.values.bytestreamsplit;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ByteStreamSplitValuesReader extends ValuesReader {

  private static final Logger LOG = LoggerFactory.getLogger(ByteStreamSplitValuesReader.class);
  private final int elementSizeInBytes;
  private byte[] byteStreamData;
  private int indexInStream;
  private int valuesCount;

  protected ByteStreamSplitValuesReader(int elementSizeInBytes) {
    this.elementSizeInBytes = elementSizeInBytes;
    this.indexInStream = 0;
    this.valuesCount = 0;
  }

  protected void gatherElementDataFromStreams(byte[] gatheredData)
          throws ParquetDecodingException {
    if (gatheredData.length != elementSizeInBytes) {
      throw new ParquetDecodingException("gatherData buffer is not of the expected size.");
    }
    if (indexInStream >= valuesCount) {
      throw new ParquetDecodingException("Byte-stream data was already exhausted.");
    }
    for (int i = 0; i < elementSizeInBytes; ++i) {
      gatheredData[i] = byteStreamData[i * valuesCount + indexInStream];
    }
    ++indexInStream;
  }

  @Override
  public void initFromPage(int valuesCount, ByteBufferInputStream stream) throws ParquetDecodingException, IOException {
    LOG.debug("init from page at offset {} for length {}", stream.position(), stream.available());

    if (valuesCount * elementSizeInBytes > stream.available()) {
      String errorMessage = String.format(
              "Stream does not contain enough data for the given number of values. Num values: %d. Element size: %d Bytes available: %d",
              valuesCount, elementSizeInBytes, stream.available());
      throw new ParquetDecodingException(errorMessage);
    }

    // Allocate buffer for all of the byte stream data.
    final int totalSizeInBytes = valuesCount * elementSizeInBytes;
    byteStreamData = new byte[totalSizeInBytes];

    // Eagerly read the data for each stream.
    final int numRead = stream.read(byteStreamData, 0, totalSizeInBytes);
    if (numRead != totalSizeInBytes) {
      String errorMessage = String.format("Failed to read requested number of bytes. Expected: %d. Read %d.",
              totalSizeInBytes, numRead);
      throw new ParquetDecodingException(errorMessage);
    }

    indexInStream = 0;
    this.valuesCount = valuesCount;
  }

  @Override
  public void skip() {
    skip(1);
  }

  @Override
  public void skip(int n) {
    if (n < 0 || indexInStream + n > valuesCount) {
      String errorMessage = String.format(
              "Cannot skip this many elements. Current index: %d. Skip %d. Total number of elements: %d",
              indexInStream, n, valuesCount);
      throw new ParquetDecodingException(errorMessage);
    }
    indexInStream += n;
  }

}
