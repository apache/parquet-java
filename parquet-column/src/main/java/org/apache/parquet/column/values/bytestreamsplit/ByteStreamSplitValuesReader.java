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
  private final int numStreams;
  private final int elementSizeInBytes;
  private byte[] byteStreamData;
  private int indexInStream;
  private int numberOfValues;

  protected ByteStreamSplitValuesReader(int elementSizeInBytes) {
    this.numStreams = elementSizeInBytes;
    this.elementSizeInBytes = elementSizeInBytes;
    this.indexInStream = 0;
    this.numberOfValues = 0;
  }

  protected void gatherElementDataFromStreams(byte[] gatheredData)
          throws IOException, ArrayIndexOutOfBoundsException {
    if (gatheredData.length != numStreams) {
      throw new IOException("gatherData buffer is not of the expected size.");
    }
    if (indexInStream >= numberOfValues) {
      throw new ArrayIndexOutOfBoundsException("Byte-stream data was already exhausted.");
    }
    for (int i = 0; i < numStreams; ++i) {
      gatheredData[i] = byteStreamData[i * numberOfValues + indexInStream];
    }
    ++indexInStream;
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream stream) throws IOException {
    LOG.debug("init from page at offset {} for length {}", stream.position(), stream.available());

    if (valueCount * elementSizeInBytes > stream.available()) {
      throw new IOException("Stream does not contain enough data for the given number of values.");
    }

    /* Allocate buffer for all of the byte stream data */
    final int totalSizeInBytes = valueCount * numStreams;
    byteStreamData = new byte[totalSizeInBytes];

    /* Eagerly read the data for each stream */
    final int numRead = stream.read(byteStreamData, 0, totalSizeInBytes);
    if (numRead != totalSizeInBytes) {
      String errorMessage = String.format("Failed to read requested number of bytes. Expected: %d. Read %d.",
              totalSizeInBytes, numRead);
      throw new IOException(errorMessage);
    }

    indexInStream = 0;
    numberOfValues = valueCount;
  }

  @Override
  public void skip() {
    skip(1);
  }

  @Override
  public void skip(int n) {
    if (indexInStream + n > numberOfValues || n < 0) {
      String errorMessage = String.format(
              "Cannot skip this many elements. Current index: %d. Skip %d. Total number of elements: %d",
              indexInStream, n, numberOfValues);
      throw new ParquetDecodingException(errorMessage);
    }
    indexInStream += n;
  }

}
