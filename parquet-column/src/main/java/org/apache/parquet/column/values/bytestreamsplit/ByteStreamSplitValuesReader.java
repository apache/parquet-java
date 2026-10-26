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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ByteStreamSplitValuesReader extends ValuesReader {
  private static final Logger LOG = LoggerFactory.getLogger(ByteStreamSplitValuesReader.class);
  protected final int elementSizeInBytes;
  protected ByteBuffer decodedDataBuffer;
  private int indexInStream;
  private int valuesCount;

  protected ByteStreamSplitValuesReader(int elementSizeInBytes) {
    this.elementSizeInBytes = elementSizeInBytes;
    this.indexInStream = 0;
    this.valuesCount = 0;
  }

  protected int nextElementByteOffset() {
    if (indexInStream >= valuesCount) {
      throw new ParquetDecodingException("Byte-stream data was already exhausted.");
    }
    int offset = indexInStream * elementSizeInBytes;
    ++indexInStream;
    return offset;
  }

  // Decode an entire data page by transposing from stream-split layout to interleaved layout.
  private byte[] decodeData(ByteBuffer encoded, int valuesCount) {
    int totalBytes = valuesCount * elementSizeInBytes;
    assert encoded.remaining() == totalBytes;

    // Bulk access: use the backing array directly if available, otherwise copy once.
    byte[] src;
    int srcBase;
    if (encoded.hasArray()) {
      src = encoded.array();
      srcBase = encoded.arrayOffset() + encoded.position();
    } else {
      src = new byte[totalBytes];
      encoded.get(src);
      srcBase = 0;
    }

    byte[] decoded = new byte[totalBytes];

    // Specialized single-pass loops for common element sizes.
    if (elementSizeInBytes == 2) {
      int s0 = srcBase, s1 = srcBase + valuesCount;
      for (int i = 0; i < valuesCount; ++i) {
        int di = i * 2;
        decoded[di] = src[s0 + i];
        decoded[di + 1] = src[s1 + i];
      }
    } else if (elementSizeInBytes == 4) {
      int s0 = srcBase,
          s1 = srcBase + valuesCount,
          s2 = srcBase + 2 * valuesCount,
          s3 = srcBase + 3 * valuesCount;
      for (int i = 0; i < valuesCount; ++i) {
        int di = i * 4;
        decoded[di] = src[s0 + i];
        decoded[di + 1] = src[s1 + i];
        decoded[di + 2] = src[s2 + i];
        decoded[di + 3] = src[s3 + i];
      }
    } else if (elementSizeInBytes == 8) {
      int s0 = srcBase,
          s1 = srcBase + valuesCount,
          s2 = srcBase + 2 * valuesCount,
          s3 = srcBase + 3 * valuesCount,
          s4 = srcBase + 4 * valuesCount,
          s5 = srcBase + 5 * valuesCount,
          s6 = srcBase + 6 * valuesCount,
          s7 = srcBase + 7 * valuesCount;
      for (int i = 0; i < valuesCount; ++i) {
        int di = i * 8;
        decoded[di] = src[s0 + i];
        decoded[di + 1] = src[s1 + i];
        decoded[di + 2] = src[s2 + i];
        decoded[di + 3] = src[s3 + i];
        decoded[di + 4] = src[s4 + i];
        decoded[di + 5] = src[s5 + i];
        decoded[di + 6] = src[s6 + i];
        decoded[di + 7] = src[s7 + i];
      }
    } else if (elementSizeInBytes == 12) {
      int s0 = srcBase,
          s1 = srcBase + valuesCount,
          s2 = srcBase + 2 * valuesCount,
          s3 = srcBase + 3 * valuesCount,
          s4 = srcBase + 4 * valuesCount,
          s5 = srcBase + 5 * valuesCount,
          s6 = srcBase + 6 * valuesCount,
          s7 = srcBase + 7 * valuesCount,
          s8 = srcBase + 8 * valuesCount,
          s9 = srcBase + 9 * valuesCount,
          s10 = srcBase + 10 * valuesCount,
          s11 = srcBase + 11 * valuesCount;
      for (int i = 0; i < valuesCount; ++i) {
        int di = i * 12;
        decoded[di] = src[s0 + i];
        decoded[di + 1] = src[s1 + i];
        decoded[di + 2] = src[s2 + i];
        decoded[di + 3] = src[s3 + i];
        decoded[di + 4] = src[s4 + i];
        decoded[di + 5] = src[s5 + i];
        decoded[di + 6] = src[s6 + i];
        decoded[di + 7] = src[s7 + i];
        decoded[di + 8] = src[s8 + i];
        decoded[di + 9] = src[s9 + i];
        decoded[di + 10] = src[s10 + i];
        decoded[di + 11] = src[s11 + i];
      }
    } else if (elementSizeInBytes == 16) {
      int s0 = srcBase,
          s1 = srcBase + valuesCount,
          s2 = srcBase + 2 * valuesCount,
          s3 = srcBase + 3 * valuesCount,
          s4 = srcBase + 4 * valuesCount,
          s5 = srcBase + 5 * valuesCount,
          s6 = srcBase + 6 * valuesCount,
          s7 = srcBase + 7 * valuesCount,
          s8 = srcBase + 8 * valuesCount,
          s9 = srcBase + 9 * valuesCount,
          s10 = srcBase + 10 * valuesCount,
          s11 = srcBase + 11 * valuesCount,
          s12 = srcBase + 12 * valuesCount,
          s13 = srcBase + 13 * valuesCount,
          s14 = srcBase + 14 * valuesCount,
          s15 = srcBase + 15 * valuesCount;
      for (int i = 0; i < valuesCount; ++i) {
        int di = i * 16;
        decoded[di] = src[s0 + i];
        decoded[di + 1] = src[s1 + i];
        decoded[di + 2] = src[s2 + i];
        decoded[di + 3] = src[s3 + i];
        decoded[di + 4] = src[s4 + i];
        decoded[di + 5] = src[s5 + i];
        decoded[di + 6] = src[s6 + i];
        decoded[di + 7] = src[s7 + i];
        decoded[di + 8] = src[s8 + i];
        decoded[di + 9] = src[s9 + i];
        decoded[di + 10] = src[s10 + i];
        decoded[di + 11] = src[s11 + i];
        decoded[di + 12] = src[s12 + i];
        decoded[di + 13] = src[s13 + i];
        decoded[di + 14] = src[s14 + i];
        decoded[di + 15] = src[s15 + i];
      }
    } else {
      // Generic fallback for arbitrary element sizes
      for (int stream = 0; stream < elementSizeInBytes; ++stream) {
        int srcOffset = srcBase + stream * valuesCount;
        for (int i = 0; i < valuesCount; ++i) {
          decoded[i * elementSizeInBytes + stream] = src[srcOffset + i];
        }
      }
    }
    return decoded;
  }

  @Override
  public void initFromPage(int valuesCount, ByteBufferInputStream stream)
      throws ParquetDecodingException, IOException {
    LOG.debug("init from page at offset {} for length {}", stream.position(), stream.available());

    // ByteStreamSplitValuesWriter does not write number of encoded values to the stream. As the parquet
    // specs state that no padding is allowed inside a data page, we can infer the number of values solely
    // by dividing the number of bytes in the stream by the size of each element.
    if (stream.available() % elementSizeInBytes != 0) {
      String errorMessage = String.format(
          "Invalid ByteStreamSplit stream, total length: %d bytes, element size: %d bytes.",
          stream.available(), elementSizeInBytes);
      throw new ParquetDecodingException(errorMessage);
    }
    this.valuesCount = stream.available() / elementSizeInBytes;

    // The input valuesCount includes number of nulls. It is used for an upperbound check only.
    if (valuesCount < this.valuesCount) {
      String errorMessage = String.format(
          "Invalid ByteStreamSplit stream, num values upper bound (w/ nulls): %d, num encoded values: %d",
          valuesCount, this.valuesCount);
      throw new ParquetDecodingException(errorMessage);
    }

    // Eagerly read and decode the data. This allows returning stable
    // Binary views into the internal decode buffer for FIXED_LEN_BYTE_ARRAY.
    final int totalSizeInBytes = stream.available();
    final ByteBuffer encodedData = stream.slice(totalSizeInBytes).slice(); // possibly zero-copy
    final byte[] decodedData = decodeData(encodedData, this.valuesCount);
    decodedDataBuffer = ByteBuffer.wrap(decodedData).order(ByteOrder.LITTLE_ENDIAN);
    indexInStream = 0;
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
