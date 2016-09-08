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
package org.apache.parquet.column.values.bitpacking;

import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;
import static org.apache.parquet.column.values.bitpacking.BitPacking.createBitPackingReader;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.BitPacking.BitPackingReader;
import org.apache.parquet.io.ParquetDecodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a column reader that packs the ints in the number of bits required based on the maximum size.
 *
 * @author Julien Le Dem
 *
 */
public class BitPackingValuesReader extends ValuesReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(BitPackingValuesReader.class);

  private ByteBufferInputStream in;
  private BitPackingReader bitPackingReader;
  private final int bitsPerValue;
  private int nextOffset;

  /**
   * @param bound the maximum value stored by this column
   */
  public BitPackingValuesReader(int bound) {
    this.bitsPerValue = getWidthFromMaxInt(bound);
  }

  /**
   * {@inheritDoc}
   * @see org.apache.parquet.column.values.ValuesReader#readInteger()
   */
  @Override
  public int readInteger() {
    try {
      return bitPackingReader.read();
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }

  /**
   * {@inheritDoc}
   * @see org.apache.parquet.column.values.ValuesReader#initFromPage(int, ByteBuffer, int)
   */
  @Override
  public void initFromPage(int valueCount, ByteBuffer in, int offset) throws IOException {
    int effectiveBitLength = valueCount * bitsPerValue;
    int length = BytesUtils.paddedByteCountFromBits(effectiveBitLength);
    LOGGER.debug("reading {} bytes for {} values of size {} bits.", length, valueCount, bitsPerValue);
    this.in = new ByteBufferInputStream(in, offset, length);
    this.bitPackingReader = createBitPackingReader(bitsPerValue, this.in, valueCount);
    this.nextOffset = offset + length;
  }

  @Override
  public int getNextOffset() {
    return nextOffset;
  }

  @Override
  public void skip() {
    readInteger();
  }

}
