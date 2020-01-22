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
package org.apache.parquet.column.values.delta;

import java.io.IOException;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.BytePackerForLong;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;

import java.nio.ByteBuffer;

/**
 * Read values written by {@link DeltaBinaryPackingValuesWriter}
 */
public class DeltaBinaryPackingValuesReader extends ValuesReader {
  private int totalValueCount;
  /**
   * values read by the caller
   */
  private int valuesRead;
  private long minDeltaInCurrentBlock;

  /**
   * stores the decoded values including the first value which is written to the
   * header
   */
  private long[] valuesBuffer;
  /**
   * values loaded to the buffer, it could be bigger than the totalValueCount when
   * data is not aligned to mini block, which means padding 0s are in the buffer
   */
  private int valuesBuffered;
  private ByteBufferInputStream in;
  private DeltaBinaryPackingConfig config;
  private int[] bitWidths;

  /**
   * eagerly loads all the data into memory
   */
  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream stream) throws IOException {
    this.in = stream;
    long startPos = in.position();
    this.config = DeltaBinaryPackingConfig.readConfig(in);
    this.totalValueCount = BytesUtils.readUnsignedVarInt(in);
    allocateValuesBuffer();
    bitWidths = new int[config.miniBlockNumInABlock];

    // read first value from header
    valuesBuffer[valuesBuffered++] = BytesUtils.readZigZagVarLong(in);

    while (valuesBuffered < totalValueCount) { // values Buffered could be more than totalValueCount, since we flush on
                                               // a mini block basis
      loadNewBlockToBuffer();
    }
    updateNextOffset((int) (in.position() - startPos));
  }

  /**
   * the value buffer is allocated so that the size of it is multiple of mini
   * block because when writing, data is flushed on a mini block basis
   */
  private void allocateValuesBuffer() {
    int totalMiniBlockCount = (int) Math.ceil((double) totalValueCount / config.miniBlockSizeInValues);
    // + 1 because first value written to header is also stored in values buffer
    valuesBuffer = new long[totalMiniBlockCount * config.miniBlockSizeInValues + 1];
  }

  @Override
  public void skip() {
    checkRead();
    valuesRead++;
  }

  @Override
  public void skip(int n) {
    // checkRead() is invoked before incrementing valuesRead so increase valuesRead
    // size in 2 steps
    valuesRead += n - 1;
    checkRead();
    ++valuesRead;
  }

  @Override
  public int readInteger() {
    // TODO: probably implement it separately
    return (int) readLong();
  }

  @Override
  public long readLong() {
    checkRead();
    return valuesBuffer[valuesRead++];
  }

  private void checkRead() {
    if (valuesRead >= totalValueCount) {
      throw new ParquetDecodingException("no more value to read, total value count is " + totalValueCount);
    }
  }

  private void loadNewBlockToBuffer() throws IOException {
    try {
      minDeltaInCurrentBlock = BytesUtils.readZigZagVarLong(in);
    } catch (IOException e) {
      throw new ParquetDecodingException("can not read min delta in current block", e);
    }

    readBitWidthsForMiniBlocks();

    // mini block is atomic for reading, we read a mini block when there are more
    // values left
    int i;
    for (i = 0; i < config.miniBlockNumInABlock && valuesBuffered < totalValueCount; i++) {
      BytePackerForLong packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(bitWidths[i]);
      unpackMiniBlock(packer);
    }

    // calculate values from deltas unpacked for current block
    int valueUnpacked = i * config.miniBlockSizeInValues;
    for (int j = valuesBuffered - valueUnpacked; j < valuesBuffered; j++) {
      int index = j;
      valuesBuffer[index] += minDeltaInCurrentBlock + valuesBuffer[index - 1];
    }
  }

  /**
   * mini block has a size of 8*n, unpack 8 value each time
   *
   * @param packer the packer created from bitwidth of current mini block
   */
  private void unpackMiniBlock(BytePackerForLong packer) throws IOException {
    for (int j = 0; j < config.miniBlockSizeInValues; j += 8) {
      unpack8Values(packer);
    }
  }

  private void unpack8Values(BytePackerForLong packer) throws IOException {
    // get a single buffer of 8 values. most of the time, this won't require a copy
    // TODO: update the packer to consume from an InputStream
    ByteBuffer buffer = in.slice(packer.getBitWidth());
    packer.unpack8Values(buffer, buffer.position(), valuesBuffer, valuesBuffered);
    this.valuesBuffered += 8;
  }

  private void readBitWidthsForMiniBlocks() {
    for (int i = 0; i < config.miniBlockNumInABlock; i++) {
      try {
        bitWidths[i] = BytesUtils.readIntLittleEndianOnOneByte(in);
      } catch (IOException e) {
        throw new ParquetDecodingException("Can not decode bitwidth in block header", e);
      }
    }
  }
}
