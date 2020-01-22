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

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.ParquetEncodingException;

import java.io.IOException;

/**
 * Write integers with delta encoding and binary packing The format is as
 * follows:
 * 
 * <pre>
 *   {@code
 *     delta-binary-packing: <page-header> <block>*
 *     page-header := <block size in values> <number of miniblocks in a block> <total value count> <first value>
 *     block := <min delta> <list of bitwidths of miniblocks> <miniblocks>
 *
 *     min delta : zig-zag var int encoded
 *     bitWidthsOfMiniBlock : 1 byte little endian
 *     blockSizeInValues,blockSizeInValues,totalValueCount,firstValue : unsigned varint
 *   }
 * </pre>
 * <p>
 * The algorithm and format is inspired by D. Lemire's paper:
 * http://lemire.me/blog/archives/2012/09/12/fast-integer-compression-decoding-billions-of-integers-per-second/
 */
public abstract class DeltaBinaryPackingValuesWriter extends ValuesWriter {

  public static final int DEFAULT_NUM_BLOCK_VALUES = 128;

  public static final int DEFAULT_NUM_MINIBLOCKS = 4;

  protected final CapacityByteArrayOutputStream baos;

  /**
   * stores blockSizeInValues, miniBlockNumInABlock and miniBlockSizeInValues
   */
  protected final DeltaBinaryPackingConfig config;

  /**
   * bit width for each mini block, reused between flushes
   */
  protected final int[] bitWidths;

  protected int totalValueCount = 0;

  /**
   * a pointer to deltaBlockBuffer indicating the end of deltaBlockBuffer the
   * number of values in the deltaBlockBuffer that haven't flushed to baos it will
   * be reset after each flush
   */
  protected int deltaValuesToFlush = 0;

  /**
   * bytes buffer for a mini block, it is reused for each mini block. Therefore
   * the size of biggest miniblock with bitwith of MAX_BITWITH is allocated
   */
  protected byte[] miniBlockByteBuffer;

// TODO: remove this.
  public DeltaBinaryPackingValuesWriter(int slabSize, int pageSize, ByteBufferAllocator allocator) {
    this(DEFAULT_NUM_BLOCK_VALUES, DEFAULT_NUM_MINIBLOCKS, slabSize, pageSize, allocator);
  }

  public DeltaBinaryPackingValuesWriter(int blockSizeInValues, int miniBlockNum, int slabSize, int pageSize,
      ByteBufferAllocator allocator) {
    this.config = new DeltaBinaryPackingConfig(blockSizeInValues, miniBlockNum);
    bitWidths = new int[config.miniBlockNumInABlock];
    baos = new CapacityByteArrayOutputStream(slabSize, pageSize, allocator);
  }

  @Override
  public long getBufferedSize() {
    return baos.size();
  }

  protected void writeBitWidthForMiniBlock(int i) {
    try {
      BytesUtils.writeIntLittleEndianOnOneByte(baos, bitWidths[i]);
    } catch (IOException e) {
      throw new ParquetEncodingException("can not write bitwith for miniblock", e);
    }
  }

  protected int getMiniBlockCountToFlush(double numberCount) {
    return (int) Math.ceil(numberCount / config.miniBlockSizeInValues);
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.DELTA_BINARY_PACKED;
  }

  @Override
  public void reset() {
    this.totalValueCount = 0;
    this.baos.reset();
    this.deltaValuesToFlush = 0;
  }

  @Override
  public void close() {
    this.totalValueCount = 0;
    this.baos.close();
    this.deltaValuesToFlush = 0;
  }

  @Override
  public long getAllocatedSize() {
    return baos.getCapacity();
  }

  @Override
  public String memUsageString(String prefix) {
    return String.format("%s DeltaBinaryPacking %d bytes", prefix, getAllocatedSize());
  }
}
