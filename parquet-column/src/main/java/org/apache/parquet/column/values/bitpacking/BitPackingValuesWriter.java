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
import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.values.bitpacking.BitPacking.getBitPackingWriter;

import java.io.IOException;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bitpacking.BitPacking.BitPackingWriter;
import org.apache.parquet.io.ParquetEncodingException;

/**
 * a column writer that packs the ints in the number of bits required based on the maximum size.
 */
public class BitPackingValuesWriter extends ValuesWriter {

  private CapacityByteArrayOutputStream out;
  private BitPackingWriter bitPackingWriter;
  private int bitsPerValue;

  /**
   * @param bound           the maximum value stored by this column
   * @param initialCapacity initial capacity for the writer
   * @param pageSize        the page size
   * @param allocator       a buffer allocator
   */
  public BitPackingValuesWriter(int bound, int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
    this.bitsPerValue = getWidthFromMaxInt(bound);
    this.out = new CapacityByteArrayOutputStream(initialCapacity, pageSize, allocator);
    init();
  }

  private void init() {
    this.bitPackingWriter = getBitPackingWriter(bitsPerValue, out);
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.values.ValuesWriter#writeInteger(int)
   */
  @Override
  public void writeInteger(int v) {
    try {
      bitPackingWriter.write(v);
    } catch (IOException e) {
      throw new ParquetEncodingException(e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.values.ValuesWriter#getBufferedSize()
   */
  @Override
  public long getBufferedSize() {
    return out.size();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.values.ValuesWriter#getBytes()
   */
  @Override
  public BytesInput getBytes() {
    try {
      this.bitPackingWriter.finish();
      return BytesInput.from(out);
    } catch (IOException e) {
      throw new ParquetEncodingException(e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.values.ValuesWriter#reset()
   */
  @Override
  public void reset() {
    out.reset();
    init();
  }

  @Override
  public void close() {
    out.close();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.values.ValuesWriter#getAllocatedSize()
   */
  @Override
  public long getAllocatedSize() {
    return out.getCapacity();
  }

  @Override
  public String memUsageString(String prefix) {
    return out.memUsageString(prefix);
  }

  @Override
  public Encoding getEncoding() {
    return BIT_PACKED;
  }
}
