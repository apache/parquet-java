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
package parquet.column.values.bitpacking;

import static parquet.bytes.BytesUtils.getWidthFromMaxInt;
import static parquet.column.Encoding.BIT_PACKED;
import static parquet.column.values.bitpacking.BitPacking.getBitPackingWriter;

import java.io.IOException;

import parquet.bytes.BytesInput;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.column.values.bitpacking.BitPacking.BitPackingWriter;
import parquet.io.ParquetEncodingException;

/**
 * a column writer that packs the ints in the number of bits required based on the maximum size.
 *
 * @author Julien Le Dem
 *
 */
public class BitPackingValuesWriter extends ValuesWriter {

  private CapacityByteArrayOutputStream out;
  private BitPackingWriter bitPackingWriter;
  private int bitsPerValue;

  /**
   * @param bound the maximum value stored by this column
   * @param pageSize
   */
  public BitPackingValuesWriter(int bound, int initialCapacity, int pageSize) {
    this.bitsPerValue = getWidthFromMaxInt(bound);
    this.out = new CapacityByteArrayOutputStream(initialCapacity, pageSize);
    init();
  }

  private void init() {
    this.bitPackingWriter = getBitPackingWriter(bitsPerValue, out);
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.values.ValuesWriter#writeInteger(int)
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
   * @see parquet.column.values.ValuesWriter#getBufferedSize()
   */
  @Override
  public long getBufferedSize() {
    return out.size();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.values.ValuesWriter#getBytes()
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
   * @see parquet.column.values.ValuesWriter#reset()
   */
  @Override
  public void reset() {
    out.reset();
    init();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.values.ValuesWriter#getAllocatedSize()
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
