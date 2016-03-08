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
package org.apache.parquet.column.values.boundedint;

import org.apache.parquet.bytes.ByteBufferAllocator;
import static org.apache.parquet.bytes.BytesInput.concat;
import static org.apache.parquet.column.Encoding.RLE;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bitpacking.BitPackingValuesWriter;
import org.apache.parquet.io.ParquetEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a special ColumnWriter for the case when you need to write
 * integers in a known range. This is intended primarily for use with
 * repetition and definition levels, since the maximum value that will
 * be written is known a priori based on the schema. Assumption is that
 * the values written are between 0 and the bound, inclusive.
 *
 * This differs from {@link BitPackingValuesWriter} in that this also performs
 * run-length encoding of the data, so is useful when long runs of repeated
 * values are expected.
 */
class BoundedIntValuesWriter extends ValuesWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(BoundedIntValuesWriter.class);

  private int currentValue = -1;
  private int currentValueCt = -1;
  private boolean currentValueIsRepeated = false;
  private boolean thereIsABufferedValue = false;
  private int shouldRepeatThreshold = 0;
  private int bitsPerValue;
  private BitWriter bitWriter;
  private boolean isFirst = true;

  private static final int[] byteToTrueMask = new int[8];
  static {
    int currentMask = 1;
    for (int i = 0; i < byteToTrueMask.length; i++) {
      byteToTrueMask[i] = currentMask;
      currentMask <<= 1;
    }
  }

  public BoundedIntValuesWriter(int bound, int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
    if (bound == 0) {
      throw new ParquetEncodingException("Value bound cannot be 0. Use DevNullColumnWriter instead.");
    }
    this.bitWriter = new BitWriter(initialCapacity, pageSize, allocator);
    bitsPerValue = (int)Math.ceil(Math.log(bound + 1)/Math.log(2));
    shouldRepeatThreshold = (bitsPerValue + 9)/(1 + bitsPerValue);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("init column with bit width of " + bitsPerValue + " and repeat threshold of " + shouldRepeatThreshold);
    }
  }

  @Override
  public long getBufferedSize() {
    // currentValue + currentValueCt = 8 bytes
    // shouldRepeatThreshold + bitsPerValue = 8 bytes
    // bitWriter = 8 bytes
    // currentValueIsRepeated + isFirst = 2 bytes (rounded to 8 b/c of word boundaries)
    return 32 + (bitWriter == null ? 0 : bitWriter.getMemSize());
  }

  // This assumes that the full state must be serialized, since there is no close method
  @Override
  public BytesInput getBytes() {
    serializeCurrentValue();
    BytesInput buf = bitWriter.finish();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("writing a buffer of size " + buf.size() + " + 4 bytes");
    }
    // We serialize the length so that on deserialization we can
    // deserialize as we go, instead of having to load everything
    // into memory
    return concat(BytesInput.fromInt((int)buf.size()), buf);
  }

  @Override
  public void reset() {
    currentValue = -1;
    currentValueCt = -1;
    currentValueIsRepeated = false;
    thereIsABufferedValue = false;
    isFirst = true;
    bitWriter.reset();
  }

  @Override
  public void close() {
    bitWriter.close();
  }

  @Override
  public void writeInteger(int val) {
    if (currentValue == val) {
      currentValueCt++;
      if (!currentValueIsRepeated && currentValueCt >= shouldRepeatThreshold) {
        currentValueIsRepeated = true;
      }
    } else {
      if (!isFirst) {
        serializeCurrentValue();
      } else {
        isFirst = false;
      }

      newCurrentValue(val);
    }
  }

  private void serializeCurrentValue() {
    if (thereIsABufferedValue) {
      if (currentValueIsRepeated) {
        bitWriter.writeBit(true);
        bitWriter.writeNBitInteger(currentValue, bitsPerValue);
        bitWriter.writeUnsignedVarint(currentValueCt);
      } else {
        for (int i = 0; i < currentValueCt; i++) {
          bitWriter.writeBit(false);
          bitWriter.writeNBitInteger(currentValue, bitsPerValue);
        }
      }
    }
    thereIsABufferedValue = false;
  }

  private void newCurrentValue(int val) {
    currentValue = val;
    currentValueCt = 1;
    currentValueIsRepeated = false;
    thereIsABufferedValue = true;
  }

  @Override
  public long getAllocatedSize() {
    return bitWriter.getCapacity();
  }

  @Override
  public Encoding getEncoding() {
    return RLE;
  }

  @Override
  public String memUsageString(String prefix) {
    return bitWriter.memUsageString(prefix);
  }

}
