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
package org.apache.parquet.column.values.symboltable;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.symboltable.SymbolTablePayload.OffsetEncoding;

/**
 * Builds the data page body {@link SymbolTablePayload} reads.
 *
 * <p>Holds the code stream and the offsets, so a codec's writer only has to compress values and
 * hand the codes over. The layout is documented on the read side.
 */
public final class SymbolTablePayloadWriter implements AutoCloseable {

  private final OffsetEncoding offsetEncoding;
  private final CapacityByteArrayOutputStream codes;

  /**
   * The offset section, written through an existing encoder rather than by hand.
   *
   * <p>Plain int32 and delta binary packing are both already spelled out elsewhere in this module,
   * and a second spelling of either would be a second thing to keep byte-compatible.
   */
  private final ValuesWriter offsetWriter;

  private int valueCount;

  public SymbolTablePayloadWriter(
      OffsetEncoding offsetEncoding, int initialSlabSize, int pageSize, ByteBufferAllocator allocator) {
    this.offsetEncoding = offsetEncoding;
    this.codes = new CapacityByteArrayOutputStream(initialSlabSize, pageSize, allocator);
    this.offsetWriter = offsetEncoding == OffsetEncoding.PLAIN
        ? new PlainValuesWriter(initialSlabSize, pageSize, allocator)
        : new DeltaBinaryPackingValuesWriterForInteger(
            DeltaBinaryPackingValuesWriter.DEFAULT_NUM_BLOCK_VALUES,
            DeltaBinaryPackingValuesWriter.DEFAULT_NUM_MINIBLOCKS,
            initialSlabSize,
            pageSize,
            allocator);
  }

  /** Appends one value's codes and records where it ends. */
  public void addValue(byte[] valueCodes, int offset, int length) {
    codes.write(valueCodes, offset, length);
    long end = codes.size();
    if (end > Integer.MAX_VALUE) {
      // The offsets are int32 on the wire, so this is a format limit rather than a Java one.
      throw new IllegalStateException("Symbol table code stream exceeds 2 GB: " + end);
    }
    offsetWriter.writeInteger((int) end);
    valueCount++;
  }

  public int valueCount() {
    return valueCount;
  }

  /** Bytes buffered so far, excluding the fixed header, for a caller watching the page size. */
  public long bufferedSize() {
    return codes.size() + offsetWriter.getBufferedSize();
  }

  public long allocatedSize() {
    return codes.getCapacity() + offsetWriter.getAllocatedSize();
  }

  /**
   * The finished page body.
   *
   * <p>A page holding no values gets an empty offset section rather than an encoder's empty-input
   * output, because the offset encoding's own framing would otherwise be the only thing in the
   * section and a reader has no values to spend it on.
   */
  public BytesInput getBytes() {
    BytesInput offsets = valueCount == 0 ? BytesInput.empty() : offsetWriter.getBytes();
    long offsetSectionSize = offsets.size();
    if (offsetSectionSize > Integer.MAX_VALUE) {
      throw new IllegalStateException("Symbol table offset section exceeds 2 GB: " + offsetSectionSize);
    }
    byte[] header = new byte[SymbolTablePayload.HEADER_SIZE];
    header[0] = (byte) offsetEncoding.value();
    writeIntLittleEndian(header, 1, valueCount);
    writeIntLittleEndian(header, 5, (int) offsetSectionSize);
    return BytesInput.concat(BytesInput.from(header), offsets, BytesInput.from(codes));
  }

  private static void writeIntLittleEndian(byte[] destination, int position, int value) {
    destination[position] = (byte) value;
    destination[position + 1] = (byte) (value >>> 8);
    destination[position + 2] = (byte) (value >>> 16);
    destination[position + 3] = (byte) (value >>> 24);
  }

  public void reset() {
    codes.reset();
    offsetWriter.reset();
    valueCount = 0;
  }

  @Override
  public void close() {
    codes.close();
    offsetWriter.close();
  }
}
