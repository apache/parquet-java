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
package org.apache.parquet.column.values.deltastrings;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.io.api.Binary;

/**
 * Write prefix lengths using delta encoding, followed by suffixes with Delta
 * length byte arrays
 * 
 * <pre>
 *   {@code
 *   delta-length-byte-array : prefix-length* suffixes*
 *   }
 * </pre>
 */
public class DeltaByteArrayWriter extends ValuesWriter {

  private ValuesWriter prefixLengthWriter;
  private ValuesWriter suffixWriter;
  private byte[] previous;

  public DeltaByteArrayWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
    this.prefixLengthWriter = new DeltaBinaryPackingValuesWriterForInteger(128, 4, initialCapacity, pageSize,
        allocator);
    this.suffixWriter = new DeltaLengthByteArrayValuesWriter(initialCapacity, pageSize, allocator);
    this.previous = new byte[0];
  }

  @Override
  public long getBufferedSize() {
    return prefixLengthWriter.getBufferedSize() + suffixWriter.getBufferedSize();
  }

  @Override
  public BytesInput getBytes() {
    return BytesInput.concat(prefixLengthWriter.getBytes(), suffixWriter.getBytes());
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.DELTA_BYTE_ARRAY;
  }

  @Override
  public void reset() {
    prefixLengthWriter.reset();
    suffixWriter.reset();
    previous = new byte[0];
  }

  @Override
  public void close() {
    prefixLengthWriter.close();
    suffixWriter.close();
  }

  @Override
  public long getAllocatedSize() {
    return prefixLengthWriter.getAllocatedSize() + suffixWriter.getAllocatedSize();
  }

  @Override
  public String memUsageString(String prefix) {
    prefix = prefixLengthWriter.memUsageString(prefix);
    return suffixWriter.memUsageString(prefix + "  DELTA_STRINGS");
  }

  @Override
  public void writeBytes(Binary v) {
    int i = 0;
    byte[] vb = v.getBytes();
    int length = previous.length < vb.length ? previous.length : vb.length;
    // find the number of matching prefix bytes between this value and the previous
    // one
    for (i = 0; (i < length) && (previous[i] == vb[i]); i++)
      ;
    prefixLengthWriter.writeInteger(i);
    suffixWriter.writeBytes(v.slice(i, vb.length - i));
    previous = vb;
  }
}
