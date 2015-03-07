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
package parquet.column.values.deltastrings;

import parquet.bytes.BytesInput;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import parquet.io.api.Binary;

/**
 * Write prefix lengths using delta encoding, followed by suffixes with Delta length byte arrays
 * <pre>
 *   {@code
 *   delta-length-byte-array : prefix-length* suffixes*
 *   }
 * </pre>
 * @author Aniket Mokashi
 *
 */
public class DeltaByteArrayWriter extends ValuesWriter{

  private ValuesWriter prefixLengthWriter;
  private ValuesWriter suffixWriter;
  private byte[] previous;

  public DeltaByteArrayWriter(int initialCapacity, int pageSize) {
    this.prefixLengthWriter = new DeltaBinaryPackingValuesWriter(128, 4, initialCapacity, pageSize);
    this.suffixWriter = new DeltaLengthByteArrayValuesWriter(initialCapacity, pageSize);
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
    for(i = 0; (i < length) && (previous[i] == vb[i]); i++);
    prefixLengthWriter.writeInteger(i);
    suffixWriter.writeBytes(Binary.fromByteArray(vb, i, vb.length - i));
    previous = vb;
  }
}
