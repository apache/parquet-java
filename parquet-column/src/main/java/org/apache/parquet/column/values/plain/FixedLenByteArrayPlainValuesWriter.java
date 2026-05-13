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
package org.apache.parquet.column.values.plain;

import java.io.IOException;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.bytes.LittleEndianDataOutputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ValuesWriter for FIXED_LEN_BYTE_ARRAY.
 */
public class FixedLenByteArrayPlainValuesWriter extends ValuesWriter {
  private static final Logger LOG = LoggerFactory.getLogger(PlainValuesWriter.class);

  private CapacityByteArrayOutputStream arrayOut;
  private LittleEndianDataOutputStream out;
  private int length;
  private ByteBufferAllocator allocator;

  public FixedLenByteArrayPlainValuesWriter(
      int length, int initialSize, int pageSize, ByteBufferAllocator allocator) {
    this.length = length;
    this.allocator = allocator;
    this.arrayOut = new CapacityByteArrayOutputStream(initialSize, pageSize, this.allocator);
    this.out = new LittleEndianDataOutputStream(arrayOut);
  }

  @Override
  public final void writeBytes(Binary v) {
    if (v.length() != length) {
      throw new IllegalArgumentException(
          "Fixed Binary size " + v.length() + " does not match field type length " + length);
    }
    try {
      v.writeTo(out);
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write fixed bytes", e);
    }
  }

  /**
   * Batch write: copies Binary values into a temporary buffer and writes them in a single
   * bulk {@code write()} call to the output stream, amortizing stream overhead across
   * the entire batch.
   */
  @Override
  public void writeBinaries(Binary[] values, int offset, int length) {
    final int fixedLen = this.length;
    // Process in chunks to avoid excessive temp allocation
    final int CHUNK = 1024;
    byte[] buf = new byte[Math.min(length, CHUNK) * fixedLen];
    try {
      int remaining = length;
      int srcIdx = offset;
      while (remaining > 0) {
        int batch = Math.min(remaining, CHUNK);
        int bufPos = 0;
        for (int i = 0; i < batch; i++) {
          Binary v = values[srcIdx++];
          if (v.length() != fixedLen) {
            throw new IllegalArgumentException(
                "Fixed Binary size " + v.length() + " does not match field type length " + fixedLen);
          }
          // Copy bytes from the Binary's backing store into the batch buffer
          byte[] bytes = v.getBytesUnsafe();
          System.arraycopy(bytes, 0, buf, bufPos, fixedLen);
          bufPos += fixedLen;
        }
        arrayOut.write(buf, 0, bufPos);
        remaining -= batch;
      }
    } catch (RuntimeException e) {
      throw new ParquetEncodingException("could not write fixed bytes", e);
    }
  }

  @Override
  public long getBufferedSize() {
    return arrayOut.size();
  }

  @Override
  public BytesInput getBytes() {
    try {
      out.flush();
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write page", e);
    }
    LOG.debug("writing a buffer of size {}", arrayOut.size());
    return BytesInput.from(arrayOut);
  }

  @Override
  public void reset() {
    arrayOut.reset();
  }

  @Override
  public void close() {
    arrayOut.close();
  }

  @Override
  public long getAllocatedSize() {
    return arrayOut.getCapacity();
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.PLAIN;
  }

  @Override
  public String memUsageString(String prefix) {
    return arrayOut.memUsageString(prefix + " PLAIN");
  }
}
