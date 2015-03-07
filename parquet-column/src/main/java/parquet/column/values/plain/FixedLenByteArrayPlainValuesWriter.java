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
package parquet.column.values.plain;

import java.io.IOException;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.bytes.LittleEndianDataOutputStream;
import parquet.column.values.ValuesWriter;
import parquet.column.Encoding;
import parquet.io.ParquetEncodingException;
import parquet.io.api.Binary;

/**
 * ValuesWriter for FIXED_LEN_BYTE_ARRAY.
 *
 * @author David Z. Chen <dchen@linkedin.com>
 */
public class FixedLenByteArrayPlainValuesWriter extends ValuesWriter {
  private static final Log LOG = Log.getLog(PlainValuesWriter.class);

  private CapacityByteArrayOutputStream arrayOut;
  private LittleEndianDataOutputStream out;
  private int length;

  public FixedLenByteArrayPlainValuesWriter(int length, int initialSize, int pageSize) {
    this.length = length;
    this.arrayOut = new CapacityByteArrayOutputStream(initialSize, pageSize);
    this.out = new LittleEndianDataOutputStream(arrayOut);
  }

  @Override
  public final void writeBytes(Binary v) {
    if (v.length() != length) {
      throw new IllegalArgumentException("Fixed Binary size " + v.length() +
          " does not match field type length " + length);
    }
    try {
      v.writeTo(out);
    } catch (IOException e) {
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
    if (Log.DEBUG) LOG.debug("writing a buffer of size " + arrayOut.size());
    return BytesInput.from(arrayOut);
  }

  @Override
  public void reset() {
    arrayOut.reset();
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
