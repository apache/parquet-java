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

import static parquet.column.Encoding.BIT_PACKED;

import java.io.IOException;

import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.io.ParquetEncodingException;

public class ByteBitPackingValuesWriter extends ValuesWriter {

  private final Packer packer;
  private final int bitWidth;
  private ByteBasedBitPackingEncoder encoder;

  public ByteBitPackingValuesWriter(int bound, Packer packer) {
    this.packer = packer;
    this.bitWidth = BytesUtils.getWidthFromMaxInt(bound);
    this.encoder = new ByteBasedBitPackingEncoder(bitWidth, packer);
  }

  @Override
  public void writeInteger(int v) {
    try {
      this.encoder.writeInt(v);
    } catch (IOException e) {
      throw new ParquetEncodingException(e);
    }
  }

  @Override
  public Encoding getEncoding() {
    return BIT_PACKED;
  }

  @Override
  public BytesInput getBytes() {
    try {
      return encoder.toBytes();
    } catch (IOException e) {
      throw new ParquetEncodingException(e);
    }
  }

  @Override
  public void reset() {
    encoder = new ByteBasedBitPackingEncoder(bitWidth, packer);
  }

  @Override
  public long getBufferedSize() {
    return encoder.getBufferSize();
  }

  @Override
  public long getAllocatedSize() {
    return encoder.getAllocatedSize();
  }

  @Override
  public String memUsageString(String prefix) {
    return encoder.memUsageString(prefix);
  }

}
