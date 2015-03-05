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
package parquet.column.values.rle;

import java.io.IOException;

import parquet.Ints;
import parquet.bytes.BytesInput;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.io.ParquetEncodingException;

/**
 * @author Alex Levenson
 */
public class RunLengthBitPackingHybridValuesWriter extends ValuesWriter {
  private final RunLengthBitPackingHybridEncoder encoder;

  public RunLengthBitPackingHybridValuesWriter(int bitWidth, int initialCapacity, int pageSize) {
    this.encoder = new RunLengthBitPackingHybridEncoder(bitWidth, initialCapacity, pageSize);
  }

  @Override
  public void writeInteger(int v) {
    try {
      encoder.writeInt(v);
    } catch (IOException e) {
      throw new ParquetEncodingException(e);
    }
  }

  @Override
  public void writeBoolean(boolean v) {
    writeInteger(v ? 1 : 0);
  }

  @Override
  public long getBufferedSize() {
    return encoder.getBufferedSize();
  }

  @Override
  public long getAllocatedSize() {
    return encoder.getAllocatedSize();
  }

  @Override
  public BytesInput getBytes() {
    try {
      // prepend the length of the column
      BytesInput rle = encoder.toBytes();
      return BytesInput.concat(BytesInput.fromInt(Ints.checkedCast(rle.size())), rle);
    } catch (IOException e) {
      throw new ParquetEncodingException(e);
    }
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.RLE;
  }

  @Override
  public void reset() {
    encoder.reset();
  }

  @Override
  public String memUsageString(String prefix) {
    return String.format("%s RunLengthBitPackingHybrid %d bytes", prefix, getAllocatedSize());
  }
}
