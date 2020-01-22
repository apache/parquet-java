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
package org.apache.parquet.column.values.rle;

import java.io.IOException;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * This ValuesReader does all the reading in {@link #initFromPage} and stores
 * the values in an in memory buffer, which is less than ideal.
 */
public class RunLengthBitPackingHybridValuesReader extends ValuesReader {
  private final int bitWidth;
  private RunLengthBitPackingHybridDecoder decoder;

  public RunLengthBitPackingHybridValuesReader(int bitWidth) {
    this.bitWidth = bitWidth;
  }

  @Override
  public void initFromPage(int valueCountL, ByteBufferInputStream stream) throws IOException {
    int length = BytesUtils.readIntLittleEndian(stream);
    this.decoder = new RunLengthBitPackingHybridDecoder(bitWidth, stream.sliceStream(length));

    // 4 is for the length which is stored as 4 bytes little endian
    updateNextOffset(length + 4);
  }

  @Override
  public int readInteger() {
    try {
      return decoder.readInt();
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }

  @Override
  public boolean readBoolean() {
    return readInteger() == 0 ? false : true;
  }

  @Override
  public void skip() {
    readInteger();
  }
}
