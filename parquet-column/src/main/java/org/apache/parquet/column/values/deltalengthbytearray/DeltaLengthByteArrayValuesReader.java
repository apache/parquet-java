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
package org.apache.parquet.column.values.deltalengthbytearray;

import java.io.IOException;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads binary data written by {@link DeltaLengthByteArrayValuesWriter}
 */
public class DeltaLengthByteArrayValuesReader extends ValuesReader {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaLengthByteArrayValuesReader.class);
  private ValuesReader lengthReader;
  private ByteBufferInputStream in;

  public DeltaLengthByteArrayValuesReader() {
    this.lengthReader = new DeltaBinaryPackingValuesReader();
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream stream) throws IOException {
    LOG.debug("init from page at offset {} for length {}", stream.position(), stream.available());
    lengthReader.initFromPage(valueCount, stream);
    this.in = stream.remainingStream();
  }

  @Override
  public Binary readBytes() {
    int length = lengthReader.readInteger();
    try {
      return Binary.fromConstantByteBuffer(in.slice(length));
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read " + length + " bytes");
    }
  }

  @Override
  public void skip() {
    skip(1);
  }

  @Override
  public void skip(int n) {
    int length = 0;
    for (int i = 0; i < n; ++i) {
      length += lengthReader.readInteger();
    }
    try {
      in.skipFully(length);
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to skip " + length + " bytes");
    }
  }
}
