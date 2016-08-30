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
import java.nio.ByteBuffer;

import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.io.api.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads binary data written by {@link DeltaLengthByteArrayValuesWriter}
 *
 * @author Aniket Mokashi
 *
 */
public class DeltaLengthByteArrayValuesReader extends ValuesReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeltaLengthByteArrayValuesReader.class);
  private ValuesReader lengthReader;
  private ByteBuffer in;
  private int offset;

  public DeltaLengthByteArrayValuesReader() {
    this.lengthReader = new DeltaBinaryPackingValuesReader();
  }

  @Override
  public void initFromPage(int valueCount, ByteBuffer in, int offset)
      throws IOException {
    LOGGER.debug("init from page at offset {} for length {}", offset, (in.limit() - offset));
    lengthReader.initFromPage(valueCount, in, offset);
    offset = lengthReader.getNextOffset();
    this.in = in;
    this.offset = offset;
  }

  @Override
  public Binary readBytes() {
    int length = lengthReader.readInteger();
    int start = offset;
    offset = start + length;
    return Binary.fromConstantByteBuffer(in, start, length);
  }

  @Override
  public void skip() {
    int length = lengthReader.readInteger();
    offset = offset + length;
  }
}
