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
import java.nio.ByteBuffer;

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinaryPlainValuesReader extends ValuesReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(BinaryPlainValuesReader.class);
  private ByteBuffer in;
  private int offset;

  @Override
  public Binary readBytes() {
    try {
      int length = BytesUtils.readIntLittleEndian(in, offset);
      int start = offset + 4;
      offset = start + length;
      return Binary.fromConstantByteBuffer(in, start, length);
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read bytes at offset " + offset, e);
    } catch (RuntimeException e) {
      throw new ParquetDecodingException("could not read bytes at offset " + offset, e);
    }
  }

  @Override
  public void skip() {
    try {
      int length = BytesUtils.readIntLittleEndian(in, offset);
      offset += 4 + length;
    } catch (IOException e) {
      throw new ParquetDecodingException("could not skip bytes at offset " + offset, e);
    } catch (RuntimeException e) {
      throw new ParquetDecodingException("could not skip bytes at offset " + offset, e);
    }
  }

  @Override
  public void initFromPage(int valueCount, ByteBuffer in, int offset)
      throws IOException {
    LOGGER.debug("init from page at offset {} for length {}", offset, (in.limit() - offset));
    this.in = in;
    this.offset = offset;
  }
}
