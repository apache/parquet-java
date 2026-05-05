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
import java.nio.ByteOrder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plain encoding reader for BINARY values.
 *
 * <p>Reads directly from a {@link ByteBuffer} with {@link ByteOrder#LITTLE_ENDIAN} byte order,
 * using {@link ByteBuffer#getInt()} for the 4-byte length prefix instead of 4 individual
 * {@code InputStream.read()} calls through {@link org.apache.parquet.bytes.BytesUtils#readIntLittleEndian}.
 */
public class BinaryPlainValuesReader extends ValuesReader {
  private static final Logger LOG = LoggerFactory.getLogger(BinaryPlainValuesReader.class);
  private ByteBuffer buffer;

  @Override
  public Binary readBytes() {
    int length = buffer.getInt();
    ByteBuffer valueSlice = buffer.slice();
    valueSlice.limit(length);
    buffer.position(buffer.position() + length);
    return Binary.fromConstantByteBuffer(valueSlice);
  }

  @Override
  public void skip() {
    int length = buffer.getInt();
    buffer.position(buffer.position() + length);
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream stream) throws IOException {
    LOG.debug(
        "init from page at offset {} for length {}",
        stream.position(),
        (stream.available() - stream.position()));
    int available = stream.available();
    if (available > 0) {
      this.buffer = stream.slice(available).order(ByteOrder.LITTLE_ENDIAN);
    } else {
      this.buffer = ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN);
    }
  }
}
