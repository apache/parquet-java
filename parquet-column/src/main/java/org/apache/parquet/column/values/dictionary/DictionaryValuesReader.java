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
package org.apache.parquet.column.values.dictionary;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads values that have been dictionary encoded
 */
public class DictionaryValuesReader extends ValuesReader {
  private static final Logger LOG = LoggerFactory.getLogger(DictionaryValuesReader.class);

  private ByteBufferInputStream in;

  private Dictionary dictionary;

  private RunLengthBitPackingHybridDecoder decoder;

  public DictionaryValuesReader(Dictionary dictionary) {
    this.dictionary = dictionary;
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream stream) throws IOException {
    this.in = stream.remainingStream();
    if (in.available() > 0) {
      LOG.debug("init from page at offset {} for length {}", stream.position(), stream.available());
      int bitWidth = BytesUtils.readIntLittleEndianOnOneByte(in);
      LOG.debug("bit width {}", bitWidth);
      ByteBuffer buf = in.slice(in.available());
      decoder = new RunLengthBitPackingHybridDecoder(bitWidth, buf);
    } else {
      decoder = new RunLengthBitPackingHybridDecoder(1, ByteBuffer.allocate(0)) {
        @Override
        public int readInt() {
          throw new ParquetDecodingException("Attempt to read from empty page");
        }
      };
    }
  }

  @Override
  public int readValueDictionaryId() {
    return decoder.readInt();
  }

  @Override
  public Binary readBytes() {
    return dictionary.decodeToBinary(decoder.readInt());
  }

  @Override
  public float readFloat() {
    return dictionary.decodeToFloat(decoder.readInt());
  }

  @Override
  public double readDouble() {
    return dictionary.decodeToDouble(decoder.readInt());
  }

  @Override
  public int readInteger() {
    return dictionary.decodeToInt(decoder.readInt());
  }

  @Override
  public long readLong() {
    return dictionary.decodeToLong(decoder.readInt());
  }

  @Override
  public void skip() {
    decoder.readInt(); // Type does not matter as we are just skipping dictionary keys
  }
}
