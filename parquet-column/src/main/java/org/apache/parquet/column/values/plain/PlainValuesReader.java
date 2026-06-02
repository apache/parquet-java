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
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plain encoding for float, double, int, long.
 *
 * <p>Reads directly from a {@link ByteBuffer} with {@link ByteOrder#LITTLE_ENDIAN} byte order,
 * bypassing the deprecated {@code LittleEndianDataInputStream} wrapper to avoid per-value virtual
 * dispatch overhead. The underlying page data is obtained as a single contiguous {@link ByteBuffer}
 * via {@link ByteBufferInputStream#slice(int)}.
 */
public abstract class PlainValuesReader extends ValuesReader {
  private static final Logger LOG = LoggerFactory.getLogger(PlainValuesReader.class);

  private static final ByteBuffer EMPTY_LE_BUFFER =
      ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN).asReadOnlyBuffer();

  protected ByteBuffer buffer;

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream stream) throws IOException {
    LOG.debug("init from page at offset {} for length {}", stream.position(), stream.available());
    int available = stream.available();
    if (available > 0) {
      this.buffer = stream.slice(available).order(ByteOrder.LITTLE_ENDIAN);
    } else {
      this.buffer = EMPTY_LE_BUFFER.duplicate();
    }
  }

  @Override
  public void skip() {
    skip(1);
  }

  public static class DoublePlainValuesReader extends PlainValuesReader {

    @Override
    public void skip(int n) {
      try {
        buffer.position(buffer.position() + Math.multiplyExact(n, 8));
      } catch (RuntimeException e) {
        throw new ParquetDecodingException("could not skip " + n + " double values", e);
      }
    }

    @Override
    public double readDouble() {
      try {
        return buffer.getDouble();
      } catch (RuntimeException e) {
        throw new ParquetDecodingException("could not read double", e);
      }
    }
  }

  public static class FloatPlainValuesReader extends PlainValuesReader {

    @Override
    public void skip(int n) {
      try {
        buffer.position(buffer.position() + Math.multiplyExact(n, 4));
      } catch (RuntimeException e) {
        throw new ParquetDecodingException("could not skip " + n + " float values", e);
      }
    }

    @Override
    public float readFloat() {
      try {
        return buffer.getFloat();
      } catch (RuntimeException e) {
        throw new ParquetDecodingException("could not read float", e);
      }
    }
  }

  public static class IntegerPlainValuesReader extends PlainValuesReader {

    @Override
    public void skip(int n) {
      try {
        buffer.position(buffer.position() + Math.multiplyExact(n, 4));
      } catch (RuntimeException e) {
        throw new ParquetDecodingException("could not skip " + n + " int values", e);
      }
    }

    @Override
    public int readInteger() {
      try {
        return buffer.getInt();
      } catch (RuntimeException e) {
        throw new ParquetDecodingException("could not read int", e);
      }
    }
  }

  public static class LongPlainValuesReader extends PlainValuesReader {

    @Override
    public void skip(int n) {
      try {
        buffer.position(buffer.position() + Math.multiplyExact(n, 8));
      } catch (RuntimeException e) {
        throw new ParquetDecodingException("could not skip " + n + " long values", e);
      }
    }

    @Override
    public long readLong() {
      try {
        return buffer.getLong();
      } catch (RuntimeException e) {
        throw new ParquetDecodingException("could not read long", e);
      }
    }
  }
}
