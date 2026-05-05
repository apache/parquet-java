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
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.LittleEndianDataInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plain encoding for float, double, int, long
 */
public abstract class PlainValuesReader extends ValuesReader {
  private static final Logger LOG = LoggerFactory.getLogger(PlainValuesReader.class);

  protected LittleEndianDataInputStream in;

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream stream) throws IOException {
    LOG.debug("init from page at offset {} for length {}", stream.position(), stream.available());
    this.in = new LittleEndianDataInputStream(stream.remainingStream());
  }

  @Override
  public void skip() {
    skip(1);
  }

  void skipBytesFully(int n) throws IOException {
    int skipped = 0;
    while (skipped < n) {
      skipped += in.skipBytes(n - skipped);
    }
  }

  public static class DoublePlainValuesReader extends PlainValuesReader {

    @Override
    public void skip(int n) {
      try {
        skipBytesFully(n * 8);
      } catch (IOException e) {
        throw new ParquetDecodingException("could not skip " + n + " double values", e);
      }
    }

    @Override
    public double readDouble() {
      try {
        return in.readDouble();
      } catch (IOException e) {
        throw new ParquetDecodingException("could not read double", e);
      }
    }
  }

  public static class FloatPlainValuesReader extends PlainValuesReader {

    @Override
    public void skip(int n) {
      try {
        skipBytesFully(n * 4);
      } catch (IOException e) {
        throw new ParquetDecodingException("could not skip " + n + " floats", e);
      }
    }

    @Override
    public float readFloat() {
      try {
        return in.readFloat();
      } catch (IOException e) {
        throw new ParquetDecodingException("could not read float", e);
      }
    }
  }

  public static class IntegerPlainValuesReader extends PlainValuesReader {

    @Override
    public void skip(int n) {
      try {
        in.skipBytes(n * 4);
      } catch (IOException e) {
        throw new ParquetDecodingException("could not skip " + n + " ints", e);
      }
    }

    @Override
    public int readInteger() {
      try {
        return in.readInt();
      } catch (IOException e) {
        throw new ParquetDecodingException("could not read int", e);
      }
    }
  }

  public static class LongPlainValuesReader extends PlainValuesReader {

    @Override
    public void skip(int n) {
      try {
        in.skipBytes(n * 8);
      } catch (IOException e) {
        throw new ParquetDecodingException("could not skip " + n + " longs", e);
      }
    }

    @Override
    public long readLong() {
      try {
        return in.readLong();
      } catch (IOException e) {
        throw new ParquetDecodingException("could not read long", e);
      }
    }
  }
}
