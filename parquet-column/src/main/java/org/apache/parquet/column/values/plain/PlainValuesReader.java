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

import static org.apache.parquet.Log.DEBUG;

import java.io.IOException;
import java.nio.ByteBuffer;

import parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.Log;
import org.apache.parquet.bytes.LittleEndianDataInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * Plain encoding for float, double, int, long
 *
 * @author Julien Le Dem
 *
 */
abstract public class PlainValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(PlainValuesReader.class);

  protected LittleEndianDataInputStream in;

  /**
   * {@inheritDoc}
   * @see org.apache.parquet.column.values.ValuesReader#initFromPage(int, byte[], int)
   */
  @Override
  public void initFromPage(int valueCount, ByteBuffer in, int offset) throws IOException {
    if (DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (in.limit() - offset));
    this.in = new LittleEndianDataInputStream(new ByteBufferInputStream(in.duplicate(), offset, in.limit() - offset));
  }
  
  @Override
  public void initFromPage(int valueCount, byte[] page, int offset) throws IOException{
    this.initFromPage(valueCount, ByteBuffer.wrap(page), offset);
  }

  public static class DoublePlainValuesReader extends PlainValuesReader {

    @Override
    public void skip() {
      try {
        in.skipBytes(8);
      } catch (IOException e) {
        throw new ParquetDecodingException("could not skip double", e);
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
    public void skip() {
      try {
        in.skipBytes(4);
      } catch (IOException e) {
        throw new ParquetDecodingException("could not skip float", e);
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
    public void skip() {
      try {
        in.skipBytes(4);
      } catch (IOException e) {
        throw new ParquetDecodingException("could not skip int", e);
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
    public void skip() {
      try {
        in.skipBytes(8);
      } catch (IOException e) {
        throw new ParquetDecodingException("could not skip long", e);
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
