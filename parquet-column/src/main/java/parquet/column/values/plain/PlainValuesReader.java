/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.column.values.plain;

import static parquet.Log.DEBUG;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import parquet.Log;
import parquet.bytes.LittleEndianDataInputStream;
import parquet.column.values.ValuesReader;
import parquet.io.ParquetDecodingException;

/**
 * Plain encoding except for booleans
 *
 * @author Julien Le Dem
 *
 */
public class PlainValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(PlainValuesReader.class);

  private LittleEndianDataInputStream in;

  @Override
  public float readFloat() {
    try {
      return in.readFloat();
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read float", e);
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

  @Override
  public int readInteger() {
    try {
      return in.readInt();
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read int", e);
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

  @Override
  public int readByte() {
    try {
      return in.read();
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read byte", e);
    }
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.values.ValuesReader#initFromPage(byte[], int)
   */
  @Override
  public int initFromPage(long valueCount, byte[] in, int offset) throws IOException {
    if (DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (in.length - offset));
    this.in = new LittleEndianDataInputStream(new ByteArrayInputStream(in, offset, in.length - offset));
    return in.length;
  }

}