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
package parquet.column.values.dictionary;

import static parquet.Log.DEBUG;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import parquet.Log;
import parquet.bytes.BytesUtils;
import parquet.column.Dictionary;
import parquet.column.values.ValuesReader;
import parquet.column.values.rle.RLEDecoder;
import parquet.io.ParquetDecodingException;
import parquet.io.api.Binary;

/**
 * Reads values that have been dictionary encoded
 *
 * @author Julien Le Dem
 *
 */
public class DictionaryValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(DictionaryValuesReader.class);

  private InputStream in;

  private Dictionary dictionary;

  private RLEDecoder decoder;

  public DictionaryValuesReader(Dictionary dictionary) {
    this.dictionary = dictionary;
  }

  @Override
  public int initFromPage(long valueCount, byte[] page, int offset)
      throws IOException {
    if (DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (page.length - offset));
    this.in = new ByteArrayInputStream(page, offset, page.length - offset);
    int bitWidth = BytesUtils.readIntLittleEndianOnOneByte(in);
    if (DEBUG) LOG.debug("bit width " + bitWidth);
    decoder = new RLEDecoder(bitWidth, in);
    return page.length;
  }

  @Override
  public int readValueDictionaryId() {
    try {
      return decoder.readInt();
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }

  @Override
  public Binary readBytes() {
    try {
      return dictionary.decodeToBinary(decoder.readInt());
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }

}
