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
import static parquet.bytes.BytesInput.concat;
import static parquet.column.Encoding.PLAIN_DICTIONARY;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.bytes.LittleEndianDataOutputStream;
import parquet.column.Encoding;
import parquet.column.page.DictionaryPage;
import parquet.column.values.ValuesWriter;
import parquet.column.values.dictionary.IntList.IntIterator;
import parquet.column.values.plain.PlainValuesWriter;
import parquet.column.values.rle.RLESimpleEncoder;
import parquet.io.ParquetEncodingException;
import parquet.io.api.Binary;

/**
 * Will attempt to encode values using a dictionary and fall back to plain encoding
 *  if the dictionary gets too big
 *
 * @author Julien Le Dem
 *
 */
public class DictionaryValuesWriter extends ValuesWriter {
  private static final Log LOG = Log.getLog(DictionaryValuesWriter.class);

  private static final int MAX_DICTIONARY_ENTRIES = 65535 /* 2^16 - 1 */;

  /**
   * maximum size in bytes allowed for the dictionary
   * will fail over to plain encoding if reached
   */
  private final int maxDictionaryByteSize;

  /**
   * contains the values encoded in plain if the dictionary grows too big
   */
  private final PlainValuesWriter plainValuesWriter;

  /**
   * will become true if the dictionary becomes too big
   */
  private boolean dictionaryTooBig;

  /**
   * current size in bytes the dictionary will take once serialized
   */
  private int dictionaryByteSize;

  /**
   * size in bytes of the dictionary at the end of last dictionary encoded page (in case the current page falls back to PLAIN)
   */
  private int lastUsedDictionaryByteSize;

  /**
   * size in items of the dictionary at the end of last dictionary encoded page (in case the current page falls back to PLAIN)
   */
  private int lastUsedDictionarySize;

  /**
   * dictionary
   */
  private Map<Binary, Integer> dict;

  /**
   * dictionary encoded values
   */
  private IntList out = new IntList();

  public DictionaryValuesWriter(int maxDictionaryByteSize, int initialSize) {
    this.maxDictionaryByteSize = maxDictionaryByteSize;
    this.plainValuesWriter = new PlainValuesWriter(initialSize);
    resetDictionary();
  }

  @Override
  public void writeBytes(Binary v) {
    if (!dictionaryTooBig) {
      writeBytesUsingDict(v);
      if (dictionaryByteSize > maxDictionaryByteSize || dict.size() > MAX_DICTIONARY_ENTRIES) {
        // if the dictionary reaches the max byte size or the values can not be encoded on two bytes anymore.
        if (DEBUG) LOG.debug("dictionary is now too big, falling back to plain: " + dictionaryByteSize + "B and " + dict.size() + " entries");
        dictionaryTooBig = true;
        if (lastUsedDictionarySize == 0) {
          // if we never used the dictionary
          // we free dictionary encoded data
          dict = null;
          dictionaryByteSize = 0;
          out = null;
        }
      }
    }
    // write also to plain encoding if we need to fall back
    plainValuesWriter.writeBytes(v);
  }

  /**
   * will add an entry to the dictionary if the value is new
   * @param v the value to dictionary encode
   */
  private void writeBytesUsingDict(Binary v) {
    Integer id = dict.get(v);
    if (id == null) {
      id = dict.size();
      dict.put(v, id);
      // length as int (2 bytes) + actual bytes
      dictionaryByteSize += 2 + v.length();
    }
    out.add(id);
  }

  @Override
  public long getBufferedSize() {
    // size that will be written to a page
    // not including the dictionary size
    return dictionaryTooBig ? plainValuesWriter.getBufferedSize() : out.size() * 4;
  }

  @Override
  public long getAllocatedSize() {
    // size used in memory
    return (out == null ? 0 : out.size() * 4) + dictionaryByteSize + plainValuesWriter.getAllocatedSize();
  }

  @Override
  public BytesInput getBytes() {
    if (!dictionaryTooBig && dict.size() > 0) {
      // remember size of dictionary when we last wrote a page
      lastUsedDictionarySize = dict.size();
      lastUsedDictionaryByteSize = dictionaryByteSize;
      final int maxDicId = dict.size() - 1;
      if (DEBUG) LOG.debug("max dic id " + maxDicId);
      final RLESimpleEncoder rleSimpleEncoder = new RLESimpleEncoder(BytesUtils.getWidthFromMaxInt(maxDicId));
      final IntIterator iterator = out.iterator();
      try {
        while (iterator.hasNext()) {
          rleSimpleEncoder.writeInt(iterator.next());
        }
        // encodes the max dict id on 2 bytes (as it is < 64K)
        byte[] bytesHeader = intTo2Bytes(maxDicId);
        final BytesInput rleEncodedBytes = rleSimpleEncoder.toBytes();
        if (DEBUG) LOG.debug("rle encoded bytes " + rleEncodedBytes.size());
        return concat(BytesInput.from(bytesHeader), rleEncodedBytes);
      } catch (IOException e) {
        throw new ParquetEncodingException("could not encode the values", e);
      }
    }
    return plainValuesWriter.getBytes();
  }

  private byte[] intTo2Bytes(final int maxDicId) {
    byte[] bytesHeader = new byte[2];
    bytesHeader[0] = (byte)(maxDicId & 0xFF);
    bytesHeader[1] = (byte)((maxDicId >>>  8) & 0xFF);
    return bytesHeader;
  }

  @Override
  public Encoding getEncoding() {
    if (!dictionaryTooBig && dict.size() > 0) {
      return PLAIN_DICTIONARY;
    }
    return plainValuesWriter.getEncoding();
  }

  @Override
  public void reset() {
    if (out != null) {
      out = new IntList();
    }
    plainValuesWriter.reset();
  }

  @Override
  public DictionaryPage createDictionaryPage() {
    if (lastUsedDictionarySize > 0) {
      // return a dictionary only if we actually used it
      try {
        CapacityByteArrayOutputStream dictBuf = new CapacityByteArrayOutputStream(lastUsedDictionaryByteSize);
        LittleEndianDataOutputStream dictOut = new LittleEndianDataOutputStream(dictBuf);
        Iterator<Binary> entryIterator = dict.keySet().iterator();
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          Binary entry = entryIterator.next();
          dictOut.writeInt(entry.length());
          entry.writeTo(dictOut);
        }
        return new DictionaryPage(BytesInput.from(dictBuf), lastUsedDictionarySize, PLAIN_DICTIONARY);
      } catch (IOException e) {
        throw new ParquetEncodingException("Could not generate dictionary Page", e);
      }
    }
    return plainValuesWriter.createDictionaryPage();
  }

  @Override
  public void resetDictionary() {
    lastUsedDictionaryByteSize = 0;
    lastUsedDictionarySize = 0;
    dictionaryByteSize = 0;
    dictionaryTooBig = false;
    if (dict == null) {
      dict = new LinkedHashMap<Binary, Integer>();
    } else {
      dict.clear();
    }
  }

  public int getDictionaryByteSize() {
    return dictionaryByteSize;
  }
}
