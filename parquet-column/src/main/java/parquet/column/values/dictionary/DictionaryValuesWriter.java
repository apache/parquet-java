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
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import it.unimi.dsi.fastutil.doubles.Double2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.doubles.Double2IntMap;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.floats.Float2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2IntMap;
import it.unimi.dsi.fastutil.floats.FloatIterator;
import it.unimi.dsi.fastutil.ints.Int2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.LongIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.column.Encoding;
import parquet.column.page.DictionaryPage;
import parquet.column.values.ValuesWriter;
import parquet.column.values.dictionary.IntList.IntIterator;
import parquet.column.values.plain.PlainValuesWriter;
import parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import parquet.io.ParquetEncodingException;
import parquet.io.api.Binary;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * Will attempt to encode values using a dictionary and fall back to plain encoding
 *  if the dictionary gets too big
 *
 * @author Julien Le Dem
 *
 */
public class DictionaryValuesWriter extends ValuesWriter {
  private static final Log LOG = Log.getLog(DictionaryValuesWriter.class);

  /* max entries allowed for the dictionary will fail over to plain encoding if reached */
  private static final int MAX_DICTIONARY_ENTRIES = 65535 /* 2^16 - 1 */;

  /* maximum size in bytes allowed for the dictionary will fail over to plain encoding if reached */
  private final int maxDictionaryByteSize;

  /* contains the values encoded in plain if the dictionary grows too big */
  private final PlainValuesWriter plainValuesWriter;

  /* will become true if the dictionary becomes too big */
  private boolean dictionaryTooBig;

  /* current size in bytes the dictionary will take once serialized */
  private int dictionaryByteSize;

  /* size in bytes of the dictionary at the end of last dictionary encoded page (in case the current page falls back to PLAIN) */
  private int lastUsedDictionaryByteSize;

  /* size in items of the dictionary at the end of last dictionary encoded page (in case the current page falls back to PLAIN) */
  private int lastUsedDictionarySize;

  /* underlying dictionary type */
  private PrimitiveTypeName dictionaryType;

  /* type specific dictionary content */
  private Map<Binary, Integer> binaryDictionaryContent;
  private Long2IntMap longDictionaryContent;
  private Double2IntMap doubleDictionaryContent;
  private Float2IntMap floatDictionaryContent;
  private Int2IntMap intDictionaryContent;

  /* dictionary encoded values */
  private IntList encodedValues = new IntList();

  /**
   * 
   * @param dictionaryType the value type to attempt to dictionary encode
   * @param maxDictionaryByteSize
   * @param initialSize
   */
  public DictionaryValuesWriter(PrimitiveTypeName dictionaryType, int maxDictionaryByteSize, int initialSize) {
    this.dictionaryType = dictionaryType;
    this.maxDictionaryByteSize = maxDictionaryByteSize;
    this.plainValuesWriter = new PlainValuesWriter(initialSize);
    resetDictionary();
  }

  /*
   * check the size constraints of the dictionary and fail over to plain values encoding if threshold reached
   */
  private void checkAndFallbackIfNeeded() {
    if (dictionaryByteSize > maxDictionaryByteSize || getDictionarySize() > MAX_DICTIONARY_ENTRIES) {
      // if the dictionary reaches the max byte size or the values can not be encoded on two bytes anymore.
      if (DEBUG)
        LOG.debug("dictionary is now too big, falling back to plain: " + dictionaryByteSize + "B and " + getDictionarySize() + " entries");
      dictionaryTooBig = true;
      if (lastUsedDictionarySize == 0) {
        // if we never used the dictionary
        // we free dictionary encoded data
        binaryDictionaryContent = null;
        longDictionaryContent = null;
        doubleDictionaryContent = null;
        floatDictionaryContent = null;
        intDictionaryContent = null;
        dictionaryByteSize = 0;
        encodedValues = null;
      }
    }
  }

  @Override
  public void writeBytes(Binary v) {
    if (dictionaryType != BINARY) {
      throw new UnsupportedOperationException("cannot writeBytes on a " + dictionaryType + " dictionary encoder");
    }
    if (!dictionaryTooBig) {
      Integer id = binaryDictionaryContent.get(v);
      if (id == null) {
        id = binaryDictionaryContent.size();
        binaryDictionaryContent.put(v, id);
        // length as int (2 bytes) + actual bytes
        dictionaryByteSize += 2 + v.length();
      }
      encodedValues.add(id);
      checkAndFallbackIfNeeded();
    }
    // write also to plain encoding if we need to fall back
    plainValuesWriter.writeBytes(v);
  }

  @Override
  public void writeInteger(int v) {
    if (dictionaryType != INT32) {
      throw new UnsupportedOperationException("cannot writeInteger on a " + dictionaryType + " dictionary encoder");
    }
    if (!dictionaryTooBig) {
      int id = intDictionaryContent.get(v);
      if (id == -1) {
        id = intDictionaryContent.size();
        intDictionaryContent.put(v, id);
        dictionaryByteSize += 4;
      }
      encodedValues.add(id);
      checkAndFallbackIfNeeded();
    }
    // write also to plain encoding if we need to fall back
    plainValuesWriter.writeInteger(v);
  }

  @Override
  public void writeLong(long v) {
    if (dictionaryType != INT64) {
      throw new UnsupportedOperationException("cannot writeLong on a " + dictionaryType + " dictionary encoder");
    }
    if (!dictionaryTooBig) {
      int id = longDictionaryContent.get(v);
      if (id == -1) {
        id = longDictionaryContent.size();
        longDictionaryContent.put(v, id);
        dictionaryByteSize += 8;
      }
      encodedValues.add(id);
      checkAndFallbackIfNeeded();
    }
    // write also to plain encoding if we need to fall back
    plainValuesWriter.writeLong(v);
  }

  @Override
  public void writeDouble(double v) {
    if (dictionaryType != DOUBLE) {
      throw new UnsupportedOperationException("cannot writeDouble on a " + dictionaryType + " dictionary encoder");
    }
    if (!dictionaryTooBig) {
      int id = doubleDictionaryContent.get(v);
      if (id == -1) {
        id = doubleDictionaryContent.size();
        doubleDictionaryContent.put(v, id);
        dictionaryByteSize += 8;
      }
      encodedValues.add(id);
      checkAndFallbackIfNeeded();
    }
    // write also to plain encoding if we need to fall back
    plainValuesWriter.writeDouble(v);
  }

  @Override
  public void writeFloat(float v) {
    if (dictionaryType != FLOAT) {
      throw new UnsupportedOperationException("cannot writeFloat on a " + dictionaryType + " dictionary encoder");
    }
    if (!dictionaryTooBig) {
      int id = floatDictionaryContent.get(v);
      if (id == -1) {
        id = floatDictionaryContent.size();
        floatDictionaryContent.put(v, id);
        dictionaryByteSize += 4;
      }
      encodedValues.add(id);
      checkAndFallbackIfNeeded();
    }
    // write also to plain encoding if we need to fall back
    plainValuesWriter.writeFloat(v);
  }

  @Override
  public long getBufferedSize() {
    // size that will be written to a page
    // not including the dictionary size
    return dictionaryTooBig ? plainValuesWriter.getBufferedSize() : encodedValues.size() * 4;
  }

  @Override
  public long getAllocatedSize() {
    // size used in memory
    return (encodedValues == null ? 0 : encodedValues.size() * 4) + dictionaryByteSize + plainValuesWriter.getAllocatedSize();
  }

  @Override
  public BytesInput getBytes() {
    if (!dictionaryTooBig && getDictionarySize() > 0) {
      // remember size of dictionary when we last wrote a page
      lastUsedDictionarySize = getDictionarySize();
      lastUsedDictionaryByteSize = dictionaryByteSize;
      int maxDicId = getDictionarySize() - 1;
      if (DEBUG)
        LOG.debug("max dic id " + maxDicId);
      int bitWidth = BytesUtils.getWidthFromMaxInt(maxDicId);

      // TODO: what is a good initialCapacity?
      final RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(bitWidth,
          64 * 1024);
      IntIterator iterator = encodedValues.iterator();
      try {
        while (iterator.hasNext()) {
          encoder.writeInt(iterator.next());
        }
        // encodes the bit width
        byte[] bytesHeader = new byte[] { (byte) bitWidth };
        BytesInput rleEncodedBytes = encoder.toBytes();
        if (DEBUG)
          LOG.debug("rle encoded bytes " + rleEncodedBytes.size());
        return concat(BytesInput.from(bytesHeader), rleEncodedBytes);
      } catch (IOException e) {
        throw new ParquetEncodingException("could not encode the values", e);
      }
    }
    return plainValuesWriter.getBytes();
  }

  @Override
  public Encoding getEncoding() {
    if (!dictionaryTooBig && getDictionarySize() > 0) {
      return PLAIN_DICTIONARY;
    }
    return plainValuesWriter.getEncoding();
  }

  @Override
  public void reset() {
    if (encodedValues != null) {
      encodedValues = new IntList();
    }
    plainValuesWriter.reset();
  }

  @Override
  public DictionaryPage createDictionaryPage() {
    if (lastUsedDictionarySize > 0) {
      // return a dictionary only if we actually used it
      PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize);
      switch (dictionaryType) {
      case BINARY:
        Iterator<Binary> binaryIterator = binaryDictionaryContent.keySet().iterator();
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          Binary entry = binaryIterator.next();
          dictionaryEncoder.writeBytes(entry);
        }
        break;
      case INT64:
        LongIterator longIterator = longDictionaryContent.keySet().iterator();
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          dictionaryEncoder.writeLong(longIterator.nextLong());
        }
        break;
      case DOUBLE:
        DoubleIterator doubleIterator = doubleDictionaryContent.keySet().iterator();
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          dictionaryEncoder.writeDouble(doubleIterator.nextDouble());
        }
        break;
      case FLOAT:
        FloatIterator floatIterator = floatDictionaryContent.keySet().iterator();
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          dictionaryEncoder.writeFloat(floatIterator.nextFloat());
        }
        break;
      case INT32:
        it.unimi.dsi.fastutil.ints.IntIterator intIterator = intDictionaryContent.keySet().iterator();
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          dictionaryEncoder.writeInteger(intIterator.nextInt());
        }
        break;
      default:
        throw new ParquetEncodingException("failed to generate dictionary page for type " + dictionaryType);
      }
      return new DictionaryPage(dictionaryEncoder.getBytes(), lastUsedDictionarySize, PLAIN_DICTIONARY);
    }
    return plainValuesWriter.createDictionaryPage();
  }

  @Override
  public void resetDictionary() {
    lastUsedDictionaryByteSize = 0;
    lastUsedDictionarySize = 0;
    dictionaryByteSize = 0;
    dictionaryTooBig = false;
    switch (dictionaryType) {
    case BINARY:
      if (binaryDictionaryContent == null) {
        binaryDictionaryContent = new LinkedHashMap<Binary, Integer>();
      } else {
        binaryDictionaryContent.clear();
      }
      break;
    case INT64:
      if (longDictionaryContent == null) {
        longDictionaryContent = new Long2IntLinkedOpenHashMap();
        longDictionaryContent.defaultReturnValue(-1);
      } else {
        longDictionaryContent.clear();
      }
      break;
    case DOUBLE:
      if (doubleDictionaryContent == null) {
        doubleDictionaryContent = new Double2IntLinkedOpenHashMap();
        doubleDictionaryContent.defaultReturnValue(-1);
      } else {
        doubleDictionaryContent.clear();
      }
      break;
    case FLOAT:
      if (floatDictionaryContent == null) {
        floatDictionaryContent = new Float2IntLinkedOpenHashMap();
        floatDictionaryContent.defaultReturnValue(-1);
      } else {
        floatDictionaryContent.clear();
      }
      break;
    case INT32:
      if (intDictionaryContent == null) {
        intDictionaryContent = new Int2IntLinkedOpenHashMap();
        intDictionaryContent.defaultReturnValue(-1);
      } else {
        intDictionaryContent.clear();
      }
      break;
    default:
      throw new UnsupportedOperationException("dictionay encoding not currently supported for type " + dictionaryType);
    }

  }

  public int getDictionaryByteSize() {
    return dictionaryByteSize;
  }

  public int getDictionarySize() {
    switch (dictionaryType) {
    case BINARY:
      return binaryDictionaryContent.size();
    case INT64:
      return longDictionaryContent.size();
    case INT32:
      return intDictionaryContent.size();
    case DOUBLE:
      return doubleDictionaryContent.size();
    case FLOAT:
      return floatDictionaryContent.size();
    default:
      return 0;
    }
  }

  @Override
  public String memUsageString(String prefix) {
    return String.format("%s DictionaryValuesWriter{\n%s\n%s\n%s\n%s}\n", prefix, plainValuesWriter.memUsageString(prefix + " plain:"),
        prefix + " dict:" + dictionaryByteSize, prefix + " values:" + (encodedValues.size() * 4), prefix);
  }
}
