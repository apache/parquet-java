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
import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.column.Encoding;
import parquet.column.page.DictionaryPage;
import parquet.column.values.ValuesWriter;
import parquet.column.values.dictionary.IntList.IntIterator;
import parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import parquet.column.values.plain.PlainValuesWriter;
import parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import parquet.io.ParquetEncodingException;
import parquet.io.api.Binary;

/**
 * Will attempt to encode values using a dictionary and fall back to plain encoding
 *  if the dictionary gets too big
 *
 * @author Julien Le Dem
 *
 */
public abstract class DictionaryValuesWriter extends ValuesWriter {
  private static final Log LOG = Log.getLog(DictionaryValuesWriter.class);

  /* max entries allowed for the dictionary will fail over to plain encoding if reached */
  private static final int MAX_DICTIONARY_ENTRIES = Integer.MAX_VALUE - 1;

  /* maximum size in bytes allowed for the dictionary will fail over to plain encoding if reached */
  protected final int maxDictionaryByteSize;

  /* contains the values encoded in plain if the dictionary grows too big */
  protected final ValuesWriter plainValuesWriter;

  /* will become true if the dictionary becomes too big */
  protected boolean dictionaryTooBig;

  /* current size in bytes the dictionary will take once serialized */
  protected int dictionaryByteSize;

  /* size in bytes of the dictionary at the end of last dictionary encoded page (in case the current page falls back to PLAIN) */
  protected int lastUsedDictionaryByteSize;

  /* size in items of the dictionary at the end of last dictionary encoded page (in case the current page falls back to PLAIN) */
  protected int lastUsedDictionarySize;

  /* dictionary encoded values */
  protected IntList encodedValues = new IntList();

  /* size of raw data, even if dictionary is used, it will not have effect on raw data size, it is used to decide
   * if fall back to plain encoding is better by comparing rawDataByteSize with Encoded data size
   * It's also used in getBufferedSize, so the page will be written based on raw data size
   */
  protected long rawDataByteSize = 0;

  /** indicates if this is the first page being processed */
  protected boolean firstPage = true;

  /**
   * @param maxDictionaryByteSize
   * @param initialSize
   */
  protected DictionaryValuesWriter(int maxDictionaryByteSize, int initialSize) {
    this.maxDictionaryByteSize = maxDictionaryByteSize;
    this.plainValuesWriter = new PlainValuesWriter(initialSize);
  }

  /**
   * Construct a DictionaryValuesWriter for fixed-length byte array values.
   *
   * @param maxDictionaryByteSize
   * @param initialSize
   * @param fixedLength Fixed length for byte arrays
   */
  protected DictionaryValuesWriter(int maxDictionaryByteSize, int initialSize, int fixedLength) {
    this.maxDictionaryByteSize = maxDictionaryByteSize;
    this.plainValuesWriter = new FixedLenByteArrayPlainValuesWriter(fixedLength, initialSize);
  }

  /**
   * check the size constraints of the dictionary and fail over to plain values encoding if threshold reached
   */
  protected void checkAndFallbackIfNeeded() {
    if (dictionaryByteSize > maxDictionaryByteSize || getDictionarySize() > MAX_DICTIONARY_ENTRIES) {
      // if the dictionary reaches the max byte size or the values can not be encoded on 4 bytes anymore.
      fallBackToPlainEncoding();
    }
  }

  private void fallBackToPlainEncoding() {
    if (DEBUG)
      LOG.debug("dictionary is now too big, falling back to plain: " + dictionaryByteSize + "B and " + getDictionarySize() + " entries");
    dictionaryTooBig = true;
    fallBackDictionaryEncodedData();
    if (lastUsedDictionarySize == 0) {
      // if we never used the dictionary
      // we free dictionary encoded data
      clearDictionaryContent();
      dictionaryByteSize = 0;
      encodedValues = new IntList();
    }
  }

  protected abstract void fallBackDictionaryEncodedData();

  @Override
  public long getBufferedSize() {
    // use raw data size to decide if we want to flush the page
    // so the acutual size of the page written could be much more smaller
    // due to dictionary encoding. This prevents page being to big when fallback happens.
    return rawDataByteSize;
  }

  @Override
  public long getAllocatedSize() {
    // size used in memory
    return encodedValues.size() * 4 + dictionaryByteSize + plainValuesWriter.getAllocatedSize();
  }

  @Override
  public BytesInput getBytes() {
    if (!dictionaryTooBig && getDictionarySize() > 0) {
      int maxDicId = getDictionarySize() - 1;
      if (DEBUG) LOG.debug("max dic id " + maxDicId);
      int bitWidth = BytesUtils.getWidthFromMaxInt(maxDicId);

      // TODO: what is a good initialCapacity?
      RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(bitWidth, 64 * 1024);
      IntIterator iterator = encodedValues.iterator();
      try {
        while (iterator.hasNext()) {
          encoder.writeInt(iterator.next());
        }
        // encodes the bit width
        byte[] bytesHeader = new byte[] { (byte) bitWidth };
        BytesInput rleEncodedBytes = encoder.toBytes();
        if (DEBUG) LOG.debug("rle encoded bytes " + rleEncodedBytes.size());
        BytesInput bytes = concat(BytesInput.from(bytesHeader), rleEncodedBytes);
        if (firstPage && ((bytes.size() + dictionaryByteSize) > rawDataByteSize)) {
          fallBackToPlainEncoding();
        } else {
          // remember size of dictionary when we last wrote a page
          lastUsedDictionarySize = getDictionarySize();
          lastUsedDictionaryByteSize = dictionaryByteSize;
          return bytes;
        }
      } catch (IOException e) {
        throw new ParquetEncodingException("could not encode the values", e);
      }
    }
    return plainValuesWriter.getBytes();
  }

  @Override
  public Encoding getEncoding() {
    firstPage = false;
    if (!dictionaryTooBig && getDictionarySize() > 0) {
      return PLAIN_DICTIONARY;
    }
    return plainValuesWriter.getEncoding();
  }

  @Override
  public void reset() {
    encodedValues = new IntList();
    plainValuesWriter.reset();
    rawDataByteSize = 0;
  }

  @Override
  public void resetDictionary() {
    lastUsedDictionaryByteSize = 0;
    lastUsedDictionarySize = 0;
    dictionaryTooBig = false;
    clearDictionaryContent();
  }

  /**
   * clear/free the underlying dictionary content
   */
  protected abstract void clearDictionaryContent();

  /**
   * @return size in items
   */
  protected abstract int getDictionarySize();

  @Override
  public String memUsageString(String prefix) {
    return String.format(
        "%s DictionaryValuesWriter{\n%s\n%s\n%s\n%s}\n",
        prefix,
        plainValuesWriter.
        memUsageString(prefix + " plain:"),
        prefix + " dict:" + dictionaryByteSize,
        prefix + " values:" + String.valueOf(encodedValues.size() * 4),
        prefix
        );
  }

  /**
   *
   */
  public static class PlainBinaryDictionaryValuesWriter extends DictionaryValuesWriter {

    /* type specific dictionary content */
    protected Object2IntMap<Binary> binaryDictionaryContent = new Object2IntLinkedOpenHashMap<Binary>();

    /**
     * @param maxDictionaryByteSize
     * @param initialSize
     */
    public PlainBinaryDictionaryValuesWriter(int maxDictionaryByteSize, int initialSize) {
      super(maxDictionaryByteSize, initialSize);
      binaryDictionaryContent.defaultReturnValue(-1);
    }

    /**
     * Constructor only used by subclasses for fixed-length byte arrays.
     */
    protected PlainBinaryDictionaryValuesWriter(int maxDictionaryByteSize, int initialSize, int length) {
      super(maxDictionaryByteSize, initialSize, length);
      binaryDictionaryContent.defaultReturnValue(-1);
    }

    @Override
    public void writeBytes(Binary v) {
      if (!dictionaryTooBig) {
        int id = binaryDictionaryContent.getInt(v);
        if (id == -1) {
          id = binaryDictionaryContent.size();
          binaryDictionaryContent.put(copy(v), id);
          // length as int (4 bytes) + actual bytes
          dictionaryByteSize += 4 + v.length();
        }
        encodedValues.add(id);
        checkAndFallbackIfNeeded();
      } else {
        plainValuesWriter.writeBytes(v);
      }
      //for rawdata, length(4 bytes int) is stored, followed by the binary content itself
      rawDataByteSize += v.length() + 4;
    }

    @Override
    public DictionaryPage createDictionaryPage() {
      if (lastUsedDictionarySize > 0) {
        // return a dictionary only if we actually used it
        PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize);
        Iterator<Binary> binaryIterator = binaryDictionaryContent.keySet().iterator();
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          Binary entry = binaryIterator.next();
          dictionaryEncoder.writeBytes(entry);
        }
        return new DictionaryPage(dictionaryEncoder.getBytes(), lastUsedDictionarySize, PLAIN_DICTIONARY);
      }
      return plainValuesWriter.createDictionaryPage();
    }

    @Override
    public int getDictionarySize() {
      return binaryDictionaryContent.size();
    }

    @Override
    protected void clearDictionaryContent() {
      binaryDictionaryContent.clear();
    }

    @Override
    protected void fallBackDictionaryEncodedData() {
      //build reverse dictionary
      Binary[] reverseDictionary = new Binary[getDictionarySize()];
      for (Object2IntMap.Entry<Binary> entry : binaryDictionaryContent.object2IntEntrySet()) {
        reverseDictionary[entry.getIntValue()] = entry.getKey();
      }

      //fall back to plain encoding
      IntIterator iterator = encodedValues.iterator();
      while (iterator.hasNext()) {
        int id = iterator.next();
        plainValuesWriter.writeBytes(reverseDictionary[id]);
      }
    }

    protected static Binary copy(Binary binary) {
      return Binary.fromByteArray(
          Arrays.copyOf(binary.getBytes(), binary.length()));
    }
  }

  /**
   *
   */
  public static class PlainFixedLenArrayDictionaryValuesWriter extends PlainBinaryDictionaryValuesWriter {

    private final int length;

    /**
     * @param maxDictionaryByteSize
     * @param initialSize
     */
    public PlainFixedLenArrayDictionaryValuesWriter(int maxDictionaryByteSize, int initialSize, int length) {
      super(maxDictionaryByteSize, initialSize, length);
      this.length = length;
    }

    @Override
    public void writeBytes(Binary value) {
      if (!dictionaryTooBig) {
        int id = binaryDictionaryContent.getInt(value);
        if (id == -1) {
          id = binaryDictionaryContent.size();
          binaryDictionaryContent.put(copy(value), id);
          dictionaryByteSize += length;
        }
        encodedValues.add(id);
        checkAndFallbackIfNeeded();
      } else {
        plainValuesWriter.writeBytes(value);
      }
      rawDataByteSize += length;
    }

    @Override
    public DictionaryPage createDictionaryPage() {
      if (lastUsedDictionarySize > 0) {
        // return a dictionary only if we actually used it
        FixedLenByteArrayPlainValuesWriter dictionaryEncoder = new FixedLenByteArrayPlainValuesWriter(12, lastUsedDictionaryByteSize);
        Iterator<Binary> binaryIterator = binaryDictionaryContent.keySet().iterator();
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          Binary entry = binaryIterator.next();
          dictionaryEncoder.writeBytes(entry);
        }
        return new DictionaryPage(dictionaryEncoder.getBytes(), lastUsedDictionarySize, PLAIN_DICTIONARY);
      }
      return plainValuesWriter.createDictionaryPage();
    }
  }

  /**
   *
   */
  public static class PlainLongDictionaryValuesWriter extends DictionaryValuesWriter {

    /* type specific dictionary content */
    private Long2IntMap longDictionaryContent = new Long2IntLinkedOpenHashMap();

    /**
     * @param maxDictionaryByteSize
     * @param initialSize
     */
    public PlainLongDictionaryValuesWriter(int maxDictionaryByteSize, int initialSize) {
      super(maxDictionaryByteSize, initialSize);
      longDictionaryContent.defaultReturnValue(-1);
    }

    @Override
    public void writeLong(long v) {
      if (!dictionaryTooBig) {
        int id = longDictionaryContent.get(v);
        if (id == -1) {
          id = longDictionaryContent.size();
          longDictionaryContent.put(v, id);
          dictionaryByteSize += 8;
        }
        encodedValues.add(id);
        checkAndFallbackIfNeeded();
      } else {
        plainValuesWriter.writeLong(v);
      }
      rawDataByteSize += 8;
    }

    @Override
    public DictionaryPage createDictionaryPage() {
      if (lastUsedDictionarySize > 0) {
        // return a dictionary only if we actually used it
        PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize);
        LongIterator longIterator = longDictionaryContent.keySet().iterator();
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          dictionaryEncoder.writeLong(longIterator.nextLong());
        }
        return new DictionaryPage(dictionaryEncoder.getBytes(), lastUsedDictionarySize, PLAIN_DICTIONARY);
      }
      return plainValuesWriter.createDictionaryPage();
    }

    @Override
    public int getDictionarySize() {
      return longDictionaryContent.size();
    }

    @Override
    protected void clearDictionaryContent() {
      longDictionaryContent.clear();
    }

    @Override
    protected void fallBackDictionaryEncodedData() {
      //build reverse dictionary
      long[] reverseDictionary = new long[getDictionarySize()];
      ObjectIterator<Long2IntMap.Entry> entryIterator = longDictionaryContent.long2IntEntrySet().iterator();
      while (entryIterator.hasNext()) {
        Long2IntMap.Entry entry = entryIterator.next();
        reverseDictionary[entry.getIntValue()] = entry.getLongKey();
      }

      //fall back to plain encoding
      IntIterator iterator = encodedValues.iterator();
      while (iterator.hasNext()) {
        int id = iterator.next();
        plainValuesWriter.writeLong(reverseDictionary[id]);
      }
    }
  }

  /**
   *
   */
  public static class PlainDoubleDictionaryValuesWriter extends DictionaryValuesWriter {

    /* type specific dictionary content */
    private Double2IntMap doubleDictionaryContent = new Double2IntLinkedOpenHashMap();

    /**
     * @param maxDictionaryByteSize
     * @param initialSize
     */
    public PlainDoubleDictionaryValuesWriter(int maxDictionaryByteSize, int initialSize) {
      super(maxDictionaryByteSize, initialSize);
      doubleDictionaryContent.defaultReturnValue(-1);
    }

    @Override
    public void writeDouble(double v) {
      if (!dictionaryTooBig) {
        int id = doubleDictionaryContent.get(v);
        if (id == -1) {
          id = doubleDictionaryContent.size();
          doubleDictionaryContent.put(v, id);
          dictionaryByteSize += 8;
        }
        encodedValues.add(id);
        checkAndFallbackIfNeeded();
      } else {
        plainValuesWriter.writeDouble(v);
      }
      rawDataByteSize += 8;
    }

    @Override
    public DictionaryPage createDictionaryPage() {
      if (lastUsedDictionarySize > 0) {
        // return a dictionary only if we actually used it
        PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize);
        DoubleIterator doubleIterator = doubleDictionaryContent.keySet().iterator();
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          dictionaryEncoder.writeDouble(doubleIterator.nextDouble());
        }
        return new DictionaryPage(dictionaryEncoder.getBytes(), lastUsedDictionarySize, PLAIN_DICTIONARY);
      }
      return plainValuesWriter.createDictionaryPage();
    }

    @Override
    public int getDictionarySize() {
      return doubleDictionaryContent.size();
    }

    @Override
    protected void clearDictionaryContent() {
      doubleDictionaryContent.clear();
    }

    @Override
    protected void fallBackDictionaryEncodedData() {
      //build reverse dictionary
      double[] reverseDictionary = new double[getDictionarySize()];
      ObjectIterator<Double2IntMap.Entry> entryIterator = doubleDictionaryContent.double2IntEntrySet().iterator();
      while (entryIterator.hasNext()) {
        Double2IntMap.Entry entry = entryIterator.next();
        reverseDictionary[entry.getIntValue()] = entry.getDoubleKey();
      }

      //fall back to plain encoding
      IntIterator iterator = encodedValues.iterator();
      while (iterator.hasNext()) {
        int id = iterator.next();
        plainValuesWriter.writeDouble(reverseDictionary[id]);
      }
    }
  }

  /**
   *
   */
  public static class PlainIntegerDictionaryValuesWriter extends DictionaryValuesWriter {

    /* type specific dictionary content */
    private Int2IntMap intDictionaryContent = new Int2IntLinkedOpenHashMap();

    /**
     * @param maxDictionaryByteSize
     * @param initialSize
     */
    public PlainIntegerDictionaryValuesWriter(int maxDictionaryByteSize, int initialSize) {
      super(maxDictionaryByteSize, initialSize);
      intDictionaryContent.defaultReturnValue(-1);
    }

    @Override
    public void writeInteger(int v) {
      if (!dictionaryTooBig) {
        int id = intDictionaryContent.get(v);
        if (id == -1) {
          id = intDictionaryContent.size();
          intDictionaryContent.put(v, id);
          dictionaryByteSize += 4;
        }
        encodedValues.add(id);
        checkAndFallbackIfNeeded();
      } else {
        plainValuesWriter.writeInteger(v);
      }

      //Each integer takes 4 bytes as raw data(plain encoding)
      rawDataByteSize += 4;
    }

    @Override
    public DictionaryPage createDictionaryPage() {
      if (lastUsedDictionarySize > 0) {
        // return a dictionary only if we actually used it
        PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize);
        it.unimi.dsi.fastutil.ints.IntIterator intIterator = intDictionaryContent.keySet().iterator();
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          dictionaryEncoder.writeInteger(intIterator.nextInt());
        }
        return new DictionaryPage(dictionaryEncoder.getBytes(), lastUsedDictionarySize, PLAIN_DICTIONARY);
      }
      return plainValuesWriter.createDictionaryPage();
    }

    @Override
    public int getDictionarySize() {
      return intDictionaryContent.size();
    }

    @Override
    protected void clearDictionaryContent() {
      intDictionaryContent.clear();
    }

    @Override
    protected void fallBackDictionaryEncodedData() {
      //build reverse dictionary
      int[] reverseDictionary = new int[getDictionarySize()];
      ObjectIterator<Int2IntMap.Entry> entryIterator = intDictionaryContent.int2IntEntrySet().iterator();
      while (entryIterator.hasNext()) {
        Int2IntMap.Entry entry = entryIterator.next();
        reverseDictionary[entry.getIntValue()] = entry.getIntKey();
      }

      //fall back to plain encoding
      IntIterator iterator = encodedValues.iterator();
      while (iterator.hasNext()) {
        int id = iterator.next();
        plainValuesWriter.writeInteger(reverseDictionary[id]);
      }
    }
  }

  /**
   *
   */
  public static class PlainFloatDictionaryValuesWriter extends DictionaryValuesWriter {

    /* type specific dictionary content */
    private Float2IntMap floatDictionaryContent = new Float2IntLinkedOpenHashMap();

    /**
     * @param maxDictionaryByteSize
     * @param initialSize
     */
    public PlainFloatDictionaryValuesWriter(int maxDictionaryByteSize, int initialSize) {
      super(maxDictionaryByteSize, initialSize);
      floatDictionaryContent.defaultReturnValue(-1);
    }

    @Override
    public void writeFloat(float v) {
      if (!dictionaryTooBig) {
        int id = floatDictionaryContent.get(v);
        if (id == -1) {
          id = floatDictionaryContent.size();
          floatDictionaryContent.put(v, id);
          dictionaryByteSize += 4;
        }
        encodedValues.add(id);
        checkAndFallbackIfNeeded();
      } else {
        plainValuesWriter.writeFloat(v);
      }
      rawDataByteSize += 4;
    }

    @Override
    public DictionaryPage createDictionaryPage() {
      if (lastUsedDictionarySize > 0) {
        // return a dictionary only if we actually used it
        PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize);
        FloatIterator floatIterator = floatDictionaryContent.keySet().iterator();
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          dictionaryEncoder.writeFloat(floatIterator.nextFloat());
        }
        return new DictionaryPage(dictionaryEncoder.getBytes(), lastUsedDictionarySize, PLAIN_DICTIONARY);
      }
      return plainValuesWriter.createDictionaryPage();
    }

    @Override
    public int getDictionarySize() {
      return floatDictionaryContent.size();
    }

    @Override
    protected void clearDictionaryContent() {
      floatDictionaryContent.clear();
    }

    @Override
    protected void fallBackDictionaryEncodedData() {
      //build reverse dictionary
      float[] reverseDictionary = new float[getDictionarySize()];
      ObjectIterator<Float2IntMap.Entry> entryIterator = floatDictionaryContent.float2IntEntrySet().iterator();
      while (entryIterator.hasNext()) {
        Float2IntMap.Entry entry = entryIterator.next();
        reverseDictionary[entry.getIntValue()] = entry.getFloatKey();
      }

      //fall back to plain encoding
      IntIterator iterator = encodedValues.iterator();
      while (iterator.hasNext()) {
        int id = iterator.next();
        plainValuesWriter.writeFloat(reverseDictionary[id]);
      }
    }
  }

}
