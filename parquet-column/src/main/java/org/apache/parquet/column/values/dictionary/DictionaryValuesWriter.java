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

import static org.apache.parquet.bytes.BytesInput.concat;

import it.unimi.dsi.fastutil.doubles.Double2IntMap;
import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.Float2IntMap;
import it.unimi.dsi.fastutil.floats.Float2IntOpenHashMap;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.RequiresFallback;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.IntList.IntIterator;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.util.AutoCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Will attempt to encode values using a dictionary and fall back to plain encoding
 * if the dictionary gets too big
 */
public abstract class DictionaryValuesWriter extends ValuesWriter implements RequiresFallback {
  private static final Logger LOG = LoggerFactory.getLogger(DictionaryValuesWriter.class);

  /* max entries allowed for the dictionary will fail over to plain encoding if reached */
  private static final int MAX_DICTIONARY_ENTRIES = Integer.MAX_VALUE - 1;
  private static final int MIN_INITIAL_SLAB_SIZE = 64;

  /* encoding to label the data page */
  private final Encoding encodingForDataPage;

  /* encoding to label the dictionary page */
  protected final Encoding encodingForDictionaryPage;

  /* maximum size in bytes allowed for the dictionary will fail over to plain encoding if reached */
  protected final int maxDictionaryByteSize;

  /* will become true if the dictionary becomes too big */
  protected boolean dictionaryTooBig;

  /* current size in bytes the dictionary will take once serialized */
  protected long dictionaryByteSize;

  /* size in bytes of the dictionary at the end of last dictionary encoded page (in case the current page falls back to PLAIN) */
  protected int lastUsedDictionaryByteSize;

  /* size in items of the dictionary at the end of last dictionary encoded page (in case the current page falls back to PLAIN) */
  protected int lastUsedDictionarySize;

  /* dictionary encoded values */
  protected IntList encodedValues = new IntList();

  /**
   * indicates if this is the first page being processed
   */
  protected boolean firstPage = true;

  protected ByteBufferAllocator allocator;
  /* Track the list of writers used so they can be appropriately closed when necessary
  (currently used for off-heap memory which is not garbage collected) */
  private List<AutoCloseable> toClose = new ArrayList<>();

  protected DictionaryValuesWriter(
      int maxDictionaryByteSize,
      Encoding encodingForDataPage,
      Encoding encodingForDictionaryPage,
      ByteBufferAllocator allocator) {
    this.allocator = allocator;
    this.maxDictionaryByteSize = maxDictionaryByteSize;
    this.encodingForDataPage = encodingForDataPage;
    this.encodingForDictionaryPage = encodingForDictionaryPage;
  }

  protected DictionaryPage dictPage(ValuesWriter dictPageWriter) {
    DictionaryPage ret =
        new DictionaryPage(dictPageWriter.getBytes(), lastUsedDictionarySize, encodingForDictionaryPage);
    toClose.add(dictPageWriter);
    return ret;
  }

  @Override
  public boolean shouldFallBack() {
    // if the dictionary reaches the max byte size or the values can not be encoded on 4 bytes anymore.
    return dictionaryByteSize > maxDictionaryByteSize || getDictionarySize() > MAX_DICTIONARY_ENTRIES;
  }

  @Override
  public boolean isCompressionSatisfying(long rawSize, long encodedSize) {
    return (encodedSize + dictionaryByteSize) < rawSize;
  }

  @Override
  public void fallBackAllValuesTo(ValuesWriter writer) {
    fallBackDictionaryEncodedData(writer);
    if (lastUsedDictionarySize == 0) {
      // if we never used the dictionary
      // we free dictionary encoded data
      clearDictionaryContent();
      dictionaryByteSize = 0;
      encodedValues = new IntList();
    }
  }

  protected abstract void fallBackDictionaryEncodedData(ValuesWriter writer);

  @Override
  public long getBufferedSize() {
    return encodedValues.size() * 4;
  }

  @Override
  public long getAllocatedSize() {
    // size used in memory
    return encodedValues.size() * 4 + dictionaryByteSize;
  }

  @Override
  public BytesInput getBytes() {
    int maxDicId = getDictionarySize() - 1;
    LOG.debug("max dic id {}", maxDicId);
    int bitWidth = BytesUtils.getWidthFromMaxInt(maxDicId);
    int initialSlabSize = CapacityByteArrayOutputStream.initialSlabSizeHeuristic(
        MIN_INITIAL_SLAB_SIZE, maxDictionaryByteSize, 10);

    RunLengthBitPackingHybridEncoder encoder =
        new RunLengthBitPackingHybridEncoder(bitWidth, initialSlabSize, maxDictionaryByteSize, this.allocator);
    toClose.add(encoder);
    IntIterator iterator = encodedValues.iterator();
    try {
      while (iterator.hasNext()) {
        encoder.writeInt(iterator.next());
      }
      // encodes the bit width
      byte[] bytesHeader = new byte[] {(byte) bitWidth};
      BytesInput rleEncodedBytes = encoder.toBytes();
      LOG.debug("rle encoded bytes {}", rleEncodedBytes.size());
      BytesInput bytes = concat(BytesInput.from(bytesHeader), rleEncodedBytes);
      // remember size of dictionary when we last wrote a page
      lastUsedDictionarySize = getDictionarySize();
      lastUsedDictionaryByteSize = Math.toIntExact(dictionaryByteSize);
      return bytes;
    } catch (IOException e) {
      throw new ParquetEncodingException("could not encode the values", e);
    }
  }

  @Override
  public Encoding getEncoding() {
    return encodingForDataPage;
  }

  @Override
  public void reset() {
    close();
    encodedValues = new IntList();
  }

  @Override
  public void close() {
    encodedValues = null;
    AutoCloseables.uncheckedClose(toClose);
    toClose.clear();
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
        "%s DictionaryValuesWriter{\n" + "%s\n" + "%s\n" + "%s}\n",
        prefix, prefix + " dict:" + dictionaryByteSize, prefix + " values:" + encodedValues.size() * 4, prefix);
  }

  public static class PlainBinaryDictionaryValuesWriter extends DictionaryValuesWriter {

    /* type specific dictionary content */
    protected Object2IntMap<Binary> binaryDictionaryContent = new Object2IntOpenHashMap<>();
    protected List<Binary> dictionaryValues = new ArrayList<>();

    public PlainBinaryDictionaryValuesWriter(
        int maxDictionaryByteSize,
        Encoding encodingForDataPage,
        Encoding encodingForDictionaryPage,
        ByteBufferAllocator allocator) {
      super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage, allocator);
      binaryDictionaryContent.defaultReturnValue(-1);
    }

    @Override
    public void writeBytes(Binary v) {
      int id = binaryDictionaryContent.getInt(v);
      if (id == -1) {
        id = dictionaryValues.size();
        Binary copied = v.copy();
        binaryDictionaryContent.put(copied, id);
        dictionaryValues.add(copied);
        // length as int (4 bytes) + actual bytes
        dictionaryByteSize += 4L + v.length();
      }
      encodedValues.add(id);
    }

    @Override
    public DictionaryPage toDictPageAndClose() {
      if (lastUsedDictionarySize > 0) {
        // return a dictionary only if we actually used it
        PlainValuesWriter dictionaryEncoder =
            new PlainValuesWriter(lastUsedDictionaryByteSize, maxDictionaryByteSize, allocator);
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          dictionaryEncoder.writeBytes(dictionaryValues.get(i));
        }
        return dictPage(dictionaryEncoder);
      }
      return null;
    }

    @Override
    public int getDictionarySize() {
      return binaryDictionaryContent.size();
    }

    @Override
    protected void clearDictionaryContent() {
      binaryDictionaryContent.clear();
      dictionaryValues.clear();
    }

    @Override
    public void fallBackDictionaryEncodedData(ValuesWriter writer) {
      // fall back to plain encoding using the ordered dictionary values list
      IntIterator iterator = encodedValues.iterator();
      while (iterator.hasNext()) {
        int id = iterator.next();
        writer.writeBytes(dictionaryValues.get(id));
      }
    }
  }

  public static class PlainFixedLenArrayDictionaryValuesWriter extends PlainBinaryDictionaryValuesWriter {

    private final int length;

    public PlainFixedLenArrayDictionaryValuesWriter(
        int maxDictionaryByteSize,
        int length,
        Encoding encodingForDataPage,
        Encoding encodingForDictionaryPage,
        ByteBufferAllocator allocator) {
      super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage, allocator);
      this.length = length;
    }

    @Override
    public void writeBytes(Binary value) {
      int id = binaryDictionaryContent.getInt(value);
      if (id == -1) {
        id = dictionaryValues.size();
        Binary copied = value.copy();
        binaryDictionaryContent.put(copied, id);
        dictionaryValues.add(copied);
        dictionaryByteSize += length;
      }
      encodedValues.add(id);
    }

    @Override
    public DictionaryPage toDictPageAndClose() {
      if (lastUsedDictionarySize > 0) {
        // return a dictionary only if we actually used it
        FixedLenByteArrayPlainValuesWriter dictionaryEncoder = new FixedLenByteArrayPlainValuesWriter(
            length, lastUsedDictionaryByteSize, maxDictionaryByteSize, allocator);
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          dictionaryEncoder.writeBytes(dictionaryValues.get(i));
        }
        return dictPage(dictionaryEncoder);
      }
      return null;
    }
  }

  public static class PlainLongDictionaryValuesWriter extends DictionaryValuesWriter {

    /* type specific dictionary content */
    private Long2IntMap longDictionaryContent = new Long2IntOpenHashMap();
    private LongArrayList dictionaryValues = new LongArrayList();

    public PlainLongDictionaryValuesWriter(
        int maxDictionaryByteSize,
        Encoding encodingForDataPage,
        Encoding encodingForDictionaryPage,
        ByteBufferAllocator allocator) {
      super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage, allocator);
      longDictionaryContent.defaultReturnValue(-1);
    }

    @Override
    public void writeLong(long v) {
      int id = longDictionaryContent.get(v);
      if (id == -1) {
        id = dictionaryValues.size();
        longDictionaryContent.put(v, id);
        dictionaryValues.add(v);
        dictionaryByteSize += 8;
      }
      encodedValues.add(id);
    }

    @Override
    public DictionaryPage toDictPageAndClose() {
      if (lastUsedDictionarySize > 0) {
        // return a dictionary only if we actually used it
        PlainValuesWriter dictionaryEncoder =
            new PlainValuesWriter(lastUsedDictionaryByteSize, maxDictionaryByteSize, allocator);
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          dictionaryEncoder.writeLong(dictionaryValues.getLong(i));
        }
        return dictPage(dictionaryEncoder);
      }
      return null;
    }

    @Override
    public int getDictionarySize() {
      return longDictionaryContent.size();
    }

    @Override
    protected void clearDictionaryContent() {
      longDictionaryContent.clear();
      dictionaryValues.clear();
    }

    @Override
    public void fallBackDictionaryEncodedData(ValuesWriter writer) {
      // fall back to plain encoding
      IntIterator iterator = encodedValues.iterator();
      while (iterator.hasNext()) {
        int id = iterator.next();
        writer.writeLong(dictionaryValues.getLong(id));
      }
    }
  }

  public static class PlainDoubleDictionaryValuesWriter extends DictionaryValuesWriter {

    /* type specific dictionary content */
    private Double2IntMap doubleDictionaryContent = new Double2IntOpenHashMap();
    private DoubleArrayList dictionaryValues = new DoubleArrayList();

    public PlainDoubleDictionaryValuesWriter(
        int maxDictionaryByteSize,
        Encoding encodingForDataPage,
        Encoding encodingForDictionaryPage,
        ByteBufferAllocator allocator) {
      super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage, allocator);
      doubleDictionaryContent.defaultReturnValue(-1);
    }

    @Override
    public void writeDouble(double v) {
      int id = doubleDictionaryContent.get(v);
      if (id == -1) {
        id = dictionaryValues.size();
        doubleDictionaryContent.put(v, id);
        dictionaryValues.add(v);
        dictionaryByteSize += 8;
      }
      encodedValues.add(id);
    }

    @Override
    public DictionaryPage toDictPageAndClose() {
      if (lastUsedDictionarySize > 0) {
        // return a dictionary only if we actually used it
        PlainValuesWriter dictionaryEncoder =
            new PlainValuesWriter(lastUsedDictionaryByteSize, maxDictionaryByteSize, allocator);
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          dictionaryEncoder.writeDouble(dictionaryValues.getDouble(i));
        }
        return dictPage(dictionaryEncoder);
      }
      return null;
    }

    @Override
    public int getDictionarySize() {
      return doubleDictionaryContent.size();
    }

    @Override
    protected void clearDictionaryContent() {
      doubleDictionaryContent.clear();
      dictionaryValues.clear();
    }

    @Override
    public void fallBackDictionaryEncodedData(ValuesWriter writer) {
      // fall back to plain encoding
      IntIterator iterator = encodedValues.iterator();
      while (iterator.hasNext()) {
        int id = iterator.next();
        writer.writeDouble(dictionaryValues.getDouble(id));
      }
    }
  }

  public static class PlainIntegerDictionaryValuesWriter extends DictionaryValuesWriter {

    /* type specific dictionary content */
    private Int2IntMap intDictionaryContent = new Int2IntOpenHashMap();
    private IntArrayList dictionaryValues = new IntArrayList();

    public PlainIntegerDictionaryValuesWriter(
        int maxDictionaryByteSize,
        Encoding encodingForDataPage,
        Encoding encodingForDictionaryPage,
        ByteBufferAllocator allocator) {
      super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage, allocator);
      intDictionaryContent.defaultReturnValue(-1);
    }

    @Override
    public void writeInteger(int v) {
      int id = intDictionaryContent.get(v);
      if (id == -1) {
        id = dictionaryValues.size();
        intDictionaryContent.put(v, id);
        dictionaryValues.add(v);
        dictionaryByteSize += 4;
      }
      encodedValues.add(id);
    }

    @Override
    public DictionaryPage toDictPageAndClose() {
      if (lastUsedDictionarySize > 0) {
        // return a dictionary only if we actually used it
        PlainValuesWriter dictionaryEncoder =
            new PlainValuesWriter(lastUsedDictionaryByteSize, maxDictionaryByteSize, allocator);
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          dictionaryEncoder.writeInteger(dictionaryValues.getInt(i));
        }
        return dictPage(dictionaryEncoder);
      }
      return null;
    }

    @Override
    public int getDictionarySize() {
      return intDictionaryContent.size();
    }

    @Override
    protected void clearDictionaryContent() {
      intDictionaryContent.clear();
      dictionaryValues.clear();
    }

    @Override
    public void fallBackDictionaryEncodedData(ValuesWriter writer) {
      // fall back to plain encoding
      IntIterator iterator = encodedValues.iterator();
      while (iterator.hasNext()) {
        int id = iterator.next();
        writer.writeInteger(dictionaryValues.getInt(id));
      }
    }
  }

  public static class PlainFloatDictionaryValuesWriter extends DictionaryValuesWriter {

    /* type specific dictionary content */
    private Float2IntMap floatDictionaryContent = new Float2IntOpenHashMap();
    private FloatArrayList dictionaryValues = new FloatArrayList();

    public PlainFloatDictionaryValuesWriter(
        int maxDictionaryByteSize,
        Encoding encodingForDataPage,
        Encoding encodingForDictionaryPage,
        ByteBufferAllocator allocator) {
      super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage, allocator);
      floatDictionaryContent.defaultReturnValue(-1);
    }

    @Override
    public void writeFloat(float v) {
      int id = floatDictionaryContent.get(v);
      if (id == -1) {
        id = dictionaryValues.size();
        floatDictionaryContent.put(v, id);
        dictionaryValues.add(v);
        dictionaryByteSize += 4;
      }
      encodedValues.add(id);
    }

    @Override
    public DictionaryPage toDictPageAndClose() {
      if (lastUsedDictionarySize > 0) {
        // return a dictionary only if we actually used it
        PlainValuesWriter dictionaryEncoder =
            new PlainValuesWriter(lastUsedDictionaryByteSize, maxDictionaryByteSize, allocator);
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          dictionaryEncoder.writeFloat(dictionaryValues.getFloat(i));
        }
        return dictPage(dictionaryEncoder);
      }
      return null;
    }

    @Override
    public int getDictionarySize() {
      return floatDictionaryContent.size();
    }

    @Override
    protected void clearDictionaryContent() {
      floatDictionaryContent.clear();
      dictionaryValues.clear();
    }

    @Override
    public void fallBackDictionaryEncodedData(ValuesWriter writer) {
      // fall back to plain encoding
      IntIterator iterator = encodedValues.iterator();
      while (iterator.hasNext()) {
        int id = iterator.next();
        writer.writeFloat(dictionaryValues.getFloat(id));
      }
    }
  }
}
