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
package parquet.column.impl;

import static java.lang.String.format;
import static parquet.Log.DEBUG;
import static parquet.Preconditions.checkNotNull;
import static parquet.column.ValuesType.DEFINITION_LEVEL;
import static parquet.column.ValuesType.REPETITION_LEVEL;
import static parquet.column.ValuesType.VALUES;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReader;
import parquet.column.Dictionary;
import parquet.column.Encoding;
import parquet.column.page.DataPage;
import parquet.column.page.DataPageV1;
import parquet.column.page.DataPageV2;
import parquet.column.page.DictionaryPage;
import parquet.column.page.PageReader;
import parquet.column.values.ValuesReader;
import parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import parquet.io.ParquetDecodingException;
import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.PrimitiveType.PrimitiveTypeNameConverter;

/**
 * ColumnReader implementation
 *
 * @author Julien Le Dem
 *
 */
class ColumnReaderImpl implements ColumnReader {
  private static final Log LOG = Log.getLog(ColumnReaderImpl.class);

  /**
   * binds the lower level page decoder to the record converter materializing the records
   *
   * @author Julien Le Dem
   *
   */
  private static abstract class Binding {

    /**
     * read one value from the underlying page
     */
    abstract void read();

    /**
     * skip one value from the underlying page
     */
    abstract void skip();

    /**
     * write current value to converter
     */
    abstract void writeValue();

    /**
     * @return current value
     */
    public int getDictionaryId() {
      throw new UnsupportedOperationException();
    }

    /**
     * @return current value
     */
    public int getInteger() {
      throw new UnsupportedOperationException();
    }

    /**
     * @return current value
     */
    public boolean getBoolean() {
      throw new UnsupportedOperationException();
    }

    /**
     * @return current value
     */
    public long getLong() {
      throw new UnsupportedOperationException();
    }

    /**
     * @return current value
     */
    public Binary getBinary() {
      throw new UnsupportedOperationException();
    }

    /**
     * @return current value
     */
    public float getFloat() {
      throw new UnsupportedOperationException();
    }

    /**
     * @return current value
     */
    public double getDouble() {
      throw new UnsupportedOperationException();
    }
  }

  private final ColumnDescriptor path;
  private final long totalValueCount;
  private final PageReader pageReader;
  private final Dictionary dictionary;

  private IntIterator repetitionLevelColumn;
  private IntIterator definitionLevelColumn;
  protected ValuesReader dataColumn;

  private int repetitionLevel;
  private int definitionLevel;
  private int dictionaryId;

  private long endOfPageValueCount;
  private int readValues;
  private int pageValueCount;

  private final PrimitiveConverter converter;
  private Binding binding;

  // this is needed because we will attempt to read the value twice when filtering
  // TODO: rework that
  private boolean valueRead;

  private void bindToDictionary(final Dictionary dictionary) {
    binding =
        new Binding() {
          void read() {
            dictionaryId = dataColumn.readValueDictionaryId();
          }
          public void skip() {
            dataColumn.skip();
          }
          public int getDictionaryId() {
            return dictionaryId;
          }
          void writeValue() {
            converter.addValueFromDictionary(dictionaryId);
          }
          public int getInteger() {
            return dictionary.decodeToInt(dictionaryId);
          }
          public boolean getBoolean() {
            return dictionary.decodeToBoolean(dictionaryId);
          }
          public long getLong() {
            return dictionary.decodeToLong(dictionaryId);
          }
          public Binary getBinary() {
            return dictionary.decodeToBinary(dictionaryId);
          }
          public float getFloat() {
            return dictionary.decodeToFloat(dictionaryId);
          }
          public double getDouble() {
            return dictionary.decodeToDouble(dictionaryId);
          }
        };
  }

  private void bind(PrimitiveTypeName type) {
    binding = type.convert(new PrimitiveTypeNameConverter<Binding, RuntimeException>() {
      @Override
      public Binding convertFLOAT(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          float current;
          void read() {
            current = dataColumn.readFloat();
          }
          public void skip() {
            current = 0;
            dataColumn.skip();
          }
          public float getFloat() {
            return current;
          }
          void writeValue() {
            converter.addFloat(current);
          }
        };
      }
      @Override
      public Binding convertDOUBLE(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          double current;
          void read() {
            current = dataColumn.readDouble();
          }
          public void skip() {
            current = 0;
            dataColumn.skip();
          }
          public double getDouble() {
            return current;
          }
          void writeValue() {
            converter.addDouble(current);
          }
        };
      }
      @Override
      public Binding convertINT32(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          int current;
          void read() {
            current = dataColumn.readInteger();
          }
          public void skip() {
            current = 0;
            dataColumn.skip();
          }
          @Override
          public int getInteger() {
            return current;
          }
          void writeValue() {
            converter.addInt(current);
          }
        };
      }
      @Override
      public Binding convertINT64(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          long current;
          void read() {
            current = dataColumn.readLong();
          }
          public void skip() {
            current = 0;
            dataColumn.skip();
          }
          @Override
          public long getLong() {
            return current;
          }
          void writeValue() {
            converter.addLong(current);
          }
        };
      }
      @Override
      public Binding convertINT96(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return this.convertBINARY(primitiveTypeName);
      }
      @Override
      public Binding convertFIXED_LEN_BYTE_ARRAY(
          PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return this.convertBINARY(primitiveTypeName);
      }
      @Override
      public Binding convertBOOLEAN(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          boolean current;
          void read() {
            current = dataColumn.readBoolean();
          }
          public void skip() {
            current = false;
            dataColumn.skip();
          }
          @Override
          public boolean getBoolean() {
            return current;
          }
          void writeValue() {
            converter.addBoolean(current);
          }
        };
      }
      @Override
      public Binding convertBINARY(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          Binary current;
          void read() {
            current = dataColumn.readBytes();
          }
          public void skip() {
            current = null;
            dataColumn.skip();
          }
          @Override
          public Binary getBinary() {
            return current;
          }
          void writeValue() {
            converter.addBinary(current);
          }
        };
      }
    });
  }

  /**
   * creates a reader for triplets
   * @param path the descriptor for the corresponding column
   * @param pageReader the underlying store to read from
   */
  public ColumnReaderImpl(ColumnDescriptor path, PageReader pageReader, PrimitiveConverter converter) {
    this.path = checkNotNull(path, "path");
    this.pageReader = checkNotNull(pageReader, "pageReader");
    this.converter = checkNotNull(converter, "converter");
    DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
    if (dictionaryPage != null) {
      try {
        this.dictionary = dictionaryPage.getEncoding().initDictionary(path, dictionaryPage);
        if (converter.hasDictionarySupport()) {
          converter.setDictionary(dictionary);
        }
      } catch (IOException e) {
        throw new ParquetDecodingException("could not decode the dictionary for " + path, e);
      }
    } else {
      this.dictionary = null;
    }
    this.totalValueCount = pageReader.getTotalValueCount();
    if (totalValueCount == 0) {
      throw new ParquetDecodingException("totalValueCount == 0");
    }
    consume();
  }

  private boolean isFullyConsumed() {
    return readValues >= totalValueCount;
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#writeCurrentValueToConverter()
   */
  @Override
  public void writeCurrentValueToConverter() {
    readValue();
    this.binding.writeValue();
  }

  @Override
  public int getCurrentValueDictionaryID() {
    readValue();
    return binding.getDictionaryId();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getInteger()
   */
  @Override
  public int getInteger() {
    readValue();
    return this.binding.getInteger();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getBoolean()
   */
  @Override
  public boolean getBoolean() {
    readValue();
    return this.binding.getBoolean();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getLong()
   */
  @Override
  public long getLong() {
    readValue();
    return this.binding.getLong();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getBinary()
   */
  @Override
  public Binary getBinary() {
    readValue();
    return this.binding.getBinary();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getFloat()
   */
  @Override
  public float getFloat() {
    readValue();
    return this.binding.getFloat();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getDouble()
   */
  @Override
  public double getDouble() {
    readValue();
    return this.binding.getDouble();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getCurrentRepetitionLevel()
   */
  @Override
  public int getCurrentRepetitionLevel() {
    return repetitionLevel;
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getDescriptor()
   */
  @Override
  public ColumnDescriptor getDescriptor() {
    return path;
  }

  /**
   * Reads the value into the binding.
   */
  public void readValue() {
    try {
      if (!valueRead) {
        binding.read();
        valueRead = true;
      }
    } catch (RuntimeException e) {
      throw new ParquetDecodingException(
          format(
              "Can't read value in column %s at value %d out of %d, %d out of %d in currentPage. repetition level: %d, definition level: %d",
              path, readValues, totalValueCount, readValues - (endOfPageValueCount - pageValueCount), pageValueCount, repetitionLevel, definitionLevel),
          e);
    }
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#skip()
   */
  @Override
  public void skip() {
    if (!valueRead) {
      binding.skip();
      valueRead = true;
    }
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getCurrentDefinitionLevel()
   */
  @Override
  public int getCurrentDefinitionLevel() {
    return definitionLevel;
  }

  // TODO: change the logic around read() to not tie together reading from the 3 columns
  private void readRepetitionAndDefinitionLevels() {
    repetitionLevel = repetitionLevelColumn.nextInt();
    definitionLevel = definitionLevelColumn.nextInt();
    ++readValues;
  }

  private void checkRead() {
    if (isPageFullyConsumed()) {
      if (isFullyConsumed()) {
        if (DEBUG) LOG.debug("end reached");
        repetitionLevel = 0; // the next repetition level
        return;
      }
      readPage();
    }
    readRepetitionAndDefinitionLevels();
  }

  private void readPage() {
    if (DEBUG) LOG.debug("loading page");
    DataPage page = pageReader.readPage();
    page.accept(new DataPage.Visitor<Void>() {
      @Override
      public Void visit(DataPageV1 dataPageV1) {
        readPageV1(dataPageV1);
        return null;
      }
      @Override
      public Void visit(DataPageV2 dataPageV2) {
        readPageV2(dataPageV2);
        return null;
      }
    });
  }

  private void initDataReader(Encoding dataEncoding, byte[] bytes, int offset, int valueCount) {
    this.pageValueCount = valueCount;
    this.endOfPageValueCount = readValues + pageValueCount;
    if (dataEncoding.usesDictionary()) {
      if (dictionary == null) {
        throw new ParquetDecodingException(
            "could not read page in col " + path + " as the dictionary was missing for encoding " + dataEncoding);
      }
      this.dataColumn = dataEncoding.getDictionaryBasedValuesReader(path, VALUES, dictionary);
    } else {
      this.dataColumn = dataEncoding.getValuesReader(path, VALUES);
    }
    if (dataEncoding.usesDictionary() && converter.hasDictionarySupport()) {
      bindToDictionary(dictionary);
    } else {
      bind(path.getType());
    }
    try {
      dataColumn.initFromPage(pageValueCount, bytes, offset);
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page in col " + path, e);
    }
  }

  private void readPageV1(DataPageV1 page) {
    ValuesReader rlReader = page.getRlEncoding().getValuesReader(path, REPETITION_LEVEL);
    ValuesReader dlReader = page.getDlEncoding().getValuesReader(path, DEFINITION_LEVEL);
    this.repetitionLevelColumn = new ValuesReaderIntIterator(rlReader);
    this.definitionLevelColumn = new ValuesReaderIntIterator(dlReader);
    try {
      byte[] bytes = page.getBytes().toByteArray();
      if (DEBUG) LOG.debug("page size " + bytes.length + " bytes and " + pageValueCount + " records");
      if (DEBUG) LOG.debug("reading repetition levels at 0");
      rlReader.initFromPage(pageValueCount, bytes, 0);
      int next = rlReader.getNextOffset();
      if (DEBUG) LOG.debug("reading definition levels at " + next);
      dlReader.initFromPage(pageValueCount, bytes, next);
      next = dlReader.getNextOffset();
      if (DEBUG) LOG.debug("reading data at " + next);
      initDataReader(page.getValueEncoding(), bytes, next, page.getValueCount());
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page " + page + " in col " + path, e);
    }
  }

  private void readPageV2(DataPageV2 page) {
    this.repetitionLevelColumn = newRLEIterator(path.getMaxRepetitionLevel(), page.getRepetitionLevels());
    this.definitionLevelColumn = newRLEIterator(path.getMaxDefinitionLevel(), page.getDefinitionLevels());
    try {
      if (DEBUG) LOG.debug("page data size " + page.getData().size() + " bytes and " + pageValueCount + " records");
      initDataReader(page.getDataEncoding(), page.getData().toByteArray(), 0, page.getValueCount());
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page " + page + " in col " + path, e);
    }
  }

  private IntIterator newRLEIterator(int maxLevel, BytesInput bytes) {
    try {
      if (maxLevel == 0) {
        return new NullIntIterator();
      }
      return new RLEIntIterator(
          new RunLengthBitPackingHybridDecoder(
              BytesUtils.getWidthFromMaxInt(maxLevel),
              new ByteArrayInputStream(bytes.toByteArray())));
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read levels in page for col " + path, e);
    }
  }

  private boolean isPageFullyConsumed() {
    return readValues >= endOfPageValueCount;
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#consume()
   */
  @Override
  public void consume() {
    checkRead();
    valueRead = false;
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getTotalValueCount()
   */
  @Override
  public long getTotalValueCount() {
    return totalValueCount;
  }

  static abstract class IntIterator {
    abstract int nextInt();
  }

  static class ValuesReaderIntIterator extends IntIterator {
    ValuesReader delegate;

    public ValuesReaderIntIterator(ValuesReader delegate) {
      super();
      this.delegate = delegate;
    }

    @Override
    int nextInt() {
      return delegate.readInteger();
    }
  }

  static class RLEIntIterator extends IntIterator {
    RunLengthBitPackingHybridDecoder delegate;

    public RLEIntIterator(RunLengthBitPackingHybridDecoder delegate) {
      this.delegate = delegate;
    }

    @Override
    int nextInt() {
      try {
        return delegate.readInt();
      } catch (IOException e) {
        throw new ParquetDecodingException(e);
      }
    }
  }

  private static final class NullIntIterator extends IntIterator {
    @Override
    int nextInt() {
      return 0;
    }
  }
}
