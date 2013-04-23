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
package parquet.column.impl;

import static parquet.Log.DEBUG;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import parquet.Log;
import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReader;
import parquet.column.Dictionary;
import parquet.column.Encoding;
import parquet.column.page.DictionaryPage;
import parquet.column.page.Page;
import parquet.column.page.PageReader;
import parquet.column.values.ValuesReader;
import parquet.column.values.ValuesType;
import parquet.io.ParquetDecodingException;
import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;

/**
 * ColumnReader implementation
 *
 * @author Julien Le Dem
 *
 */
abstract class ColumnReaderImpl implements ColumnReader {
  private static final Log LOG = Log.getLog(ColumnReaderImpl.class);

  private final ColumnDescriptor path;
  private final long totalValueCount;
  private final PageReader pageReader;
  private Map<Encoding, Dictionary> dictionaries = new HashMap<Encoding, Dictionary>();

  private ValuesReader repetitionLevelColumn;
  private ValuesReader definitionLevelColumn;
  protected ValuesReader dataColumn;

  private int repetitionLevel;
  private int definitionLevel;
  private boolean valueRead = false;
  private boolean consumed = true;

  private int readValues;
  private int readValuesInPage;
  private long pageValueCount;

  final PrimitiveConverter converter;


  /**
   * creates a reader for triplets
   * @param path the descriptor for the corresponding column
   * @param pageReader the underlying store to read from
   */
  public ColumnReaderImpl(ColumnDescriptor path, PageReader pageReader, PrimitiveConverter converter) {
    this.converter = converter;
    if (path == null) {
      throw new NullPointerException("path");
    }
    if (pageReader == null) {
      throw new NullPointerException("pageReader");
    }
    this.path = path;
    this.pageReader = pageReader;
    DictionaryPage dictionaryPage;
    while ((dictionaryPage = pageReader.readDictionaryPage()) != null) {
      try {
        Dictionary dictionary = dictionaryPage.getEncoding().initDictionary(path, dictionaryPage);
        dictionaries.put(dictionary.getEncoding(), dictionary);
      } catch (IOException e) {
        throw new ParquetDecodingException("could not decode the dictionary for " + path, e);
      }
    }
    this.totalValueCount = pageReader.getTotalValueCount();
    if (totalValueCount == 0) {
      throw new ParquetDecodingException("totalValueCount == 0");
    }
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#isFullyConsumed()
   */
  @Override
  public boolean isFullyConsumed() {
    return readValues >= totalValueCount;
  }

  public abstract void writeNextValueToConverter();

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getString()
   */
  @Override
  public String getString() {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getInteger()
   */
  @Override
  public int getInteger() {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getBoolean()
   */
  @Override
  public boolean getBoolean() {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getLong()
   */
  public long getLong() {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getBinary()
   */
  @Override
  public Binary getBinary() {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getFloat()
   */
  @Override
  public float getFloat() {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getDouble()
   */
  @Override
  public double getDouble() {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getCurrentRepetitionLevel()
   */
  @Override
  public int getCurrentRepetitionLevel() {
    checkRead();
    return repetitionLevel;
  }

  /**
   * used for debugging
   * @return
   * @throws IOException
   */
  abstract public String getCurrentValueToString() throws IOException;

  /**
   * reads the current value
   */
  protected abstract void readCurrentValue();

  protected void checkValueRead() {
    checkRead();
    if (!consumed && !valueRead) {
      readCurrentValue();
      valueRead = true;
    }
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getCurrentDefinitionLevel()
   */
  @Override
  public int getCurrentDefinitionLevel() {
    checkRead();
    return definitionLevel;
  }

  // TODO: change the logic around read() to not tie together reading from the 3 columns
  private void read() {
    repetitionLevel = repetitionLevelColumn.readInteger();
    definitionLevel = definitionLevelColumn.readInteger();
    ++readValues;
    ++readValuesInPage;
    consumed = false;
  }

  protected void checkRead() {
    if (!consumed) {
      return;
    }
    if (isFullyConsumed()) {
      if (DEBUG) LOG.debug("end reached");
      repetitionLevel = 0; // the next repetition level
      return;
    }
    if (isPageFullyConsumed()) {
      if (DEBUG) LOG.debug("loading page");
      Page page = pageReader.readPage();

      this.repetitionLevelColumn = page.getRlEncoding().getValuesReader(path, ValuesType.REPETITION_LEVEL);
      this.definitionLevelColumn = page.getDlEncoding().getValuesReader(path, ValuesType.DEFINITION_LEVEL);
      this.dataColumn = page.getValueEncoding().getValuesReader(path, ValuesType.VALUES);
      final Dictionary dictionary = dictionaries.get(page.getValueEncoding());
      if (dictionary != null) {
        dataColumn.setDictionary(dictionary);
      }
      this.pageValueCount = page.getValueCount();
      this.readValuesInPage = 0;
      try {
        byte[] bytes = page.getBytes().toByteArray();
        if (DEBUG) LOG.debug("page size " + bytes.length + " bytes and " + pageValueCount + " records");
        if (DEBUG) LOG.debug("reading repetition levels at 0");
        int next = repetitionLevelColumn.initFromPage(pageValueCount, bytes, 0);
        if (DEBUG) LOG.debug("reading definition levels at " + next);
        next = definitionLevelColumn.initFromPage(pageValueCount, bytes, next);
        if (DEBUG) LOG.debug("reading data at " + next);
        dataColumn.initFromPage(pageValueCount, bytes, next);
      } catch (IOException e) {
        throw new ParquetDecodingException("could not read page " + page + " in col " + path, e);
      }
    }
    read();
  }

  private boolean isPageFullyConsumed() {
    return readValuesInPage >= pageValueCount;
  }

  /**
   *
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#consume()
   */
  @Override
  public void consume() {
    consumed = true;
    valueRead = false;
  }

}
