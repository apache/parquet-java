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
package parquet.column.mem;

import static parquet.Log.DEBUG;
import static parquet.column.Encoding.PLAIN;

import java.io.IOException;

import parquet.Log;
import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReader;
import parquet.column.primitive.BitPackingColumnReader;
import parquet.column.primitive.BooleanPlainColumnReader;
import parquet.column.primitive.BoundedColumnFactory;
import parquet.column.primitive.PlainColumnReader;
import parquet.column.primitive.PrimitiveColumnReader;
import parquet.io.Binary;
import parquet.io.ParquetDecodingException;

/**
 * ColumnReader implementation 
 *
 * @author Julien Le Dem
 *
 */
abstract class MemColumnReader implements ColumnReader {
  private static final Log LOG = Log.getLog(MemColumnReader.class);

  private final ColumnDescriptor path;
  private final long totalValueCount;
  private final PageReader pageReader;

  private PrimitiveColumnReader repetitionLevelColumn;
  private PrimitiveColumnReader definitionLevelColumn;
  protected PrimitiveColumnReader dataColumn;

  private int repetitionLevel;
  private int definitionLevel;
  private boolean valueRead = false;
  private boolean consumed = true;

  private int readValues;
  private int readValuesInPage;
  private long pageValueCount;

  /**
   *
   * @param path the descriptor for the corresponding column
   * @param pageReader the underlying store to read from
   */
  public MemColumnReader(ColumnDescriptor path, PageReader pageReader) {
    if (path == null) {
      throw new NullPointerException("path");
    }
    if (pageReader == null) {
      throw new NullPointerException("pageReader");
    }
    this.path = path;
    this.pageReader = pageReader;
    this.totalValueCount = pageReader.getTotalValueCount();
    if (totalValueCount == 0) {
      throw new ParquetDecodingException("totalValueCount == 0");
    }
  }

  /**
   *
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#isFullyConsumed()
   */
  @Override
  public boolean isFullyConsumed() {
    return readValues >= totalValueCount;
  }

  /**
   *
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getString()
   */
  @Override
  public String getString() {
    throw new UnsupportedOperationException();
  }

  /**
   *
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getInteger()
   */
  @Override
  public int getInteger() {
    throw new UnsupportedOperationException();
  }

  /**
   *
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getBoolean()
   */
  @Override
  public boolean getBoolean() {
    throw new UnsupportedOperationException();
  }

  /**
   *
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getLong()
   */
  public long getLong() {
    throw new UnsupportedOperationException();
  }

  /**
   *
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getBinary()
   */
  @Override
  public Binary getBinary() {
    throw new UnsupportedOperationException();
  }

  /**
   *
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getFloat()
   */
  @Override
  public float getFloat() {
    throw new UnsupportedOperationException();
  }

  /**
   *
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getDouble()
   */
  @Override
  public double getDouble() {
    throw new UnsupportedOperationException();
  }

  /**
   *
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
   *
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
      if (page.getEncoding() != PLAIN) {
        // TODO: implement more encoding
        throw new ParquetDecodingException("Unsupported encoding: " + page.getEncoding());
      }

      repetitionLevelColumn = new BitPackingColumnReader(path.getMaxRepetitionLevel());
      definitionLevelColumn = BoundedColumnFactory.getBoundedReader(path.getMaxDefinitionLevel());
      switch (path.getType()) {
      case BOOLEAN:
        this.dataColumn = new BooleanPlainColumnReader();
      default:
        this.dataColumn = new PlainColumnReader();
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
