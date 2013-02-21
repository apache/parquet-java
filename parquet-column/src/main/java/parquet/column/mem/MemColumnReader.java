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

import java.io.IOException;

import parquet.Log;
import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReader;
import parquet.column.primitive.BitPackingColumnReader;
import parquet.column.primitive.BooleanPlainColumnReader;
import parquet.column.primitive.BoundedColumnFactory;
import parquet.column.primitive.PlainColumnReader;
import parquet.column.primitive.PrimitiveColumnReader;


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

  @Override
  public boolean isFullyConsumed() {
    return readValues >= totalValueCount;
  }

  @Override
  public String getString() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInteger() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getBoolean() {
    throw new UnsupportedOperationException();
  }

  public long getLong() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBinary() {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat() {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getCurrentRepetitionLevel() {
    checkRead();
    return repetitionLevel;
  }

  abstract public String getCurrentValueToString() throws IOException;

  protected abstract void readCurrentValue();

  protected void checkValueRead() {
    checkRead();
    if (!consumed && !valueRead) {
      readCurrentValue();
      valueRead = true;
    }
  }

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
      repetitionLevelColumn = new BitPackingColumnReader(path.getRepetitionLevel());
      definitionLevelColumn = BoundedColumnFactory.getBoundedReader(path.getDefinitionLevel());
      // TODO: from encoding
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
        int next = repetitionLevelColumn.initFromPage(pageValueCount, bytes, 0);
        next = definitionLevelColumn.initFromPage(pageValueCount, bytes, next);
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

  @Override
  public void consume() {
    consumed = true;
    valueRead = false;
  }

}