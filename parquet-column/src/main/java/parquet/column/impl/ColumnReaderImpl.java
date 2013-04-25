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

import parquet.Log;
import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReader;
import parquet.column.Dictionary;
import parquet.column.page.DictionaryPage;
import parquet.column.page.Page;
import parquet.column.page.PageReader;
import parquet.column.values.ValuesReader;
import parquet.column.values.ValuesType;
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

  private static abstract class Binding {
    abstract void read();
    abstract void writeValue();
    public int getDictionaryId() {
      throw new UnsupportedOperationException();
    }
    public String getString() {
      throw new UnsupportedOperationException();
    }
    public int getInteger() {
      throw new UnsupportedOperationException();
    }
    public boolean getBoolean() {
      throw new UnsupportedOperationException();
    }
    public long getLong() {
      throw new UnsupportedOperationException();
    }
    public Binary getBinary() {
      throw new UnsupportedOperationException();
    }
    public float getFloat() {
      throw new UnsupportedOperationException();
    }
    public double getDouble() {
      throw new UnsupportedOperationException();
    }
  }

  private final ColumnDescriptor path;
  private final long totalValueCount;
  private final PageReader pageReader;
  private final Dictionary dictionary;

  private ValuesReader repetitionLevelColumn;
  private ValuesReader definitionLevelColumn;
  protected ValuesReader dataColumn;

  private int repetitionLevel;
  private int definitionLevel;
  private int dictionaryId;
  private boolean valueRead = false;
  private boolean consumed = true;

  private int readValues;
  private int readValuesInPage;
  private long pageValueCount;

  private final PrimitiveConverter converter;
  private Binding binding;

  private void bindToDictionary(final Dictionary dictionary) {
    binding =
        new Binding() {
          void read() {
            dictionaryId = dataColumn.readValueDictionaryId();
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
    binding = type.convert(new PrimitiveTypeNameConverter<Binding>() {
      @Override
      public Binding convertFLOAT(PrimitiveTypeName primitiveTypeName) {
        return new Binding() {
          float current;
          void read() {
            current = dataColumn.readFloat();
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
      public Binding convertDOUBLE(PrimitiveTypeName primitiveTypeName) {
        return new Binding() {
          double current;
          void read() {
            current = dataColumn.readDouble();
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
      public Binding convertINT32(PrimitiveTypeName primitiveTypeName) {
        return new Binding() {
          int current;
          void read() {
            current = dataColumn.readInteger();
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
      public Binding convertINT64(PrimitiveTypeName primitiveTypeName) {
        return new Binding() {
          long current;
          void read() {
            current = dataColumn.readLong();
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
      public Binding convertINT96(PrimitiveTypeName primitiveTypeName) {
        throw new UnsupportedOperationException("INT96 NYI");
      }
      @Override
      public Binding convertFIXED_LEN_BYTE_ARRAY(
          PrimitiveTypeName primitiveTypeName) {
        throw new UnsupportedOperationException("FIXED_LEN_BYTE_ARRAY NYI");
      }
      @Override
      public Binding convertBOOLEAN(PrimitiveTypeName primitiveTypeName) {
        return new Binding() {
          boolean current;
          void read() {
            current = dataColumn.readBoolean();
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
      public Binding convertBINARY(PrimitiveTypeName primitiveTypeName) {
        return new Binding() {
          Binary current;
          void read() {
            current = dataColumn.readBytes();
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
    if (path == null) {
      throw new NullPointerException("path");
    }
    if (pageReader == null) {
      throw new NullPointerException("pageReader");
    }
    if (converter == null) {
      throw new NullPointerException("converter");
    }
    this.path = path;
    this.pageReader = pageReader;
    this.converter = converter;
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
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#isFullyConsumed()
   */
  @Override
  public boolean isFullyConsumed() {
    return readValues >= totalValueCount;
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#writeCurrentValueToConverter()
   */
  @Override
  public void writeCurrentValueToConverter() {
    checkValueRead();
    this.binding.writeValue();
  }

  @Override
  public int getCurrentValueDictionaryID() {
    checkValueRead();
    return binding.getDictionaryId();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getInteger()
   */
  @Override
  public int getInteger() {
    checkValueRead();
    return this.binding.getInteger();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getBoolean()
   */
  @Override
  public boolean getBoolean() {
    checkValueRead();
    return this.binding.getBoolean();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getLong()
   */
  @Override
  public long getLong() {
    checkValueRead();
    return this.binding.getLong();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getBinary()
   */
  @Override
  public Binary getBinary() {
    checkValueRead();
    return this.binding.getBinary();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getFloat()
   */
  @Override
  public float getFloat() {
    checkValueRead();
    return this.binding.getFloat();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getDouble()
   */
  @Override
  public double getDouble() {
    checkValueRead();
    return this.binding.getDouble();
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
   * reads the current value
   */
  public void readCurrentValue() {
    binding.read();
  }

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
      if (dictionary == null) {
        this.dataColumn = page.getValueEncoding().getValuesReader(path, ValuesType.VALUES);
      } else {
        this.dataColumn = page.getValueEncoding().getDictionaryBasedValuesReader(path, ValuesType.VALUES, dictionary);
      }
      if (dictionary != null && converter.hasDictionarySupport()) {
        bindToDictionary(dictionary);
      } else {
        bind(path.getType());
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
