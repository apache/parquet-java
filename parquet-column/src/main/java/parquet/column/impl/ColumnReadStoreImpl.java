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

import java.io.IOException;

import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReadStore;
import parquet.column.ColumnReader;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;
import parquet.io.ParquetDecodingException;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.Type;


public class ColumnReadStoreImpl implements ColumnReadStore {

  private final PageReadStore pageReadStore;
  private final GroupConverter recordConverter;
  private final MessageType schema;

  public ColumnReadStoreImpl(PageReadStore pageReadStore, GroupConverter recordConverter, MessageType schema) {
    super();
    this.pageReadStore = pageReadStore;
    this.recordConverter = recordConverter;
    this.schema = schema;
  }

  public ColumnReader getColumnReader(ColumnDescriptor path) {
    return newMemColumnReader(path, pageReadStore.getPageReader(path));
  }

  private ColumnReaderImpl newMemColumnReader(ColumnDescriptor path, PageReader pageReader) {
    PrimitiveConverter converter = getPrimitiveConverter(path);
    switch (path.getType()) {
    case INT32:
      return new INT32MemColumnReader(path, pageReader, converter);
    case INT64:
      return new INT64MemColumnReader(path, pageReader, converter);
    case BOOLEAN:
      return new BOOLEANMemColumnReader(path, pageReader, converter);
    case BINARY:
      return new BINARYMemColumnReader(path, pageReader, converter);
    case FLOAT:
      return new FLOATMemColumnReader(path, pageReader, converter);
    case DOUBLE:
      return new DOUBLEMemColumnReader(path, pageReader, converter);
    }
    throw new ParquetDecodingException("type "+path.getType()+" not supported");
  }

  private PrimitiveConverter getPrimitiveConverter(ColumnDescriptor path) {
    Type currentType = schema;
    Converter currentConverter = recordConverter;
    for (String fieldName : path.getPath()) {
      final GroupType groupType = currentType.asGroupType();
      int fieldIndex = groupType.getFieldIndex(fieldName);
      currentType = groupType.getType(fieldName);
      currentConverter = currentConverter.asGroupConverter().getConverter(fieldIndex);
    }
    PrimitiveConverter converter = currentConverter.asPrimitiveConverter();
    return converter;
  }

  private static final class INT32MemColumnReader extends ColumnReaderImpl {
    private int currentInt;

    public INT32MemColumnReader(ColumnDescriptor path, PageReader pageReader, PrimitiveConverter converter) {
      super(path, pageReader, converter);
    }

    @Override
    public int getInteger() {
      checkValueRead();
      return currentInt;
    }

    @Override
    protected void readCurrentValue() {
        currentInt = dataColumn.readInteger();
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(currentInt);
    }

    @Override
    public void writeNextValueToConverter() {
      converter.addInt(getInteger());
    }
  }

  private static final class INT64MemColumnReader extends ColumnReaderImpl {
    private long currentLong;

    public INT64MemColumnReader(ColumnDescriptor path, PageReader pageReader, PrimitiveConverter converter) {
      super(path, pageReader, converter);
    }

    @Override
    public long getLong() {
      checkValueRead();
      return currentLong;
    }

    @Override
    protected void readCurrentValue() {
      currentLong = dataColumn.readLong();
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(currentLong);
    }

    @Override
    public void writeNextValueToConverter() {
      converter.addLong(getLong());
    }
  }

  private static final class BINARYMemColumnReader extends ColumnReaderImpl {
    private Binary current;

    public BINARYMemColumnReader(ColumnDescriptor path, PageReader pageReader, PrimitiveConverter converter) {
      super(path, pageReader, converter);
    }

    @Override
    public Binary getBinary() {
      checkValueRead();
      return current;
    }

    @Override
    protected void readCurrentValue() {
      current = dataColumn.readBytes();
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(current);
    }

    @Override
    public void writeNextValueToConverter() {
      converter.addBinary(getBinary());
    }
  }

  private static final class BOOLEANMemColumnReader extends ColumnReaderImpl {
    private boolean current;

    public BOOLEANMemColumnReader(ColumnDescriptor path, PageReader pageReader, PrimitiveConverter converter) {
      super(path, pageReader, converter);
    }

    @Override
    public boolean getBoolean() {
      checkValueRead();
      return current;
    }

    @Override
    protected void readCurrentValue() {
      current = dataColumn.readBoolean();
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(current);
    }

    @Override
    public void writeNextValueToConverter() {
      converter.addBoolean(getBoolean());
    }
  }

  private static final class FLOATMemColumnReader extends ColumnReaderImpl {
    private float current;

    public FLOATMemColumnReader(ColumnDescriptor path, PageReader pageReader, PrimitiveConverter converter) {
      super(path, pageReader, converter);
    }

    @Override
    public float getFloat() {
      checkValueRead();
      return current;
    }

    @Override
    protected void readCurrentValue() {
      current = dataColumn.readFloat();
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(current);
    }

    @Override
    public void writeNextValueToConverter() {
      converter.addFloat(getFloat());
    }
  }

  private static final class DOUBLEMemColumnReader extends ColumnReaderImpl {
    private double current;

    public DOUBLEMemColumnReader(ColumnDescriptor path, PageReader pageReader, PrimitiveConverter converter) {
      super(path, pageReader, converter);
    }

    @Override
    public double getDouble() {
      checkValueRead();
      return current;
    }

    @Override
    protected void readCurrentValue() {
      current = dataColumn.readDouble();
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(current);
    }

    @Override
    public void writeNextValueToConverter() {
      converter.addDouble(getDouble());
    }
  }
}