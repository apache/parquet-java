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
package parquet.tools.read;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import parquet.column.Dictionary;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.GroupType;
import parquet.schema.OriginalType;
import parquet.schema.Type;

/**
 * 
 * 
 * @author 
 */
public class SimpleRecordConverter extends GroupConverter {
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final CharsetDecoder UTF8_DECODER = UTF8.newDecoder();

  private final Converter converters[];
  private final String name;
  private final SimpleRecordConverter parent;
  private SimpleRecord record;

  public SimpleRecordConverter(GroupType schema) {
    this(schema, null, null);
  }

  public SimpleRecordConverter(GroupType schema, String name, SimpleRecordConverter parent) {
    this.converters = new Converter[schema.getFieldCount()];
    this.parent = parent;
    this.name = name;

    int i = 0;
    for (Type field: schema.getFields()) {
      converters[i++] = createConverter(field);
    }
  }

  private Converter createConverter(Type field) {
    if (field.isPrimitive()) {
      OriginalType otype = field.getOriginalType();
      if (otype != null) {
        switch (otype) {
          case MAP: break;
          case LIST: break;
          case UTF8: return new StringConverter(field.getName());
          case MAP_KEY_VALUE: break;
          case ENUM: break;
        }
      }

      return new SimplePrimitiveConverter(field.getName());
    }

    return new SimpleRecordConverter(field.asGroupType(), field.getName(), this);
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  SimpleRecord getCurrentRecord() {
    return record;
  }

  @Override
  public void start() {
    record = new SimpleRecord();
  }

  @Override
  public void end() {
    if (parent != null) {
      parent.getCurrentRecord().add(name, record);
    }
  }

  private class SimplePrimitiveConverter extends PrimitiveConverter {
    protected final String name;

    public SimplePrimitiveConverter(String name) {
      this.name = name;
    }

    @Override
    public void addBinary(Binary value) {
      byte[] data = value.getBytes();
      if (data == null) {
        record.add(name, null);
        return;
      }

      if (data != null) {
        try {
          CharBuffer buffer = UTF8_DECODER.decode(value.toByteBuffer());
          record.add(name, buffer.toString());
          return;
        } catch (Throwable th) {
        }
      }

      record.add(name, value.getBytes());
    }

    @Override
    public void addBoolean(boolean value) {
      record.add(name, value);
    }

    @Override
    public void addDouble(double value) {
      record.add(name, value);
    }

    @Override
    public void addFloat(float value) {
      record.add(name, value);
    }

    @Override
    public void addInt(int value) {
      record.add(name, value);
    }

    @Override
    public void addLong(long value) {
      record.add(name, value);
    }
  }

  private class StringConverter extends SimplePrimitiveConverter {
    public StringConverter(String name) {
      super(name);
    }

    @Override
    public void addBinary(Binary value) {
      record.add(name, value.toStringUsingUTF8());
    }
  }
}

