/**
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
package org.apache.parquet.avro;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.PrimitiveStringifier;
import org.apache.parquet.schema.PrimitiveType;

public class AvroConverters {

  public abstract static class AvroGroupConverter extends GroupConverter {
    protected final ParentValueContainer parent;

    public AvroGroupConverter(ParentValueContainer parent) {
      this.parent = parent;
    }
  }

  static class AvroPrimitiveConverter extends PrimitiveConverter {
    protected final ParentValueContainer parent;

    public AvroPrimitiveConverter(ParentValueContainer parent) {
      this.parent = parent;
    }
  }

  abstract static class BinaryConverter<T> extends AvroPrimitiveConverter {
    private T[] dict = null;

    public BinaryConverter(ParentValueContainer parent) {
      super(parent);
    }

    public abstract T convert(Binary binary);

    @Override
    public void addBinary(Binary value) {
      parent.add(convert(value));
    }

    @Override
    public boolean hasDictionarySupport() {
      return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setDictionary(Dictionary dictionary) {
      dict = (T[]) new Object[dictionary.getMaxId() + 1];
      for (int i = 0; i <= dictionary.getMaxId(); i++) {
        dict[i] = convert(dictionary.decodeToBinary(i));
      }
    }

    public T prepareDictionaryValue(T value) {
      return value;
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
      parent.add(prepareDictionaryValue(dict[dictionaryId]));
    }
  }

  static final class FieldByteConverter extends AvroPrimitiveConverter {
    public FieldByteConverter(ParentValueContainer parent) {
      super(parent);
    }

    @Override
    public void addInt(int value) {
      parent.addByte((byte) value);
    }
  }

  static final class FieldShortConverter extends AvroPrimitiveConverter {
    public FieldShortConverter(ParentValueContainer parent) {
      super(parent);
    }

    @Override
    public void addInt(int value) {
      parent.addShort((short) value);
    }
  }

  static final class FieldCharConverter extends AvroPrimitiveConverter {
    public FieldCharConverter(ParentValueContainer parent) {
      super(parent);
    }

    @Override
    public void addInt(int value) {
      parent.addChar((char) value);
    }
  }

  static final class FieldBooleanConverter extends AvroPrimitiveConverter {
    public FieldBooleanConverter(ParentValueContainer parent) {
      super(parent);
    }

    @Override
    final public void addBoolean(boolean value) {
      parent.addBoolean(value);
    }
  }

  static final class FieldIntegerConverter extends AvroPrimitiveConverter {
    public FieldIntegerConverter(ParentValueContainer parent) {
      super(parent);
    }

    @Override
    final public void addInt(int value) {
      parent.addInt(value);
    }
  }

  static final class FieldLongConverter extends AvroPrimitiveConverter {
    public FieldLongConverter(ParentValueContainer parent) {
      super(parent);
    }

    @Override
    final public void addInt(int value) {
      parent.addLong((long) value);
    }

    @Override
    final public void addLong(long value) {
      parent.addLong(value);
    }
  }

  static final class FieldFloatConverter extends AvroPrimitiveConverter {
    public FieldFloatConverter(ParentValueContainer parent) {
      super(parent);
    }

    @Override
    final public void addInt(int value) {
      parent.addFloat((float) value);
    }

    @Override
    final public void addLong(long value) {
      parent.addFloat((float) value);
    }

    @Override
    final public void addFloat(float value) {
      parent.addFloat(value);
    }
  }

  static final class FieldDoubleConverter extends AvroPrimitiveConverter {
    public FieldDoubleConverter(ParentValueContainer parent) {
      super(parent);
    }

    @Override
    final public void addInt(int value) {
      parent.addDouble((double) value);
    }

    @Override
    final public void addLong(long value) {
      parent.addDouble((double) value);
    }

    @Override
    final public void addFloat(float value) {
      parent.addDouble((double) value);
    }

    @Override
    final public void addDouble(double value) {
      parent.addDouble(value);
    }
  }

  static final class FieldByteArrayConverter extends BinaryConverter<byte[]> {
    public FieldByteArrayConverter(ParentValueContainer parent) {
      super(parent);
    }

    @Override
    public byte[] convert(Binary binary) {
      return binary.getBytes();
    }
  }

  static final class FieldByteBufferConverter extends BinaryConverter<ByteBuffer> {
    public FieldByteBufferConverter(ParentValueContainer parent) {
      super(parent);
    }

    @Override
    public ByteBuffer convert(Binary binary) {
      return ByteBuffer.wrap(binary.getBytes());
    }

    @Override
    public ByteBuffer prepareDictionaryValue(ByteBuffer value) {
      return value.duplicate();
    }
  }

  static final class FieldStringConverter extends BinaryConverter<String> {
    public FieldStringConverter(ParentValueContainer parent) {
      super(parent);
    }

    @Override
    public String convert(Binary binary) {
      return binary.toStringUsingUTF8();
    }
  }

  static final class FieldUTF8Converter extends BinaryConverter<Utf8> {
    public FieldUTF8Converter(ParentValueContainer parent) {
      super(parent);
    }

    @Override
    public Utf8 convert(Binary binary) {
      return new Utf8(binary.getBytes());
    }
  }

  static final class FieldStringableConverter extends BinaryConverter<Object> {
    private final String stringableName;
    private final Constructor<?> ctor;

    public FieldStringableConverter(ParentValueContainer parent,
                                    Class<?> stringableClass) {
      super(parent);
      stringableName = stringableClass.getName();
      try {
        this.ctor = stringableClass.getConstructor(String.class);
      } catch (NoSuchMethodException e) {
        throw new ParquetDecodingException(
            "Unable to get String constructor for " + stringableName, e);
      }
    }

    @Override
    public Object convert(Binary binary) {
      try {
        return ctor.newInstance(binary.toStringUsingUTF8());
      } catch (InstantiationException | IllegalAccessException
          | InvocationTargetException e) {
        throw new ParquetDecodingException(
            "Cannot convert binary to " + stringableName, e);
      }
    }
  }

  static final class FieldEnumConverter extends BinaryConverter<Object> {
    private final Schema schema;
    private final GenericData model;

    public FieldEnumConverter(ParentValueContainer parent, Schema enumSchema,
                              GenericData model) {
      super(parent);
      this.schema = enumSchema;
      this.model = model;
    }

    @Override
    public Object convert(Binary binary) {
      return model.createEnum(binary.toStringUsingUTF8(), schema);
    }
  }

  static final class FieldFixedConverter extends BinaryConverter<Object> {
    private final Schema schema;
    private final GenericData model;

    public FieldFixedConverter(ParentValueContainer parent, Schema avroSchema,
                               GenericData model) {
      super(parent);
      this.schema = avroSchema;
      this.model = model;
    }

    @Override
    public Object convert(Binary binary) {
      return model.createFixed(null /* reuse */, binary.getBytes(), schema);
    }
  }

  static final class FieldUUIDConverter extends BinaryConverter<String> {
    private final PrimitiveStringifier stringifier;

    public FieldUUIDConverter(ParentValueContainer parent, PrimitiveType type) {
      super(parent);
      stringifier = type.stringifier();
    }

    @Override
    public String convert(Binary binary) {
      return stringifier.stringify(binary);
    }
  }
}
