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

import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;

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

  static final class FieldByteArrayConverter extends AvroPrimitiveConverter {
    public FieldByteArrayConverter(ParentValueContainer parent) {
      super(parent);
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(value.getBytes());
    }
  }

  static final class FieldByteBufferConverter extends AvroPrimitiveConverter {
    public FieldByteBufferConverter(ParentValueContainer parent) {
      super(parent);
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(ByteBuffer.wrap(value.getBytes()));
    }
  }

  static final class FieldStringConverter extends AvroPrimitiveConverter {
    // TODO: dictionary support should be generic and provided by a parent
    // TODO: this always produces strings, but should respect avro.java.string
    private String[] dict;

    public FieldStringConverter(ParentValueContainer parent) {
      super(parent);
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(value.toStringUsingUTF8());
    }

    @Override
    public boolean hasDictionarySupport() {
      return true;
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
      dict = new String[dictionary.getMaxId() + 1];
      for (int i = 0; i <= dictionary.getMaxId(); i++) {
        dict[i] = dictionary.decodeToBinary(i).toStringUsingUTF8();
      }
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
      parent.add(dict[dictionaryId]);
    }
  }

  static final class FieldEnumConverter extends AvroPrimitiveConverter {
    private final Schema schema;
    private final GenericData model;

    public FieldEnumConverter(ParentValueContainer parent, Schema enumSchema,
                              GenericData model) {
      super(parent);
      this.schema = enumSchema;
      this.model = model;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(model.createEnum(value.toStringUsingUTF8(), schema));
    }
  }

  static final class FieldFixedConverter extends AvroPrimitiveConverter {
    private final Schema schema;
    private final GenericData model;

    public FieldFixedConverter(ParentValueContainer parent, Schema avroSchema,
                               GenericData model) {
      super(parent);
      this.schema = avroSchema;
      this.model = model;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(model.createFixed(null /* reuse */, value.getBytes(), schema));
    }
  }
}
