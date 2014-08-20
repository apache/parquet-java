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
package parquet.pojo.writer;

import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.pojo.field.FieldAccessor;

/**
 * Writers for primitives and their object equivalents
 */
public class PrimitiveWriters {
  public static class BooleanWriter implements RecordWriter {
    @Override
    public void writeValue(Object value, RecordConsumer recordConsumer) {
      recordConsumer.addBoolean((Boolean) value);
    }

    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      Boolean value = (Boolean) fieldAccessor.get(parent);

      if (value == null) {
        return;
      }

      recordConsumer.startField(null, index);
      recordConsumer.addBoolean(value);
      recordConsumer.endField(null, index);
    }
  }

  public static class PrimitiveBooleanWriter extends BooleanWriter {
    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      recordConsumer.startField(null, index);
      recordConsumer.addBoolean(fieldAccessor.getBoolean(parent));
      recordConsumer.endField(null, index);
    }
  }

  public static class ByteWriter implements RecordWriter {
    @Override
    public void writeValue(Object value, RecordConsumer recordConsumer) {
      recordConsumer.addInteger((Byte) value);
    }

    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      Byte value = (Byte) fieldAccessor.get(parent);

      if (value == null) {
        return;
      }

      recordConsumer.startField(null, index);
      recordConsumer.addInteger(value);
      recordConsumer.endField(null, index);
    }
  }

  public static class PrimitiveByteWriter extends ByteWriter {
    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      recordConsumer.startField(null, index);
      recordConsumer.addInteger(fieldAccessor.getByte(parent));
      recordConsumer.endField(null, index);
    }
  }

  public static class ShortWriter implements RecordWriter {
    @Override
    public void writeValue(Object value, RecordConsumer recordConsumer) {
      recordConsumer.addInteger((Short) value);
    }

    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      Short value = (Short) fieldAccessor.get(parent);

      if (value == null) {
        return;
      }

      recordConsumer.startField(null, index);
      recordConsumer.addInteger(value);
      recordConsumer.endField(null, index);
    }
  }

  public static class PrimitiveShortWriter extends ShortWriter {
    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      recordConsumer.startField(null, index);
      recordConsumer.addInteger(fieldAccessor.getShort(parent));
      recordConsumer.endField(null, index);
    }
  }

  public static class CharWriter implements RecordWriter {
    @Override
    public void writeValue(Object value, RecordConsumer recordConsumer) {
      recordConsumer.addInteger((Character) value);
    }

    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      Character value = (Character) fieldAccessor.get(parent);

      if (value == null) {
        return;
      }

      recordConsumer.startField(null, index);
      recordConsumer.addInteger(value);
      recordConsumer.endField(null, index);
    }
  }

  public static class PrimitiveCharWriter extends CharWriter {
    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      recordConsumer.startField(null, index);
      recordConsumer.addInteger(fieldAccessor.getChar(parent));
      recordConsumer.endField(null, index);
    }
  }

  public static class IntWriter implements RecordWriter {
    @Override
    public void writeValue(Object value, RecordConsumer recordConsumer) {
      recordConsumer.addInteger((Integer) value);
    }

    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      Integer value = (Integer) fieldAccessor.get(parent);

      if (value == null) {
        return;
      }

      recordConsumer.startField(null, index);
      recordConsumer.addInteger(value);
      recordConsumer.endField(null, index);
    }
  }

  public static class PrimitiveIntWriter extends IntWriter {
    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      recordConsumer.startField(null, index);
      recordConsumer.addInteger(fieldAccessor.getInt(parent));
      recordConsumer.endField(null, index);
    }
  }

  public static class LongWriter implements RecordWriter {
    @Override
    public void writeValue(Object value, RecordConsumer recordConsumer) {
      recordConsumer.addLong((Long) value);
    }

    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      Long value = (Long) fieldAccessor.get(parent);

      if (value == null) {
        return;
      }

      recordConsumer.startField(null, index);
      recordConsumer.addLong(value);
      recordConsumer.endField(null, index);
    }
  }

  public static class PrimitiveLongWriter extends LongWriter {
    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      recordConsumer.startField(null, index);
      recordConsumer.addLong(fieldAccessor.getLong(parent));
      recordConsumer.endField(null, index);
    }
  }

  public static class FloatWriter implements RecordWriter {
    @Override
    public void writeValue(Object value, RecordConsumer recordConsumer) {
      recordConsumer.addFloat((Float) value);
    }

    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      Float value = (Float) fieldAccessor.get(parent);

      if (value == null) {
        return;
      }

      recordConsumer.startField(null, index);
      recordConsumer.addFloat(value);
      recordConsumer.endField(null, index);
    }
  }

  public static class PrimitiveFloatWriter extends FloatWriter {
    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      recordConsumer.startField(null, index);
      recordConsumer.addFloat(fieldAccessor.getFloat(parent));
      recordConsumer.endField(null, index);
    }
  }

  public static class DoubleWriter implements RecordWriter {
    @Override
    public void writeValue(Object value, RecordConsumer recordConsumer) {
      recordConsumer.addDouble((Double) value);
    }

    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      Double value = (Double) fieldAccessor.get(parent);

      if (value == null) {
        return;
      }

      recordConsumer.startField(null, index);
      recordConsumer.addDouble(value);
      recordConsumer.endField(null, index);
    }
  }

  public static class PrimitiveDoubleWriter extends DoubleWriter {
    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      recordConsumer.startField(null, index);
      recordConsumer.addDouble(fieldAccessor.getDouble(parent));
      recordConsumer.endField(null, index);
    }
  }

  public static class EnumWriter implements RecordWriter {
    @Override
    public void writeValue(Object value, RecordConsumer recordConsumer) {
      recordConsumer.addBinary(Binary.fromString(((Enum) value).name()));
    }

    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      Enum value = (Enum) fieldAccessor.get(parent);

      if (value == null) {
        return;
      }

      recordConsumer.startField(null, index);
      recordConsumer.addBinary(Binary.fromString(value.name()));
      recordConsumer.endField(null, index);
    }
  }

  public static class ByteArrayWriter implements RecordWriter {
    @Override
    public void writeValue(Object value, RecordConsumer recordConsumer) {
      byte[] bytes = (byte[]) value;

      recordConsumer.addBinary(Binary.fromByteArray(bytes));
    }

    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      byte[] value = (byte[]) fieldAccessor.get(parent);

      if (value == null) {
        return;
      }

      recordConsumer.startField(null, index);
      recordConsumer.addBinary(Binary.fromByteArray(value));
      recordConsumer.endField(null, index);
    }
  }

  public static class StringWriter implements RecordWriter {
    @Override
    public void writeValue(Object value, RecordConsumer recordConsumer) {
      String string = (String) value;

      recordConsumer.addBinary(Binary.fromString(string));
    }

    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      String value = (String) fieldAccessor.get(parent);

      if (value == null) {
        return;
      }

      recordConsumer.startField(null, index);
      recordConsumer.addBinary(Binary.fromString(value));
      recordConsumer.endField(null, index);
    }
  }
}
