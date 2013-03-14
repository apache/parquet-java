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
package parquet.pig.convert;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import parquet.io.ParquetDecodingException;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.pig.TupleConversionException;
import parquet.schema.GroupType;
import parquet.schema.OriginalType;
import parquet.schema.Type;

public class TupleConverter extends GroupConverter {

  private static final TupleFactory TF = TupleFactory.getInstance();

  private final int schemaSize;

  protected Tuple currentTuple;
  private final Converter[] converters;

  public TupleConverter(GroupType parquetSchema, Schema pigSchema) {
    try {
      this.schemaSize = parquetSchema.getFieldCount();
      if (schemaSize != pigSchema.size()) {
        throw new IllegalArgumentException("schema sizes don't match:\n" + parquetSchema + "\n" + pigSchema);
      }
      this.converters = new Converter[this.schemaSize];
      for (int i = 0; i < schemaSize; i++) {
        FieldSchema field = pigSchema.getField(i);
        Type type = parquetSchema.getType(i);
        final int index = i;
        converters[i] = newConverter(field, type, new ValueContainer() {
          @Override
          void add(Object value) {
            TupleConverter.this.set(index, value);
          }
        });
      }
    } catch (FrontendException e) {
      throw new ParquetDecodingException("can not initialize pig converter from:\n" + parquetSchema + "\n" + pigSchema, e);
    }
  }

  static Converter newConverter(FieldSchema pigField, Type type, final ValueContainer parent)
      throws FrontendException {
    try {
      switch (pigField.type) {
      case DataType.BAG:
        return new BagConverter(type.asGroupType(), pigField, parent);
      case DataType.MAP:
        return new MapConverter(type.asGroupType(), pigField, parent);
      case DataType.TUPLE:
        return new TupleConverter(type.asGroupType(), pigField.schema) {
          @Override
          public void end() {
            super.end();
            parent.add(this.currentTuple);
          }
        };
      case DataType.CHARARRAY:
        return new FieldStringConverter(parent);
      case DataType.BYTEARRAY:
        return new FieldByteArrayConverter(parent);
      case DataType.INTEGER:
        return new FieldIntegerConverter(parent);
      case DataType.BOOLEAN:
        return new FieldBooleanConverter(parent);
      case DataType.FLOAT:
        return new FieldFloatConverter(parent);
      case DataType.DOUBLE:
        return new FieldDoubleConverter(parent);
      case DataType.LONG:
        return new FieldLongConverter(parent);
      default:
        throw new TupleConversionException("unsupported pig type: " + pigField);
      }
    } catch (FrontendException e) {
      throw new TupleConversionException("error while preparing converter for:\n" + pigField + "\n" + type, e);
    } catch (RuntimeException e) {
      throw new TupleConversionException("error while preparing converter for:\n" + pigField + "\n" + type, e);
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  final public void start() {
    currentTuple = TF.newTuple(schemaSize);
  }

  final void set(int fieldIndex, Object value) {
    try {
      currentTuple.set(fieldIndex, value);
    } catch (ExecException e) {
      throw new TupleConversionException(
          "Could not set " + value +
          " to current tuple " + currentTuple + " at " + fieldIndex, e);
    }
  }

  @Override
  public void end() {
  }

  final public Tuple getCurrentTuple() {
    return currentTuple;
  }

  static final class FieldStringConverter extends PrimitiveConverter {

    private final ValueContainer parent;

    public FieldStringConverter(ValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(value.toStringUsingUTF8());
    }

  }

  static final class FieldByteArrayConverter extends PrimitiveConverter {

    private final ValueContainer parent;

    public FieldByteArrayConverter(ValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(new DataByteArray(value.getBytes()));
    }

  }

  static final class FieldDoubleConverter extends PrimitiveConverter {

    private final ValueContainer parent;

    public FieldDoubleConverter(ValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addDouble(double value) {
      parent.add(value);
    }

  }

  static final class FieldFloatConverter extends PrimitiveConverter {

    private final ValueContainer parent;

    public FieldFloatConverter(ValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addFloat(float value) {
      parent.add(value);
    }

  }

  static final class FieldLongConverter extends PrimitiveConverter {

    private final ValueContainer parent;

    public FieldLongConverter(ValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addLong(long value) {
      parent.add(value);
    }

  }

  static final class FieldIntegerConverter extends PrimitiveConverter {

    private final ValueContainer parent;

    public FieldIntegerConverter(ValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBoolean(boolean value) {
      parent.add(value ? 1 : 0);
    }

    @Override
    final public void addInt(int value) {
      parent.add(value);
    }

  }

  static final class FieldBooleanConverter extends PrimitiveConverter {

    private final ValueContainer parent;

    public FieldBooleanConverter(ValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBoolean(boolean value) {
      parent.add(value);
    }

    @Override
    final public void addInt(int value) {
      parent.add(value != 0);
    }

  }

  static class BagConverter extends GroupConverter {

    private final List<Tuple> buffer = new ArrayList<Tuple>();
    private final Converter child;
    private final ValueContainer parent;

    BagConverter(GroupType parquetSchema, FieldSchema pigSchema, ValueContainer parent) throws FrontendException {
      this.parent = parent;
      if (parquetSchema.getFieldCount() != 1) {
        throw new IllegalArgumentException("bags have only one field. " + parquetSchema + " size = " + parquetSchema.getFieldCount());
      }
      Type nestedType = parquetSchema.getType(0);

      ValueContainer childsParent;
      FieldSchema pigField;
      if (nestedType.isPrimitive() || nestedType.getOriginalType() == OriginalType.MAP) {
        // Pig bags always contain tuples
        // In that case we need to wrap the value in an extra tuple
        childsParent = new ValueContainer() {
          @Override
          void add(Object value) {
            buffer.add(TF.newTuple(value));
          }};
        pigField = pigSchema.schema.getField(0).schema.getField(0);
      } else {
        childsParent = new ValueContainer() {
          @Override
          void add(Object value) {
            buffer.add((Tuple)value);
          }};
        pigField = pigSchema.schema.getField(0);
      }
      child = newConverter(pigField, nestedType, childsParent);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      if (fieldIndex != 0) {
        throw new IllegalArgumentException("bags have only one field. can't reach " + fieldIndex);
      }
      return child;
    }

    /** runtime methods */

    @Override
    final public void start() {
      buffer.clear();
    }

    @Override
    public void end() {
      parent.add(new NonSpillableDataBag(new ArrayList<Tuple>(buffer)));
    }

  }

}
