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

import static parquet.bytes.BytesUtils.UTF8;

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

import parquet.io.convert.GroupConverter;
import parquet.io.convert.PrimitiveConverter;
import parquet.pig.TupleConversionException;
import parquet.schema.GroupType;
import parquet.schema.Type;

public class TupleConverter extends GroupConverter {

  private static final TupleFactory TF = TupleFactory.getInstance();

  private final GroupType parquetSchema;
  private final int schemaSize;

  private Tuple currentTuple;
  private final GroupConverter[] groupConverters;
  private final PrimitiveConverter[] primitiveConverters;

  TupleConverter(GroupType parquetSchema, Schema pigSchema) throws FrontendException {
    this.parquetSchema = parquetSchema;
    this.schemaSize = parquetSchema.getFieldCount();
    if (schemaSize != pigSchema.size()) {
      throw new IllegalArgumentException("schema sizes don't match:\n" + parquetSchema + "\n" + pigSchema);
    }
    this.groupConverters = new GroupConverter[this.schemaSize];
    this.primitiveConverters = new PrimitiveConverter[this.schemaSize];
    for (int i = 0; i < schemaSize; i++) {
      FieldSchema field = pigSchema.getField(i);
      Type type = parquetSchema.getType(i);
      switch (field.type) {
      case DataType.BAG:
        groupConverters[i] = new BagConverter(type.asGroupType(), field, i);
        break;
      case DataType.MAP:
        groupConverters[i] = new MapConverter(type.asGroupType(), field, this, i);
        break;
      case DataType.TUPLE:
        final int index = i;
        groupConverters[i] = new TupleConverter(type.asGroupType(), field.schema) {
          @Override
          public void end() {
            super.end();
            set(index, currentTuple);
          }
        };
        break;
      case DataType.CHARARRAY:
        primitiveConverters[i] = new FieldStringConverter(i);
        break;
      case DataType.BYTEARRAY:
        primitiveConverters[i] = new FieldByteArrayConverter(i);
        break;
      default:
        primitiveConverters[i] = new FieldPrimitiveConverter(i);
      }
    }
  }

  @Override
  public GroupConverter getGroupConverter(int fieldIndex) {
    if (fieldIndex < 0 || fieldIndex >= groupConverters.length || groupConverters[fieldIndex] == null) {
      throw new IllegalArgumentException("not the index of a group field in " + parquetSchema + " : " + fieldIndex);
    }
    return groupConverters[fieldIndex];
  }

  @Override
  public PrimitiveConverter getPrimitiveConverter(int fieldIndex) {
    if (fieldIndex < 0 || fieldIndex >= primitiveConverters.length || primitiveConverters[fieldIndex] == null) {
      throw new IllegalArgumentException("not the index of a primitive field in " + parquetSchema + " : " + fieldIndex);
    }
    return primitiveConverters[fieldIndex];
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

  final class FieldStringConverter extends PrimitiveConverter {

    private final int index;

    public FieldStringConverter(int index) {
      this.index = index;
    }

    @Override
    final public void addBinary(byte[] value) {
      set(index, new String(value, UTF8));
    }

  }

  final class FieldByteArrayConverter extends PrimitiveConverter {

    private final int index;

    public FieldByteArrayConverter(int index) {
      this.index = index;
    }

    @Override
    final public void addBinary(byte[] value) {
      set(index, new DataByteArray(value));
    }

  }

  final class FieldPrimitiveConverter extends PrimitiveConverter {

    private final int index;

    public FieldPrimitiveConverter(int index) {
      this.index = index;
    }

    @Override
    final public void addBoolean(boolean value) {
      set(index, value);
    }

    @Override
    final public void addDouble(double value) {
      set(index, value);
    }

    @Override
    final public void addFloat(float value) {
      set(index, value);
    }

    @Override
    final public void addInt(int value) {
      set(index, value);
    }

    @Override
    final public void addLong(long value) {
      set(index, value);
    }

  }
  class BagConverter extends GroupConverter {

    private final List<Tuple> buffer = new ArrayList<Tuple>();
    private final TupleConverter child;
    private final int index;

    BagConverter(GroupType parquetSchema, FieldSchema pigSchema, int index) throws FrontendException {
      this.index = index;
      if (parquetSchema.getFieldCount() != 0) {
        throw new IllegalArgumentException("bags have only one field. " + parquetSchema);
      }
      child = new TupleConverter(parquetSchema.getType(0).asGroupType(), pigSchema.schema.getField(0).schema) {
        public void end() {
          super.end();
          buffer.add(getCurrentTuple());
        }
      };
    }

    @Override
    public GroupConverter getGroupConverter(int fieldIndex) {
      if (fieldIndex != 0) {
        throw new IllegalArgumentException("bags have only one field. can't reach " + fieldIndex);
      }
      return child;
    }

    @Override
    public PrimitiveConverter getPrimitiveConverter(int fieldIndex) {
      throw new UnsupportedOperationException();
    }

    /** runtime methods */

    @Override
    final public void start() {
      buffer.clear();
    }

    @Override
    public void end() {
      set(index, new NonSpillableDataBag(new ArrayList<Tuple>(buffer)));
    }

  }
}
