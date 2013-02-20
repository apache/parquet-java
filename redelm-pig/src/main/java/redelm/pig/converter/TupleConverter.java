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
package redelm.pig.converter;

import java.io.UnsupportedEncodingException;

import scala.annotation.target.field;
import scala.collection.mutable.StringBuilder;

import redelm.pig.TupleConversionException;
import redelm.schema.GroupType;
import redelm.schema.Type;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class TupleConverter extends Converter {

  private static final TupleFactory TF = TupleFactory.getInstance();

  private Tuple currentTuple;
  private int schemaSize;
  private int currentField;
  private Converter[] converters;
  private FieldSchema[] fields;

  TupleConverter(GroupType redelmSchema, Schema pigSchema, Converter parent) throws FrontendException {
    super(parent);
    this.schemaSize = redelmSchema.getFieldCount();
    this.converters = new Converter[this.schemaSize];
    this.fields = new FieldSchema[this.schemaSize];
    for (int i = 0; i < converters.length; i++) {
      FieldSchema field = pigSchema.getField(i);
      fields[i] = field;
      Type type = redelmSchema.getType(i);
      switch (field.type) {
      case DataType.BAG:
        converters[i] = new BagConverter(type.asGroupType(), field, this);
        break;
      case DataType.MAP:
        converters[i] = new MapConverter(type.asGroupType(), field, this);
        break;
      case DataType.TUPLE:
        converters[i] = new TupleConverter(type.asGroupType(), field.schema, this);
        break;
      default:
        converters[i] = null;
      }
    }
  }

  @Override
  public Converter startGroup() {
    return converters[currentField];
  }

  @Override
  public void endGroup() {
    try {
      currentTuple.set(currentField, converters[currentField].get());
    } catch (ExecException e) {
      throw new TupleConversionException(
          "Could not set the child value to the currentTuple " + currentTuple +
          " at " + currentField, e);
    }
  }

  @Override
  public void startField(String field, int index) {
    this.currentField = index;
  }

  @Override
  public void endField(String field, int index) {
    this.currentField = -1;
  }

  @Override
  public void start() {
    currentTuple = TF.newTuple(schemaSize);
  }

  @Override
  public Tuple get() {
    return currentTuple;
  }

  @Override
  public void set(Object value) {
    try {
      if (fields[currentField].type == DataType.CHARARRAY) {
        try {
          currentTuple.set(currentField, new String((byte[])value, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
          throw new RuntimeException(e);
        }
      } else {
        currentTuple.set(currentField, value);
      }

    } catch (ExecException e) {
      throw new TupleConversionException(
          "Could not set " + value +
          " to current tuple " + currentTuple + " at " + currentField, e);
    }
  }

  @Override
  public void toString(String indent, StringBuffer sb) {
    sb.append(indent).append(getClass().getSimpleName()).append("{\n");
    for (Converter converter : converters) {
      if (converter == null) {
        sb.append(indent).append("primitive\n");
      } else {
        converter.toString(" " + indent, sb);
        sb.append("\n");
      }
    }
    sb.append("\n").append(indent).append("}");
  }
}
