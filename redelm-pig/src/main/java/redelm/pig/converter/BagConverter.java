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

import redelm.pig.TupleConversionException;
import redelm.schema.GroupType;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class BagConverter extends Converter {

  private DataBag currentBag;
  private TupleConverter child;

  BagConverter(GroupType redelmSchema, FieldSchema pigSchema, Converter parent) throws FrontendException {
    super(parent);
    child = new TupleConverter(redelmSchema.getType(0).asGroupType(), pigSchema.schema.getField(0).schema, this);
  }

  @Override
  public void start() {
    currentBag = new NonSpillableDataBag();
  }

  @Override
  public void startField(String field, int index) {
    assert index == 0;
  }

  @Override
  public void endField(String field, int index) {
    assert index == 0;
  }

  @Override
  public Converter startGroup() {
    return child;
  }

  @Override
  public void endGroup() {
    currentBag.add(child.get());
  }

  @Override
  public DataBag get() {
    return currentBag;
  }

  @Override
  public void set(Object value) {
    throw new TupleConversionException("bag can not contain primitive value " + value);
  }

}
