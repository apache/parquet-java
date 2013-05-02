/**
 * Copyright 2013 Criteo.
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
package parquet.hive.convert;

import org.apache.hadoop.io.Writable;

import parquet.io.ParquetDecodingException;
import parquet.io.api.Converter;
import parquet.schema.GroupType;

/**
 *
 * A ArrayWritableGroupConverter
 *
 *
 * @author Rémy Pecqueur <r.pecqueur@criteo.com>
 *
 */
public class ArrayWritableGroupConverter extends HiveGroupConverter {

  private final Converter[] converters;
  private final HiveGroupConverter parent;
  private final int index;
  private Writable currentValue;

  public ArrayWritableGroupConverter(final GroupType groupType, final HiveGroupConverter parent, final int index) {
    this.parent = parent;
    this.index = index;

    if (groupType.getFieldCount() == 2) {
      final MapWritableGroupConverter intermediateConverter = new MapWritableGroupConverter(groupType, this, 0);
      converters = new Converter[groupType.getFieldCount()];
      converters[0] = getConverterFromDescription(groupType.getType(0), 0, intermediateConverter);
      converters[1] = getConverterFromDescription(groupType.getType(1), 1, intermediateConverter);
    } else if (groupType.getFieldCount() == 1) {
      converters = new Converter[1];
      converters[0] = getConverterFromDescription(groupType.getType(0), 0, this);
    } else {
      throw new RuntimeException("Invalid parquet hive schema: " + groupType);
    }

  }

  @Override
  public Converter getConverter(final int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
  }

  @Override
  public void end() {
    parent.add(index, currentValue);
  }

  @Override
  protected void set(final int index, final Writable value) {
    if (index != 0) {
      throw new ParquetDecodingException("Repeated group can only have one field. Not allowed to set for the index : " + index);
    }
    currentValue = value;
  }

  @Override
  protected void add(final int index, final Writable value) {
    set(index, value);
  }
}
