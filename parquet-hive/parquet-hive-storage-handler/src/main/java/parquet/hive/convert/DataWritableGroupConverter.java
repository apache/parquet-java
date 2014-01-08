/**
 * Copyright 2013 Criteo.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License
 * at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package parquet.hive.convert;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import parquet.io.api.Converter;
import parquet.schema.GroupType;
import parquet.schema.Type;

/**
 *
 * A MapWritableGroupConverter, real converter between hive and parquet types recursively for complex types.
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 *
 */
public class DataWritableGroupConverter extends HiveGroupConverter {

  private final Converter[] converters;
  private final HiveGroupConverter parent;
  private final int index;
  private final Writable[] currentArr;
  private Writable[] rootMap;

  public DataWritableGroupConverter(final GroupType requestedSchema, final GroupType tableSchema) {
    this(requestedSchema, null, 0, tableSchema);
    final int fieldCount = tableSchema.getFieldCount();
    this.rootMap = new Writable[fieldCount];
  }

  public DataWritableGroupConverter(final GroupType groupType, final HiveGroupConverter parent, final int index) {
    this(groupType, parent, index, groupType);
  }

  public DataWritableGroupConverter(final GroupType selectedGroupType, final HiveGroupConverter parent, final int index, final GroupType containingGroupType) {
    this.parent = parent;
    this.index = index;
    final int totalFieldCount = containingGroupType.getFieldCount();
    final int selectedFieldCount = selectedGroupType.getFieldCount();

    currentArr = new Writable[totalFieldCount];
    converters = new Converter[selectedFieldCount];

    int i = 0;
    for (final Type subtype : selectedGroupType.getFields()) {
      if (containingGroupType.getFields().contains(subtype)) {
        converters[i] = getConverterFromDescription(subtype, containingGroupType.getFieldIndex(subtype.getName()), this);
      } else {
        throw new RuntimeException("Group type [" + containingGroupType + "] does not contain requested field: " + subtype);
      }
      ++i;
    }
  }

  final public ArrayWritable getCurrentArray() {
    return new ArrayWritable(Writable.class, currentArr);
  }

  @Override
  final protected void set(final int index, final Writable value) {
    currentArr[index] = value;
  }

  @Override
  public Converter getConverter(final int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    for (int i = 0; i < currentArr.length; i++) {
      currentArr[i] = null;
    }
  }

  @Override
  public void end() {
    if (parent != null) {
      parent.set(index, getCurrentArray());
    }
  }

  @Override
  protected void add(final int index, final Writable value) {

    if (currentArr[index] != null) {
      ((ListWritable) currentArr[index]).add(value);
    } else {
      ListWritable buffer = new ListWritable();
      buffer.add(value);
      currentArr[index] = buffer;
    }

  }

  private static class ListWritable extends ArrayWritable {

    private List<Writable> buffer = new ArrayList<Writable>();

    public ListWritable() {
      super(Writable.class);
    }

    public void add(Writable value) {
      buffer.add(value);
    }

    @Override
    public Writable[] get() {
      Writable[] array = super.get();
      if (array == null) {
        array = buffer.toArray(new Writable[buffer.size()]);
        super.set(array);
      }
      return array;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      get();
      super.write(out);
    }
  }

}
