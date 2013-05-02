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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import parquet.io.api.Converter;
import parquet.schema.GroupType;
import parquet.schema.Type;

/**
 *
 * A MapWritableGroupConverter
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 *
 */

public class MapWritableGroupConverter extends HiveGroupConverter {

  private final GroupType groupType;
  private final Converter[] converters;

  private final HiveGroupConverter parent;
  private final int index;
  private final Map<Writable, Writable> currentMap;
  private final Map<Writable, List<Writable>> bufferMap; // Holds values from children arrays
  private MapWritable rootMap;

  public MapWritableGroupConverter(final GroupType groupType) {
    this(groupType, null, 0);
    this.rootMap = new MapWritable();
  }

  public MapWritableGroupConverter(final GroupType groupType, final HiveGroupConverter parent, final int index) {
    this.groupType = groupType;
    this.parent = parent;
    this.index = index;
    this.currentMap = new HashMap<Writable, Writable>();
    this.bufferMap = new HashMap<Writable, List<Writable>>();
    converters = new Converter[this.groupType.getFieldCount()];

    int i = 0;
    for (final Type subtype : this.groupType.getFields()) {
      converters[i] = getConverterFromDescription(subtype, i, this);
      ++i;
    }
  }

  final public MapWritable getCurrentMap() {
    for (final Entry<Writable, List<Writable>> entry : bufferMap.entrySet()) {
      final ArrayWritable arr = new ArrayWritable(Writable.class, entry.getValue().toArray(new Writable[entry.getValue().size()]));
      currentMap.put(entry.getKey(), arr);
    }

    MapWritable mapWritable;
    if (this.rootMap != null) { // We're at the root : we can safely re-use the same map to save perf
      mapWritable = this.rootMap;
      mapWritable.clear();
    } else {
      mapWritable = new MapWritable();
    }

    mapWritable.putAll(currentMap);

    return mapWritable;
  }

  @Override
  final protected void set(final int index, final Writable value) {
    currentMap.put(new Text(groupType.getFieldName(index)), value);
  }

  @Override
  public Converter getConverter(final int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    currentMap.clear();
    bufferMap.clear();
  }

  @Override
  public void end() {
    if (parent != null) {
      parent.set(index, getCurrentMap());
    }
  }

  @Override
  protected void add(final int index, final Writable value) {
    final Text key = new Text(groupType.getFieldName(index));

    if (bufferMap.containsKey(key)) {
      bufferMap.get(key).add(value);
    } else {
      final List<Writable> buffer = new ArrayList<Writable>();
      buffer.add(value);
      bufferMap.put(key, buffer);
    }

  }
}
