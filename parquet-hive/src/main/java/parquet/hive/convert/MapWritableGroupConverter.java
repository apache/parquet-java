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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import parquet.Log;
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
    private static final Log LOG = Log.getLog(MapWritableGroupConverter.class);

    private final GroupType groupType;
    private final Converter[] converters;
    private final HiveGroupConverter parent;
    private final int index;
    private final Map<Writable, Writable> currentMap;
    private MapWritable mapWritable = null;

    public MapWritableGroupConverter(final GroupType groupType) {
        this(groupType, null, 0);
    }

    public MapWritableGroupConverter(final GroupType groupType, final HiveGroupConverter parent, final int index) {
        this.groupType = groupType;
        this.parent = parent;
        this.index = index;
        this.currentMap = new HashMap<Writable, Writable>();
        converters = new Converter[this.groupType.getFieldCount()];

        int i = 0;
        for (final Type subtype : this.groupType.getFields()) {
            converters[i] = getConverterFromDescription(subtype, i, this);
            ++i;
        }
    }

    final public MapWritable getCurrentMap() {
        if (mapWritable == null) {
            mapWritable = new MapWritable();
        }
        mapWritable.clear();
        mapWritable.putAll(currentMap);
        return mapWritable;
    }

    @Override
    final protected void set(final int index, final Writable value) {
        LOG.info("current group name: " + groupType.getName());
        LOG.info("setting " + value + " at index " + index + " in map " + currentMap);
        currentMap.put(new Text(groupType.getFieldName(index)), value);
    }

    @Override
    public Converter getConverter(final int fieldIndex) {
        return converters[fieldIndex];
    }

    @Override
    public void start() {
        // LOG.info("starting for group: " + groupType);
        currentMap.clear();
    }

    @Override
    public void end() {
        if (parent != null) {
            parent.set(index, getCurrentMap());
        }
    }
}
