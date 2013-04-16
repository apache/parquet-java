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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.Type;

/**
 *
 * A MapWritableGroupConverter
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 *
 */
public class MapWritableGroupConverter extends GroupConverter {
    @SuppressWarnings("unused")
    private final GroupType parquetSchema;

    private MapWritable mapWritable = null;
    private final Converter[] converters;
    private Map<Writable, Writable> currentMap;
    static final Log LOG = LogFactory.getLog(MapWritableGroupConverter.class);
    public MapWritableGroupConverter(final GroupType requestedSchema, final Map<String, String> keyValueMetaData, final MessageType fileSchema) {
        LOG.error("Group Converter");
        LOG.error("Group Converter : fileschema " + fileSchema);
        LOG.error("Group Converter + parquetSchema " + requestedSchema);
        this.parquetSchema = requestedSchema;
        converters = new Converter[requestedSchema.getFieldCount()];

        int i = 0;
        for (final Type type : requestedSchema.getFields()) {
            converters[i] = getConverterFromDescription(type, requestedSchema.getFieldName(i));
            ++i;
        }
    }

    private Converter getConverterFromDescription(final Type type, final String fieldName) {
        if (type == null) {
            return null;
        }

        if (type.isPrimitive()) {
            return ETypeConverter.getNewConverter(type.asPrimitiveType().getPrimitiveTypeName().javaType, fieldName, this);
        } else {
            //throw new NotImplementedException("Non primitive converters not implemented");
            return null;
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

    final protected void set(final String fieldName, final Writable value) {
        currentMap.put(new Text(fieldName), value);
    }

    @Override
    public Converter getConverter(final int fieldIndex) {
        return converters[fieldIndex];
    }

    @Override
    public void start() {
        currentMap = new HashMap<Writable, Writable>();
    }

    @Override
    public void end() {
    }
}
