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

import org.apache.commons.lang.NotImplementedException;
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

    public MapWritableGroupConverter(final GroupType parquetSchema, Map<String, String> keyValueMetaData, MessageType fileSchema) {
        this.parquetSchema = parquetSchema;
        converters = new Converter[fileSchema.getFieldCount()];

        int i = 0;
        for (Type type : fileSchema.getFields()) {
            converters[i] = getConverterFromDescription(type, fileSchema.getFieldName(i));
            ++i;
        }
    }

    private Converter getConverterFromDescription(final Type type, final String fieldName) {
        if (type == null)
            return null;

        if (type.isPrimitive())
            return ETypeConverter.getNewConverter(type.asPrimitiveType().getPrimitiveTypeName().javaType, fieldName, this);
        else
            throw new NotImplementedException("Non primitive converters not implemented");
    }

    final public MapWritable getCurrentMap() {
        if (mapWritable == null)
            mapWritable = new MapWritable();
        mapWritable.clear();
        mapWritable.putAll(currentMap);
        return mapWritable;
    }

    final protected void set(final String fieldName, Writable value) {
        currentMap.put(new Text(fieldName), value);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
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
