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
package parquet.hive.write;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import parquet.Log;
import parquet.hive.writable.BinaryWritable;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.Type;

/**
 *
 * A MapWritableWriter
 * TODO
 *
 *
 * @author Rémy Pecqueur <r.pecqueur@criteo.com>
 *
 */
public class MapWritableWriter {
    private static final Log LOG = Log.getLog(MapWritableWriter.class);

    private final RecordConsumer recordConsumer;
    private final GroupType schema;

    public MapWritableWriter(final RecordConsumer recordConsumer, final GroupType schema) {
        this.recordConsumer = recordConsumer;
        this.schema = schema;
    }

    public void write(final MapWritable map) {
        if (map == null) {
            return;
        }

        recordConsumer.startMessage();
        writeMap(map, schema);
        recordConsumer.endMessage();
    }

    private void writeMap(final MapWritable map, final GroupType type) {
        if (map == null) {
            return;
        }

        final int fieldCount = type.getFieldCount();
        for (int field = 0; field < fieldCount; ++field) {
            final Type fieldType = type.getType(field);
            final String fieldName = fieldType.getName();
            recordConsumer.startField(fieldName, field);
            final Writable value = map.get(new Text(fieldName));

            if (fieldType.isPrimitive()) {
                writePrimitive(value);
            } else {
                recordConsumer.startGroup();

                if (value instanceof MapWritable) {
                    writeMap((MapWritable) value, fieldType.asGroupType());
                } else if (value instanceof ArrayWritable) {
                    writeArray((ArrayWritable) value, fieldType.asGroupType());
                } else if (value != null) {
                    throw new RuntimeException("This should be an ArrayWritable or MapWritable: " + value);
                }

                recordConsumer.endGroup();
            }

            recordConsumer.endField(fieldName, field);
        }
    }

    private void writeArray(final ArrayWritable array, final GroupType type) {
        if (array == null) {
            return;
        }

        final Writable[] subValues = array.get();

        final int fieldCount = type.getFieldCount();
        for (int field = 0; field < fieldCount; ++field) {
            final Type subType = type.getType(field);
            recordConsumer.startField(subType.getName(), field);
            //            LOG.info("writing " + subValues.length + " values for " + type.getName());
            for (int i = 0; i < subValues.length; ++i) {
                final Writable subValue = subValues[i];
                if (subValue != null) {

                    if (subType.isPrimitive()) {
                        if (subValue instanceof MapWritable) {
                            writePrimitive(((MapWritable) subValue).get(new Text(subType.getName())));
                        } else {
                            writePrimitive(subValue);
                        }
                    } else {
                        if (!(subValue instanceof MapWritable)) {
                            throw new RuntimeException("This should be a MapWritable: " + subValue);
                        } else {
                            recordConsumer.startGroup();
                            writeMap((MapWritable) subValue, subType.asGroupType());
                            recordConsumer.endGroup();
                        }
                    }
                }
            }
            recordConsumer.endField(subType.getName(), field);
        }

    }

    private void writePrimitive(final Writable value) {
        if (value == null) {
            return;
        }
        if (value instanceof DoubleWritable) {
            recordConsumer.addDouble(((DoubleWritable) value).get());
        } else if (value instanceof BooleanWritable) {
            recordConsumer.addBoolean(((BooleanWritable) value).get());
        } else if (value instanceof FloatWritable) {
            recordConsumer.addFloat(((FloatWritable) value).get());
        } else if (value instanceof IntWritable) {
            recordConsumer.addInteger(((IntWritable) value).get());
        } else if (value instanceof BinaryWritable) {
            recordConsumer.addBinary(Binary.fromByteArray(((BinaryWritable) value).getBytes()));
        } else {
            throw new RuntimeException("Unknown value type: " + value + " " + value.getClass());
        }
    }
}
