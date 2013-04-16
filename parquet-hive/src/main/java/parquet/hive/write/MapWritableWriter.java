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

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

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

    private final RecordConsumer recordConsumer;
    private final GroupType schema;

    public MapWritableWriter(final RecordConsumer recordConsumer, final GroupType schema) {
        this.recordConsumer = recordConsumer;
        this.schema = schema;
    }

    public void write(final MapWritable map) {
        recordConsumer.startMessage();
        writeMap(map, schema);
        recordConsumer.endMessage();
    }

    private void writeMap(final MapWritable map, final GroupType type) {
        final int fieldCount = type.getFieldCount();
        for (int field = 0; field < fieldCount; ++field) {
            final Type fieldType = type.getType(field);
            final String fieldName = fieldType.getName();
            recordConsumer.startField(fieldName, field);
            if (fieldType.isPrimitive()) {
                final Writable value = map.get(new Text(fieldName));
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
                }
            } else {
                throw new NotImplementedException("Non primitive writing not implemented");
            }
            recordConsumer.endField(fieldName, field);
        }
    }
}
