/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package parquet.cascading;

import cascading.tuple.TupleEntry;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.api.WriteSupport;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

/**
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 */
public class TupleWriteSupport extends WriteSupport<TupleEntry> {

  private RecordConsumer recordConsumer;
  private MessageType rootSchema;
  public static final String PARQUET_CASCADING_SCHEMA = "parquet.cascading.schema";

  @Override
  public WriteContext init(Configuration configuration) {
    String schema = configuration.get(PARQUET_CASCADING_SCHEMA);
    rootSchema = MessageTypeParser.parseMessageType(schema);
    return new WriteContext(rootSchema, new HashMap<String, String>());
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(TupleEntry record) {
    recordConsumer.startMessage();
    final List<Type> fields = rootSchema.getFields();

    for (int i = 0; i < fields.size(); i++) {
      Type field = fields.get(i);

      if (record == null || record.getObject(field.getName()) == null) {
        continue;
      }
      recordConsumer.startField(field.getName(), i);
      if (field.isPrimitive()) {
        writePrimitive(record, field.asPrimitiveType());
      } else {
        throw new UnsupportedOperationException("Complex type not implemented");
      }
      recordConsumer.endField(field.getName(), i);
    }
    recordConsumer.endMessage();
  }

  private void writePrimitive(TupleEntry record, PrimitiveType field) {
    switch (field.getPrimitiveTypeName()) {
      case BINARY:
        recordConsumer.addBinary(Binary.fromString(record.getString(field.getName())));
        break;
      case BOOLEAN:
        recordConsumer.addBoolean(record.getBoolean(field.getName()));
        break;
      case INT32:
        recordConsumer.addInteger(record.getInteger(field.getName()));
        break;
      case INT64:
        recordConsumer.addLong(record.getLong(field.getName()));
        break;
      case DOUBLE:
        recordConsumer.addDouble(record.getDouble(field.getName()));
        break;
      case FLOAT:
        recordConsumer.addFloat(record.getFloat(field.getName()));
        break;
      case FIXED_LEN_BYTE_ARRAY:
        throw new UnsupportedOperationException("Fixed len byte array type not implemented");
      case INT96:
        throw new UnsupportedOperationException("Int96 type not implemented");
      default:
        throw new UnsupportedOperationException(field.getName() + " type not implemented");
    }
  }
}
