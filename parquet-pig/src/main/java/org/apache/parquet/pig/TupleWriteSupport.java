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
package org.apache.parquet.pig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

public class TupleWriteSupport extends WriteSupport<Tuple> {
  private static final TupleFactory TF = TupleFactory.getInstance();
  private static final PigSchemaConverter pigSchemaConverter = new PigSchemaConverter(false);

  public static TupleWriteSupport fromPigSchema(String pigSchemaString) throws ParserException {
    return new TupleWriteSupport(Utils.getSchemaFromString(pigSchemaString));
  }

  private RecordConsumer recordConsumer;
  private MessageType rootSchema;
  private Schema rootPigSchema;

  /**
   * @param pigSchema the pigSchema
   */
  public TupleWriteSupport(Schema pigSchema) {
    super();
    this.rootSchema = pigSchemaConverter.convert(pigSchema);
    this.rootPigSchema = pigSchema;
  }

  @Override
  public String getName() {
    return "pig";
  }

  public Schema getPigSchema() {
    return rootPigSchema;
  }

  public MessageType getParquetSchema() {
    return rootSchema;
  }

  @Override
  public WriteContext init(Configuration configuration) {
    Map<String, String> extraMetaData = new HashMap<String, String>();
    new PigMetaData(rootPigSchema).addToMetaData(extraMetaData);
    return new WriteContext(rootSchema, extraMetaData);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  public void write(Tuple t) {
    try {
      recordConsumer.startMessage();
      writeTuple(rootSchema, rootPigSchema, t);
      recordConsumer.endMessage();
    } catch (ExecException | FrontendException e) {
      throw new RuntimeException(e);
    }
  }

  private void writeTuple(GroupType schema, Schema pigSchema, Tuple t) throws ExecException, FrontendException {
    List<Type> fields = schema.getFields();
    List<FieldSchema> pigFields = pigSchema.getFields();
    assert fields.size() == pigFields.size();
    for (int i = 0; i < fields.size(); i++) {
      if (t.isNull(i)) {
        continue;
      }
      Type fieldType = fields.get(i);
      recordConsumer.startField(fieldType.getName(), i);
      FieldSchema pigType = pigFields.get(i);
      switch (pigType.type) {
      case DataType.BAG:
        Type bagType = fieldType.asGroupType().getType(0);
        FieldSchema pigBagInnerType = pigType.schema.getField(0);
        DataBag bag = (DataBag)t.get(i);
        recordConsumer.startGroup();
        if (bag.size() > 0) {
          recordConsumer.startField(bagType.getName(), 0);
          for (Tuple tuple : bag) {
            if (bagType.isPrimitive()) {
              writeValue(bagType, pigBagInnerType, tuple, 0);
            } else {
              recordConsumer.startGroup();
              writeTuple(bagType.asGroupType(), pigBagInnerType.schema, tuple);
              recordConsumer.endGroup();
            }
          }
          recordConsumer.endField(bagType.getName(), 0);
        }
        recordConsumer.endGroup();
        break;
      case DataType.MAP:
        Type mapType = fieldType.asGroupType().getType(0);
        FieldSchema pigMapInnerType = pigType.schema.getField(0);
        @SuppressWarnings("unchecked") // I know
        Map<String, Object> map = (Map<String, Object>)t.get(i);
        recordConsumer.startGroup();
        if (map.size() > 0) {
          recordConsumer.startField(mapType.getName(), 0);
          Set<Entry<String, Object>> entrySet = map.entrySet();
          for (Entry<String, Object> entry : entrySet) {
            recordConsumer.startGroup();
            Schema keyValueSchema = new Schema(Arrays.asList(new FieldSchema("key", DataType.CHARARRAY), new FieldSchema("value", pigMapInnerType.schema, pigMapInnerType.type)));
            writeTuple(mapType.asGroupType(), keyValueSchema, TF.newTuple(Arrays.asList(entry.getKey(), entry.getValue())));
            recordConsumer.endGroup();
          }
          recordConsumer.endField(mapType.getName(), 0);
        }
        recordConsumer.endGroup();
        break;
      default:
        writeValue(fieldType, pigType, t, i);
        break;
      }
      recordConsumer.endField(fieldType.getName(), i);
    }
  }

  private void writeValue(Type type, FieldSchema pigType, Tuple t, int i) {
    try {
      if (type.isPrimitive()) {
        switch (type.asPrimitiveType().getPrimitiveTypeName()) {
        // TODO: use PrimitiveTuple accessors
        case BINARY:
          byte[] bytes;
          if (pigType.type == DataType.BYTEARRAY) {
            bytes = ((DataByteArray)t.get(i)).get();
          } else if (pigType.type == DataType.CHARARRAY) {
            bytes = ((String)t.get(i)).getBytes("UTF-8");
          } else {
            throw new UnsupportedOperationException("can not convert from " + DataType.findTypeName(pigType.type) + " to BINARY ");
          }
          recordConsumer.addBinary(Binary.fromReusedByteArray(bytes));
          break;
        case BOOLEAN:
          recordConsumer.addBoolean((Boolean)t.get(i));
          break;
        case INT32:
          recordConsumer.addInteger(((Number)t.get(i)).intValue());
          break;
        case INT64:
          recordConsumer.addLong(((Number)t.get(i)).longValue());
          break;
        case DOUBLE:
          recordConsumer.addDouble(((Number)t.get(i)).doubleValue());
          break;
        case FLOAT:
          recordConsumer.addFloat(((Number)t.get(i)).floatValue());
          break;
        default:
          throw new UnsupportedOperationException(type.asPrimitiveType().getPrimitiveTypeName().name());
        }
      } else {
        assert pigType.type == DataType.TUPLE;
        recordConsumer.startGroup();
        writeTuple(type.asGroupType(), pigType.schema, (Tuple)t.get(i));
        recordConsumer.endGroup();
      }
    } catch (Exception e) {
      throw new ParquetEncodingException("can not write value at " + i + " in tuple " + t + " from type '" + pigType + "' to type '" + type +"'", e);
    }
  }

}
