/**
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
package parquet.avro;

import com.google.common.collect.Lists;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.node.NullNode;

public class AvroTestUtil {

  public static Schema record(String name, Schema.Field... fields) {
    Schema record = Schema.createRecord(name, null, null, false);
    record.setFields(Arrays.asList(fields));
    return record;
  }

  public static Schema.Field field(String name, Schema schema) {
    return new Schema.Field(name, schema, null, null);
  }

  public static Schema.Field optionalField(String name, Schema schema) {
    return new Schema.Field(name, optional(schema), null, NullNode.getInstance());
  }

  public static Schema array(Schema element) {
    return Schema.createArray(element);
  }

  public static Schema primitive(Schema.Type type) {
    return Schema.create(type);
  }

  public static Schema optional(Schema original) {
    return Schema.createUnion(Lists.newArrayList(
        Schema.create(Schema.Type.NULL),
        original));
  }

  public static GenericRecord instance(Schema schema, Object... pairs) {
    if ((pairs.length % 2) != 0) {
      throw new RuntimeException("Not enough values");
    }
    GenericRecord record = new GenericData.Record(schema);
    for (int i = 0; i < pairs.length; i += 2) {
      record.put(pairs[i].toString(), pairs[i + 1]);
    }
    return record;
  }

}
