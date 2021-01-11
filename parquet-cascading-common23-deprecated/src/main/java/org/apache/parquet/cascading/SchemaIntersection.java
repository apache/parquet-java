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
package org.apache.parquet.cascading;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import cascading.tuple.Fields;

import java.util.List;
import java.util.ArrayList;

public class SchemaIntersection {

  private final MessageType requestedSchema;
  private final Fields sourceFields;

  public SchemaIntersection(MessageType fileSchema, Fields requestedFields) {
    if(requestedFields == Fields.UNKNOWN)
      requestedFields = Fields.ALL;

    Fields newFields = Fields.NONE;
    List<Type> newSchemaFields = new ArrayList<Type>();
    int schemaSize = fileSchema.getFieldCount();

    for (int i = 0; i < schemaSize; i++) {
      Type type = fileSchema.getType(i);
      Fields name = new Fields(type.getName());

      if(requestedFields.contains(name)) {
        newFields = newFields.append(name);
        newSchemaFields.add(type);
      }
    }

    this.sourceFields = newFields;
    this.requestedSchema = new MessageType(fileSchema.getName(), newSchemaFields);
  }

  public MessageType getRequestedSchema() {
    return requestedSchema;
  }

  public Fields getSourceFields() {
    return sourceFields;
  }
}
