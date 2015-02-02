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
package parquet.hadoop.example;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import parquet.example.data.Group;
import parquet.example.data.simple.convert.GroupRecordConverter;
import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

public class GroupReadSupport extends ReadSupport<Group> {

  @Override
  public parquet.hadoop.api.ReadSupport.ReadContext init(
      Configuration configuration, Map<String, String> keyValueMetaData,
      MessageType fileSchema) {
    String partialSchemaString = configuration.get(ReadSupport.PARQUET_READ_SCHEMA);
    MessageType requestedProjection = getSchemaForRead(fileSchema, partialSchemaString);
    return new ReadContext(requestedProjection);
  }

  @Override
  public RecordMaterializer<Group> prepareForRead(Configuration configuration,
      Map<String, String> keyValueMetaData, MessageType fileSchema,
      parquet.hadoop.api.ReadSupport.ReadContext readContext) {
    return new GroupRecordConverter(readContext.getRequestedSchema());
  }

}
