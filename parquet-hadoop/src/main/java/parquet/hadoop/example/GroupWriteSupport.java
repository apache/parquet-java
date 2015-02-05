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

import static parquet.Preconditions.checkNotNull;
import static parquet.schema.MessageTypeParser.parseMessageType;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

import parquet.Preconditions;
import parquet.example.data.Group;
import parquet.example.data.GroupWriter;
import parquet.hadoop.api.WriteSupport;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

public class GroupWriteSupport extends WriteSupport<Group> {

  public static final String PARQUET_EXAMPLE_SCHEMA = "parquet.example.schema";

  public static void setSchema(MessageType schema, Configuration configuration) {
    configuration.set(PARQUET_EXAMPLE_SCHEMA, schema.toString());
  }

  public static MessageType getSchema(Configuration configuration) {
    return parseMessageType(checkNotNull(configuration.get(PARQUET_EXAMPLE_SCHEMA), PARQUET_EXAMPLE_SCHEMA));
  }

  private MessageType schema;
  private GroupWriter groupWriter;

  @Override
  public parquet.hadoop.api.WriteSupport.WriteContext init(Configuration configuration) {
    schema = getSchema(configuration);
    return new WriteContext(schema, new HashMap<String, String>());
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    groupWriter = new GroupWriter(recordConsumer, schema);
  }

  @Override
  public void write(Group record) {
    groupWriter.write(record);
  }

}
