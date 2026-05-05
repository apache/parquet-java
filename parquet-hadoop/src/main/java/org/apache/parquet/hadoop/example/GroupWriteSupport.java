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
package org.apache.parquet.hadoop.example;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

public class GroupWriteSupport extends WriteSupport<Group> {

  public static final String PARQUET_EXAMPLE_SCHEMA = "parquet.example.schema";

  public static void setSchema(MessageType schema, Configuration configuration) {
    configuration.set(PARQUET_EXAMPLE_SCHEMA, schema.toString());
  }

  public static MessageType getSchema(Configuration configuration) {
    return getSchema(new HadoopParquetConfiguration(configuration));
  }

  public static MessageType getSchema(ParquetConfiguration configuration) {
    return parseMessageType(
        Objects.requireNonNull(configuration.get(PARQUET_EXAMPLE_SCHEMA), PARQUET_EXAMPLE_SCHEMA));
  }

  private MessageType schema;
  private GroupWriter groupWriter;
  private Map<String, String> extraMetaData;

  public GroupWriteSupport() {
    this(null, new HashMap<String, String>());
  }

  GroupWriteSupport(MessageType schema) {
    this(schema, new HashMap<String, String>());
  }

  GroupWriteSupport(MessageType schema, Map<String, String> extraMetaData) {
    this.schema = schema;
    this.extraMetaData = extraMetaData;
  }

  @Override
  public String getName() {
    return "example";
  }

  @Override
  public org.apache.parquet.hadoop.api.WriteSupport.WriteContext init(Configuration configuration) {
    return init(new HadoopParquetConfiguration(configuration));
  }

  @Override
  public org.apache.parquet.hadoop.api.WriteSupport.WriteContext init(ParquetConfiguration configuration) {
    // if present, prefer the schema passed to the constructor
    if (schema == null) {
      schema = getSchema(configuration);
    }
    return new WriteContext(schema, this.extraMetaData);
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
