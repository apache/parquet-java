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
package org.apache.parquet.proto;


import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;

import java.util.Collections;
import java.util.Map;

/**
 * Converts data content of root message from Protocol Buffer message to parquet message.
 * It delegates conversion of inner fields to {@link ProtoMessageConverter} class using inheritance.
 * Schema is converted in {@link ProtoSchemaConverter} class.
 *
 * @param <T> the Java class of protobuf messages created by this converter
 */
public class ProtoRecordConverter<T extends MessageOrBuilder> extends ProtoMessageConverter {

  private boolean buildBefore;

  /**
   * We dont need to write message value at top level.
   */
  private static class SkipParentValueContainer extends ParentValueContainer {
    @Override
    public void add(Object a) {
      throw new RuntimeException("Should never happen");
    }
  }

  public ProtoRecordConverter(Configuration conf, MessageOrBuilder message, MessageType parquetSchema, Map<String, String> extraMetadata) {
    super(conf, new SkipParentValueContainer(), message, parquetSchema, extraMetadata);
  }

  public ProtoRecordConverter(Configuration conf, Message.Builder builder, MessageType parquetSchema, Map<String, String> extraMetadata) {
    super(conf, new SkipParentValueContainer(), builder, parquetSchema, extraMetadata);
  }

  // Old version constructors, kept for code backward compatibility.
  // The instance will not be able to handle unknowned enum values written by parquet-proto (the behavior before PARQUET-1455)
  @Deprecated
  public ProtoRecordConverter(Class<? extends Message> protoclass, MessageType parquetSchema) {
    super(new Configuration(), new SkipParentValueContainer(), protoclass, parquetSchema, Collections.emptyMap());
  }

  @Deprecated
  public ProtoRecordConverter(Message.Builder builder, MessageType parquetSchema) {
    this(new Configuration(), builder, parquetSchema, Collections.emptyMap());
  }

  @Override
  public void start() {
    this.getBuilder().clear();
    super.start();
  }

  @Override
  public void end() {
    // do nothing, dont call ParentValueContainer at top level.
  }

  public T getCurrentRecord() {
    if (buildBefore) {
      return (T) this.getBuilder().build();
    } else {
      return (T) this.getBuilder();
    }
  }

  /***
   * if buildBefore is true, Protocol Buffer builder is build to message before returning record.
   * @param buildBefore whether to build before
   */
  public void setBuildBefore(boolean buildBefore) {
    this.buildBefore = buildBefore;
  }

}
