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
package parquet.proto;


import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import parquet.schema.MessageType;

/**
 * Converts data content of root message from Protocol Buffer message to parquet message.
 * It delegates conversion of inner fields to {@link ProtoMessageConverter} class using inheritance.
 * Schema is converted in {@link ProtoSchemaConverter} class.
 *
 * @author Lukas Nalezenec
 */
public class ProtoRecordConverter<T extends MessageOrBuilder> extends ProtoMessageConverter {

  private final Message.Builder reusedBuilder;
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


  public ProtoRecordConverter(Class<? extends Message> protoclass, MessageType parquetSchema) {
    super(new SkipParentValueContainer(), protoclass, parquetSchema);
    reusedBuilder = getBuilder();
  }

  public ProtoRecordConverter(Message.Builder builder, MessageType parquetSchema) {
    super(new SkipParentValueContainer(), builder, parquetSchema);
    reusedBuilder = getBuilder();
  }

  @Override
  public void start() {
    reusedBuilder.clear();
    super.start();
  }

  @Override
  public void end() {
    // do nothing, dont call ParentValueContainer at top level.
  }

  public T getCurrentRecord() {
    if (buildBefore) {
      return (T) this.reusedBuilder.build();
    } else {
      return (T) this.reusedBuilder;
    }
  }

  /***
   * if buildBefore is true, Protocol Buffer builder is build to message before returning record.
   */
  public void setBuildBefore(boolean buildBefore) {
    this.buildBefore = buildBefore;
  }
}
