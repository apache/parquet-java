/**
 * Copyright 2013 Lukas Nalezenec
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
package parquet.proto;


import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import parquet.proto.converters.ParentValueContainer;
import parquet.schema.MessageType;

/**
 * Converts data content of root message from protobuffer message to parquet message.
 * It delegates conversion of inner fields to ProtoMessageConverter class using inheritance.
 * Schema is converted in ProtoSchemaConverter class.
 *
 * @author Lukas Nalezenec
 */
class ProtobufferRecordConverter<T extends MessageOrBuilder> extends parquet.proto.converters.ProtoMessageConverter {

  final Message.Builder reusedBuilder;
  boolean buildBefore;

  /**
   * We dont need to write message value at top level.
   */
  private static class SkipParentValueContainer extends ParentValueContainer {
    @Override
    public void add(Object a) {
      throw new RuntimeException("Should never happen");
    }
  }


  public ProtobufferRecordConverter(Class<? extends Message> protoclass, MessageType parquetSchema) {
    super(new SkipParentValueContainer(), protoclass, parquetSchema);
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

  T getCurrentRecord() {
    if (buildBefore) {
      return (T) this.reusedBuilder.build();
    } else {
      return (T) this.reusedBuilder;
    }
  }

}
