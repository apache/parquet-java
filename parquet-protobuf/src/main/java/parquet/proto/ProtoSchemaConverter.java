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

import static parquet.schema.OriginalType.ENUM;
import static parquet.schema.OriginalType.UTF8;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import java.util.List;

import parquet.Log;
import parquet.schema.MessageType;
import parquet.schema.Type;
import parquet.schema.Types;
import parquet.schema.Types.Builder;
import parquet.schema.Types.GroupBuilder;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;

/**
 * <p/>
 * Converts a Protocol Buffer Descriptor into a Parquet schema.
 *
 * @author Lukas Nalezenec
 */
public class ProtoSchemaConverter {

  private static final Log LOG = Log.getLog(ProtoSchemaConverter.class);

  public MessageType convert(Class<? extends Message> protobufClass) {
    LOG.debug("Converting protocol buffer class \"" + protobufClass + "\" to parquet schema.");
    Descriptors.Descriptor descriptor = Protobufs.getMessageDescriptor(protobufClass);
    MessageType messageType =
        convertFields(Types.buildMessage(), descriptor.getFields())
        .named(descriptor.getFullName());
    LOG.debug("Converter info:\n " + descriptor.toProto() + " was converted to \n" + messageType);
    return messageType;
  }

  /* Iterates over list of fields. **/
  private <T> GroupBuilder<T> convertFields(GroupBuilder<T> groupBuilder, List<Descriptors.FieldDescriptor> fieldDescriptors) {
    for (Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {
      groupBuilder =
          addField(fieldDescriptor, groupBuilder)
          .id(fieldDescriptor.getNumber())
          .named(fieldDescriptor.getName());
    }
    return groupBuilder;
  }

  private Type.Repetition getRepetition(Descriptors.FieldDescriptor descriptor) {
    if (descriptor.isRequired()) {
      return Type.Repetition.REQUIRED;
    } else if (descriptor.isRepeated()) {
      return Type.Repetition.REPEATED;
    } else {
      return Type.Repetition.OPTIONAL;
    }
  }

  private <T> Builder<? extends Builder<?, GroupBuilder<T>>, GroupBuilder<T>> addField(Descriptors.FieldDescriptor descriptor, GroupBuilder<T> builder) {
    Type.Repetition repetition = getRepetition(descriptor);
    JavaType javaType = descriptor.getJavaType();
    switch (javaType) {
    case BOOLEAN : return builder.primitive(BOOLEAN, repetition);
    case INT : return builder.primitive(INT32, repetition);
    case LONG : return builder.primitive(INT64, repetition);
    case FLOAT : return builder.primitive(FLOAT, repetition);
    case DOUBLE: return builder.primitive(DOUBLE, repetition);
    case BYTE_STRING: return builder.primitive(BINARY, repetition);
    case STRING: return builder.primitive(BINARY, repetition).as(UTF8);
    case MESSAGE: {
      GroupBuilder<GroupBuilder<T>> group = builder.group(repetition);
      convertFields(group, descriptor.getMessageType().getFields());
      return group;
    }
    case ENUM: return builder.primitive(BINARY, repetition).as(ENUM);
    default:
      throw new UnsupportedOperationException("Cannot convert Protocol Buffer: unknown type " + javaType);
    }
  }

}