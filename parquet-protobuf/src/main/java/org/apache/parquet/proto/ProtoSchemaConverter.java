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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.Builder;
import org.apache.parquet.schema.Types.GroupBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

/**
 * Converts a Protocol Buffer Descriptor into a Parquet schema.
 */
public class ProtoSchemaConverter {

  private static final Logger LOG = LoggerFactory.getLogger(ProtoSchemaConverter.class);
  private final boolean parquetSpecsCompliant;
  private final List<FieldDescriptor> kafkaMetadataFields;

  public ProtoSchemaConverter() {
    this(false);
  }

  public ProtoSchemaConverter(boolean parquetSpecsCompliant) {
    this(parquetSpecsCompliant, new ArrayList<>());
  }

  /**
   * Instantiate a schema converter to get the parquet schema corresponding to protobuf classes.
   * @param parquetSpecsCompliant   If set to false, the parquet schema generated will be using the old
   *                                schema style (prior to PARQUET-968) to provide backward-compatibility
   *                                but which does not use LIST and MAP wrappers around collections as required
   *                                by the parquet specifications. If set to true, specs compliant schemas are used.
   * @param kafkaMetadataFields     Kafka metadata fields to be serialized in parquet
   */
  public ProtoSchemaConverter(boolean parquetSpecsCompliant, List<FieldDescriptor> kafkaMetadataFields) {
    this.parquetSpecsCompliant = parquetSpecsCompliant;
    this.kafkaMetadataFields = kafkaMetadataFields;
  }

  public MessageType convert(Class<? extends Message> protobufClass) {
    LOG.debug("Converting protocol buffer class \"" + protobufClass + "\" to parquet schema.");
    Descriptors.Descriptor descriptor = Protobufs.getMessageDescriptor(protobufClass);
    MessageType messageType =
        convertFields(Types.buildMessage(), descriptor.getFields())
        .named(descriptor.getFullName());
    LOG.debug("Converter info:\n " + descriptor.toProto() + " was converted to \n" + messageType);
    return messageType;
  }

  public MessageType convert(Descriptors.Descriptor descriptor) {
    LOG.debug("Converting protocol buffer class to parquet schema using descriptors." + descriptor);
    List<FieldDescriptor> fields = new ArrayList<>(descriptor.getFields());
    GroupBuilder<MessageType> messageTypeGroupBuilder = convertFields(Types.buildMessage(), fields);
    if (kafkaMetadataFields.size() != 0) {
      messageTypeGroupBuilder = convertFields(messageTypeGroupBuilder, new ArrayList<>(kafkaMetadataFields));
    }
    MessageType messageType =
      messageTypeGroupBuilder
        .named(descriptor.getFullName());
    LOG.debug("Converter info:\n " + descriptor.toProto() + " was converted to \n" + messageType);
    return messageType;
  }

  /* Iterates over list of fields. **/
  private <T> GroupBuilder<T> convertFields(GroupBuilder<T> groupBuilder, List<FieldDescriptor> fieldDescriptors) {
    for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
      groupBuilder =
          addField(fieldDescriptor, groupBuilder)
          .id(fieldDescriptor.getNumber())
          .named(fieldDescriptor.getName());
    }
    return groupBuilder;
  }

  private Type.Repetition getRepetition(FieldDescriptor descriptor) {
    if (descriptor.isRequired()) {
      return Type.Repetition.REQUIRED;
    } else if (descriptor.isRepeated()) {
      return Type.Repetition.REPEATED;
    } else {
      return Type.Repetition.OPTIONAL;
    }
  }

  private boolean isStructType(FieldDescriptor descriptor) {
    return descriptor.getJavaType() == JavaType.MESSAGE && descriptor.getMessageType().getFullName().equals(com.google.protobuf.Struct.getDescriptor().getFullName());
  }

  private boolean isTimestampType(FieldDescriptor fieldDescriptor) {
    return fieldDescriptor.getJavaType() == JavaType.MESSAGE && fieldDescriptor.getMessageType().getFullName().equals(com.google.protobuf.Timestamp.getDescriptor().getFullName());
  }

  private <T> Builder<? extends Builder<?, GroupBuilder<T>>, GroupBuilder<T>> addField(FieldDescriptor descriptor, final GroupBuilder<T> builder) {
    if (descriptor.getJavaType() == JavaType.MESSAGE && !isStructType(descriptor) && !isTimestampType(descriptor)) {
      return addMessageField(descriptor, builder);
    }

    ParquetType parquetType = getParquetType(descriptor);
    if (descriptor.isRepeated() && parquetSpecsCompliant) {
      // the old schema style did not include the LIST wrapper around repeated fields
      return addRepeatedPrimitive(parquetType.primitiveType, parquetType.logicalTypeAnnotation, builder);
    }

    return builder.primitive(parquetType.primitiveType, getRepetition(descriptor)).as(parquetType.logicalTypeAnnotation);
  }

  private <T> Builder<? extends Builder<?, GroupBuilder<T>>, GroupBuilder<T>> addRepeatedPrimitive(PrimitiveTypeName primitiveType,
                                                                                                   LogicalTypeAnnotation logicalTypeAnnotation,
                                                                                                   final GroupBuilder<T> builder) {
    return builder
        .group(Type.Repetition.OPTIONAL).as(listType())
          .group(Type.Repetition.REPEATED)
            .primitive(primitiveType, Type.Repetition.REQUIRED).as(logicalTypeAnnotation)
          .named("element")
        .named("list");
  }

  private <T> GroupBuilder<GroupBuilder<T>> addRepeatedMessage(FieldDescriptor descriptor, GroupBuilder<T> builder) {
    GroupBuilder<GroupBuilder<GroupBuilder<GroupBuilder<T>>>> result =
      builder
        .group(Type.Repetition.OPTIONAL).as(listType())
        .group(Type.Repetition.REPEATED)
        .group(Type.Repetition.OPTIONAL);

    convertFields(result, descriptor.getMessageType().getFields());

    return result.named("element").named("list");
  }

  private <T> GroupBuilder<GroupBuilder<T>> addMessageField(FieldDescriptor descriptor, final GroupBuilder<T> builder) {
    if (descriptor.isMapField() && parquetSpecsCompliant) {
      // the old schema style did not include the MAP wrapper around map groups
      return addMapField(descriptor, builder);
    }
    if (descriptor.isRepeated() && parquetSpecsCompliant) {
      // the old schema style did not include the LIST wrapper around repeated messages
      return addRepeatedMessage(descriptor, builder);
    }

    // Plain message
    GroupBuilder<GroupBuilder<T>> group = builder.group(getRepetition(descriptor));
    convertFields(group, descriptor.getMessageType().getFields());
    return group;
  }

  private <T> GroupBuilder<GroupBuilder<T>> addMapField(FieldDescriptor descriptor, final GroupBuilder<T> builder) {
    List<FieldDescriptor> fields = descriptor.getMessageType().getFields();
    if (fields.size() != 2) {
      throw new UnsupportedOperationException("Expected two fields for the map (key/value), but got: " + fields);
    }

    ParquetType mapKeyParquetType = getParquetType(fields.get(0));

    GroupBuilder<GroupBuilder<GroupBuilder<T>>> group = builder
      .group(Type.Repetition.OPTIONAL).as(mapType()) // only optional maps are allowed in Proto3
      .group(Type.Repetition.REPEATED) // key_value wrapper
      .primitive(mapKeyParquetType.primitiveType, Type.Repetition.REQUIRED).as(mapKeyParquetType.logicalTypeAnnotation).named("key");

    return addField(fields.get(1), group).named("value")
      .named("key_value");
  }

  private ParquetType getParquetType(FieldDescriptor fieldDescriptor) {
    // consider timestamp fields with LogicalAnnotation of TIMESTAMP_MILLIS
    // This facilitates using timestamp fields as TIMESTAMP type in bigquery rather than nested type.
    // More info: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet#type_conversions
    if(isTimestampType(fieldDescriptor)) {
      return ParquetType.of(INT64, timestampType(true, TimeUnit.MILLIS));
    }
    // consider Struct as a string
    else if(isStructType(fieldDescriptor)) {
      return ParquetType.of(BINARY, stringType());
    }

    JavaType javaType = fieldDescriptor.getJavaType();
    switch (javaType) {
      case INT: return ParquetType.of(INT32);
      case LONG: return ParquetType.of(INT64);
      case DOUBLE: return ParquetType.of(DOUBLE);
      case BOOLEAN: return ParquetType.of(BOOLEAN);
      case FLOAT: return ParquetType.of(FLOAT);
      case STRING: return ParquetType.of(BINARY, stringType());
      // changing the Annotation of Enum from enumType() to stringType()
      // since Bigquery load job considers Enum as bytes type otherwise
      case ENUM: return ParquetType.of(BINARY, stringType());
      case BYTE_STRING: return ParquetType.of(BINARY);
      default:
        throw new UnsupportedOperationException("Cannot convert Protocol Buffer: unknown type " + javaType);
    }
  }

  private static class ParquetType {
    PrimitiveTypeName primitiveType;
    LogicalTypeAnnotation logicalTypeAnnotation;

    private ParquetType(PrimitiveTypeName primitiveType, LogicalTypeAnnotation logicalTypeAnnotation) {
      this.primitiveType = primitiveType;
      this.logicalTypeAnnotation = logicalTypeAnnotation;
    }

    public static ParquetType of(PrimitiveTypeName primitiveType, LogicalTypeAnnotation logicalTypeAnnotation) {
      return new ParquetType(primitiveType, logicalTypeAnnotation);
    }

    public static ParquetType of(PrimitiveTypeName primitiveType) {
      return of(primitiveType, null);
    }
  }

}
