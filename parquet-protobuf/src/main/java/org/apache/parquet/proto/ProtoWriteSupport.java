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

import com.google.protobuf.*;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


import static java.util.Optional.ofNullable;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

import static org.apache.parquet.proto.ProtoConstants.METADATA_ENUM_ITEM_SEPARATOR;
import static org.apache.parquet.proto.ProtoConstants.METADATA_ENUM_KEY_VALUE_SEPARATOR;
import static org.apache.parquet.proto.ProtoConstants.METADATA_ENUM_PREFIX;

/**
 * Implementation of {@link WriteSupport} for writing Protocol Buffers.
 */
public class ProtoWriteSupport<T> extends WriteSupport<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ProtoWriteSupport.class);
  public static final String PB_CLASS_WRITE = "parquet.proto.writeClass";
  // PARQUET-968 introduces changes to allow writing specs compliant schemas with parquet-protobuf.
  // In the past, collection were not written using the LIST and MAP wrappers and thus were not compliant
  // with the parquet specs. This flag, is set to true, allows to write using spec compliant schemas
  // but is set to false by default to keep backward compatibility
  public static final String PB_SPECS_COMPLIANT_WRITE = "parquet.proto.writeSpecsCompliant";

  private boolean writeSpecsCompliant = false;
  private RecordConsumer recordConsumer;
  private ProtoDescriptorSupport protoDescriptorSupport;
  private MessageWriter messageWriter;
  private List<FieldDescriptor> kafkaMetadataFields;
  // Keep protobuf enum value with number in the metadata, so that in read time, a reader can read at least
  // the number back even with an outdated schema which might not contain all enum values.
  private Map<String, Map<String, Integer>> protoEnumBookKeeper = new HashMap<>();

  public ProtoWriteSupport() {
    Descriptors.Descriptor protoDescriptor = null;
    this.protoDescriptorSupport = new ProtoDescriptorSupport(protoDescriptor);
  }

  public ProtoWriteSupport(Class<? extends Message> protobufClass) {
    this.protoDescriptorSupport = new ProtoDescriptorSupport(protobufClass);
  }

  public ProtoWriteSupport(Descriptors.Descriptor messageDescriptor, List<FieldDescriptor> kafkaMetadataFields) {
    this.protoDescriptorSupport = new ProtoDescriptorSupport(messageDescriptor);
    this.kafkaMetadataFields = kafkaMetadataFields;
  }

  public ProtoWriteSupport(Descriptors.Descriptor messageDescriptor) {
    this(messageDescriptor, new ArrayList<>());
  }

  @Override
  public String getName() {
    return "protobuf";
  }

  public static void setSchema(Configuration configuration, Class<? extends Message> protoClass) {
    configuration.setClass(PB_CLASS_WRITE, protoClass, Message.class);
  }

  /**
   * Make parquet-protobuf use the LIST and MAP wrappers for collections. Set to false if you need backward
   * compatibility with parquet before PARQUET-968 (1.9.0 and older).
   * @param configuration           The hadoop configuration
   * @param writeSpecsCompliant     If set to true, the old schema style will be used (without wrappers).
   */
  public static void setWriteSpecsCompliant(Configuration configuration, boolean writeSpecsCompliant) {
    configuration.setBoolean(PB_SPECS_COMPLIANT_WRITE, writeSpecsCompliant);
  }

  /**
   * Writes Protocol buffer to parquet file.
   * @param record instance of Message.Builder or Message.
   * */
  @Override
  public void write(T record) {
    recordConsumer.startMessage();
    try {
      if(record instanceof List) {
        for(Object obj : (List)record) {
          messageWriter.writeTopLevelMessage(obj);
        }
      } else {
        messageWriter.writeTopLevelMessage(record);
      }
    } catch (RuntimeException e) {
      if(record instanceof List) {
        for(Object obj : (List)record) {
          Message m = (obj instanceof Message.Builder) ? ((Message.Builder) obj).build() : (Message) obj;
          LOG.error("Cannot write message " + e.getMessage() + ":" + m);
        }
        throw e;
      }
      Message m = (record instanceof Message.Builder) ? ((Message.Builder) record).build() : (Message) record;
      LOG.error("Cannot write message " + e.getMessage() + " : " + m);
      throw e;
    }
    recordConsumer.endMessage();
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public WriteContext init(Configuration configuration) {
    Descriptor messageDescriptor = protoDescriptorSupport.getMessageDescriptor(configuration);
    writeSpecsCompliant = configuration.getBoolean(PB_SPECS_COMPLIANT_WRITE, writeSpecsCompliant);
    MessageType rootSchema = new ProtoSchemaConverter(writeSpecsCompliant, kafkaMetadataFields).convert(messageDescriptor);
    validatedMapping(messageDescriptor, rootSchema);

    this.messageWriter = new MessageWriter(messageDescriptor, rootSchema, kafkaMetadataFields);

    Map<String, String> extraMetaData = new HashMap<String, String>();
    extraMetaData.put(ProtoReadSupport.PB_CLASS, createMessageClassName(messageDescriptor));
    extraMetaData.put(ProtoReadSupport.PB_DESCRIPTOR, serializeDescriptor(messageDescriptor));
    extraMetaData.put(PB_SPECS_COMPLIANT_WRITE, String.valueOf(writeSpecsCompliant));
    return new WriteContext(rootSchema, extraMetaData);
  }


  private String createMessageClassName(Descriptors.Descriptor messageDescriptor) {
    String messageClassName = messageDescriptor.getFile().getOptions().getJavaPackage() + "."
        + messageDescriptor.getFullName().replace(".", "$");
    return messageClassName;
  }

  @Override
  public FinalizedWriteContext finalizeWrite() {
    Map<String, String> protoMetadata = enumMetadata();
    return new FinalizedWriteContext(protoMetadata);
  }

  private Map<String, String> enumMetadata() {
    Map<String, String> enumMetadata = new HashMap<>();
    for (Map.Entry<String, Map<String, Integer>> enumNameNumberMapping : protoEnumBookKeeper.entrySet()) {
      StringBuilder nameNumberPairs = new StringBuilder();
      if (enumNameNumberMapping.getValue().isEmpty()) {
        // No enum is ever written to any column of this file, put an empty string as the value in the metadata
        LOG.info("No enum is written for " + enumNameNumberMapping.getKey());
      }
      int idx = 0;
      for (Map.Entry<String, Integer> nameNumberPair : enumNameNumberMapping.getValue().entrySet()) {
        nameNumberPairs.append(nameNumberPair.getKey())
          .append(METADATA_ENUM_KEY_VALUE_SEPARATOR)
          .append(nameNumberPair.getValue());
        idx ++;
        if (idx < enumNameNumberMapping.getValue().size()) {
          nameNumberPairs.append(METADATA_ENUM_ITEM_SEPARATOR);
        }
      }
      enumMetadata.put(METADATA_ENUM_PREFIX + enumNameNumberMapping.getKey(), nameNumberPairs.toString());
    }
    return enumMetadata;
  }

  class FieldWriter {
    String fieldName;
    int index = -1;

     void setFieldName(String fieldName) {
      this.fieldName = fieldName;
    }

    /** sets index of field inside parquet message.*/
     void setIndex(int index) {
      this.index = index;
    }

    /** Used for writing repeated fields*/
     void writeRawValue(Object value) {

    }

    /** Used for writing nonrepeated (optional, required) fields*/
    void writeField(Object value) {
      if (!(this instanceof ProtoWriteSupport.MapWriter)) {
        recordConsumer.startField(fieldName, index);
      }
      writeRawValue(value);
      if (!(this instanceof ProtoWriteSupport.MapWriter)) {
        recordConsumer.endField(fieldName, index);
      }
    }
  }

  class MessageWriter extends FieldWriter {

    final FieldWriter[] fieldWriters;
    final List<FieldDescriptor> msgWriterKafkaMetadataFields;

    @SuppressWarnings("unchecked")
    MessageWriter(Descriptor descriptor, GroupType schema, List<FieldDescriptor> kafkaMetadataFields) {
      this.msgWriterKafkaMetadataFields = kafkaMetadataFields;
      List<FieldDescriptor> fields = Stream.concat(descriptor.getFields().stream(), kafkaMetadataFields.stream())
        .collect(Collectors.toList());
      fieldWriters = (FieldWriter[]) Array.newInstance(FieldWriter.class, fields.size());

      for (FieldDescriptor fieldDescriptor: fields) {
        String name = fieldDescriptor.getName();
        Type type = schema.getType(name);
        FieldWriter writer = createWriter(fieldDescriptor, type);

        if(writeSpecsCompliant && fieldDescriptor.isRepeated() && !fieldDescriptor.isMapField()) {
          writer = new ArrayWriter(writer);
        }
        else if (!writeSpecsCompliant && fieldDescriptor.isRepeated()) {
          // the old schemas style used to write maps as repeated fields instead of wrapping them in a LIST
          writer = new RepeatedWriter(writer);
        }

        writer.setFieldName(name);
        writer.setIndex(schema.getFieldIndex(name));

        fieldWriters[getIndex(fieldDescriptor)] = writer;
      }
    }

    private int getIndex(FieldDescriptor field) {
      if(msgWriterKafkaMetadataFields.size() == 0) {
        return field.getIndex();
      }
      for(FieldDescriptor metadataFd: msgWriterKafkaMetadataFields) {
        if(field.getNumber() == metadataFd.getNumber()) {
          return field.getIndex();
        }
      }
      return field.getIndex() + msgWriterKafkaMetadataFields.size();
    }

    private boolean isStructType(FieldDescriptor descriptor) {
      return descriptor.getJavaType() == JavaType.MESSAGE && descriptor.getMessageType().getFullName().equals(com.google.protobuf.Struct.getDescriptor().getFullName());
    }

    private boolean isTimestampType(FieldDescriptor fieldDescriptor) {
      return fieldDescriptor.getJavaType() == JavaType.MESSAGE && fieldDescriptor.getMessageType().getFullName().equals(com.google.protobuf.Timestamp.getDescriptor().getFullName());
    }

    private FieldWriter createWriter(FieldDescriptor fieldDescriptor, Type type) {
      // consider timestamp fields with LogicalAnnotation of TIMESTAMP_MILLIS
      // This facilitates using timestamp fields as TIMESTAMP type in bigquery rather than nested type.
      // More info: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet#type_conversions
      if(isTimestampType(fieldDescriptor)) {
        return new TimestampWriter();
      }
      // Write Struct field as string, since recursive data types are not supported by parquet writer
      else if (isStructType(fieldDescriptor)) {
        return new StringWriter();
      }

      switch (fieldDescriptor.getJavaType()) {
        case STRING: return new StringWriter() ;
        case MESSAGE: return createMessageWriter(fieldDescriptor, type);
        case INT: return new IntWriter();
        case LONG: return new LongWriter();
        case FLOAT: return new FloatWriter();
        case DOUBLE: return new DoubleWriter();
        case ENUM: return new EnumWriter(fieldDescriptor.getEnumType());
        case BOOLEAN: return new BooleanWriter();
        case BYTE_STRING: return new BinaryWriter();
      }

      return unknownType(fieldDescriptor);//should not be executed, always throws exception.
    }

    private FieldWriter createMessageWriter(FieldDescriptor fieldDescriptor, Type type) {
      if (fieldDescriptor.isMapField() && writeSpecsCompliant) {
        return createMapWriter(fieldDescriptor, type);
      }

      return new MessageWriter(fieldDescriptor.getMessageType(), getGroupType(type), new ArrayList<>());
    }

    private GroupType getGroupType(Type type) {
      LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
      if (logicalTypeAnnotation == null) {
        return type.asGroupType();
      }
      return logicalTypeAnnotation.accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<GroupType>() {
        @Override
        public Optional<GroupType> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
          return ofNullable(type.asGroupType().getType("list").asGroupType().getType("element").asGroupType());
        }

        @Override
        public Optional<GroupType> visit(LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
          return ofNullable(type.asGroupType().getType("key_value").asGroupType().getType("value").asGroupType());
        }
      }).orElse(type.asGroupType());
    }

    private MapWriter createMapWriter(FieldDescriptor fieldDescriptor, Type type) {
      List<FieldDescriptor> fields = fieldDescriptor.getMessageType().getFields();
      if (fields.size() != 2) {
        throw new UnsupportedOperationException("Expected two fields for the map (key/value), but got: " + fields);
      }

      // KeyFieldWriter
      FieldDescriptor keyProtoField = fields.get(0);
      FieldWriter keyWriter = createWriter(keyProtoField, type);
      keyWriter.setFieldName(keyProtoField.getName());
      keyWriter.setIndex(0);

      // ValueFieldWriter
      FieldDescriptor valueProtoField = fields.get(1);
      FieldWriter valueWriter = createWriter(valueProtoField, type);
      valueWriter.setFieldName(valueProtoField.getName());
      valueWriter.setIndex(1);

      return new MapWriter(keyWriter, valueWriter);
    }

    /** Writes top level message. It cannot call startGroup() */
    void writeTopLevelMessage(Object value) {
      writeAllFields((MessageOrBuilder) value);
    }

    /** Writes message as part of repeated field. It cannot start field*/
    @Override
    final void writeRawValue(Object value) {
      recordConsumer.startGroup();
      writeAllFields((MessageOrBuilder) value);
      recordConsumer.endGroup();
    }

    /** Used for writing nonrepeated (optional, required) fields*/
    @Override
    final void writeField(Object value) {
      recordConsumer.startField(fieldName, index);
      writeRawValue(value);
      recordConsumer.endField(fieldName, index);
    }

    private void writeAllFields(MessageOrBuilder pb) {
      Descriptor messageDescriptor = pb.getDescriptorForType();
      Descriptors.FileDescriptor.Syntax syntax = messageDescriptor.getFile().getSyntax();

      if (Descriptors.FileDescriptor.Syntax.PROTO2.equals(syntax)) {
        //returns changed fields with values. Map is ordered by id.
        Map<FieldDescriptor, Object> changedPbFields = pb.getAllFields();

        for (Map.Entry<FieldDescriptor, Object> entry : changedPbFields.entrySet()) {
          FieldDescriptor fieldDescriptor = entry.getKey();

          if(fieldDescriptor.isExtension()) {
            // Field index of an extension field might overlap with a base field.
            throw new UnsupportedOperationException(
              "Cannot convert Protobuf message with extension field(s)");
          }

          int fieldIndex = getIndex(fieldDescriptor);
          fieldWriters[fieldIndex].writeField(entry.getValue());
        }
      } else if (Descriptors.FileDescriptor.Syntax.PROTO3.equals(syntax)) {
        List<FieldDescriptor> fieldDescriptors = messageDescriptor.getFields();
        for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
          FieldDescriptor.Type type = fieldDescriptor.getType();
          if (!fieldDescriptor.isRepeated() && FieldDescriptor.Type.MESSAGE.equals(type) && !pb.hasField(fieldDescriptor)) {
            continue;
          }
          int fieldIndex = getIndex(fieldDescriptor);
          FieldWriter fieldWriter = fieldWriters[fieldIndex];
          fieldWriter.writeField(pb.getField(fieldDescriptor));
        }
      }
    }
  }

  class ArrayWriter extends FieldWriter {
    final FieldWriter fieldWriter;

    ArrayWriter(FieldWriter fieldWriter) {
      this.fieldWriter = fieldWriter;
    }

    @Override
    final void writeRawValue(Object value) {
      throw new UnsupportedOperationException("Array has no raw value");
    }

    @Override
    final void writeField(Object value) {
      List<?> list = (List<?>) value;
      if (list.isEmpty()) {
        return;
      }

      recordConsumer.startField(fieldName, index);
      recordConsumer.startGroup();

      recordConsumer.startField("list", 0); // This is the wrapper group for the array field
      for (Object listEntry: list) {
        recordConsumer.startGroup();
        recordConsumer.startField("element", 0); // This is the mandatory inner field

        fieldWriter.writeRawValue(listEntry);

        recordConsumer.endField("element", 0);
        recordConsumer.endGroup();
      }
      recordConsumer.endField("list", 0);

      recordConsumer.endGroup();
      recordConsumer.endField(fieldName, index);
    }
  }

  /**
   * The RepeatedWriter is used to write collections (lists and maps) using the old style (without LIST and MAP
   * wrappers).
   */
  class RepeatedWriter extends FieldWriter {
    final FieldWriter fieldWriter;

    RepeatedWriter(FieldWriter fieldWriter) {
      this.fieldWriter = fieldWriter;
    }

    @Override
    final void writeRawValue(Object value) {
      throw new UnsupportedOperationException("Array has no raw value");
    }

    @Override
    final void writeField(Object value) {
      List<?> list = (List<?>) value;
      if (list.isEmpty()) {
        return;
      }

      recordConsumer.startField(fieldName, index);

      for (Object listEntry: list) {
        fieldWriter.writeRawValue(listEntry);
      }

      recordConsumer.endField(fieldName, index);
    }
  }

  /** validates mapping between protobuffer fields and parquet fields.*/
  private void validatedMapping(Descriptor descriptor, GroupType parquetSchema) {
    List<FieldDescriptor> allFields = descriptor.getFields();

    for (FieldDescriptor fieldDescriptor: allFields) {
      String fieldName = fieldDescriptor.getName();
      int fieldIndex = fieldDescriptor.getIndex();
      int parquetIndex = parquetSchema.getFieldIndex(fieldName);
      if (fieldIndex != parquetIndex) {
        String message = "FieldIndex mismatch name=" + fieldName + ": " + fieldIndex + " != " + parquetIndex;
        throw new IncompatibleSchemaModificationException(message);
      }
    }
  }

  /*
  * Custom Writer to write Timestamp field as a single long value depicting time in epoch milliseconds
  */
  class TimestampWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object entry) {
      List<Object> timeFields = ((MessageOrBuilder) entry).getAllFields().values().stream().collect(Collectors.toList());
      long eventTimestampMillis = (((Long) timeFields.get(0)) * 1000);
      if(timeFields.size() > 1) {
        eventTimestampMillis += (((Integer)(timeFields.get(1))) / 1000000);
      }
      recordConsumer.addLong(eventTimestampMillis);
    }
  }


  class StringWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      if(!value.getClass().equals(String.class)) {
        value = ((Object) value.toString());
      }
      Binary binaryString = Binary.fromString((String) value);
      recordConsumer.addBinary(binaryString);
    }
  }

  class IntWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addInteger((Integer) value);
    }
  }

  class LongWriter extends FieldWriter {

    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addLong((Long) value);
    }
  }

  class MapWriter extends FieldWriter {

    private final FieldWriter keyWriter;
    private final FieldWriter valueWriter;

    public MapWriter(FieldWriter keyWriter, FieldWriter valueWriter) {
      super();
      this.keyWriter = keyWriter;
      this.valueWriter = valueWriter;
    }

    @Override
    final void writeRawValue(Object value) {
      Collection<Message> collection = (Collection<Message>) value;
      if (collection.isEmpty()) {
        return;
      }
      recordConsumer.startField(fieldName, index);
      recordConsumer.startGroup();

      recordConsumer.startField("key_value", 0); // This is the wrapper group for the map field
      for (Message msg : collection) {
        recordConsumer.startGroup();

        final Descriptor descriptorForType = msg.getDescriptorForType();
        final FieldDescriptor keyDesc = descriptorForType.findFieldByName("key");
        final FieldDescriptor valueDesc = descriptorForType.findFieldByName("value");

        keyWriter.writeField(msg.getField(keyDesc));
        valueWriter.writeField(msg.getField(valueDesc));

        recordConsumer.endGroup();
      }

      recordConsumer.endField("key_value", 0);

      recordConsumer.endGroup();
      recordConsumer.endField(fieldName, index);
    }
  }

  class FloatWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addFloat((Float) value);
    }
  }

  class DoubleWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addDouble((Double) value);
    }
  }

  class EnumWriter extends FieldWriter {
    Map<String, Integer> enumNameNumberPairs;

    public EnumWriter(Descriptors.EnumDescriptor enumType) {
      if (protoEnumBookKeeper.containsKey(enumType.getFullName())) {
        enumNameNumberPairs = protoEnumBookKeeper.get(enumType.getFullName());
      } else {
        enumNameNumberPairs = new HashMap<>();
        protoEnumBookKeeper.put(enumType.getFullName(), enumNameNumberPairs);
      }
    }

    @Override
    final void writeRawValue(Object value) {
      Descriptors.EnumValueDescriptor enumValueDesc = (Descriptors.EnumValueDescriptor) value;
      Binary binary = Binary.fromString(enumValueDesc.getName());
      recordConsumer.addBinary(binary);
      enumNameNumberPairs.putIfAbsent(enumValueDesc.getName(), enumValueDesc.getNumber());
    }
  }

  class BooleanWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addBoolean((Boolean) value);
    }
  }

  class BinaryWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      ByteString byteString = (ByteString) value;
      Binary binary = Binary.fromConstantByteArray(byteString.toByteArray());
      recordConsumer.addBinary(binary);
    }
  }

  private FieldWriter unknownType(FieldDescriptor fieldDescriptor) {
    String exceptionMsg = "Unknown type with descriptor \"" + fieldDescriptor
            + "\" and type \"" + fieldDescriptor.getJavaType() + "\".";
    throw new InvalidRecordException(exceptionMsg);
  }

  /** Returns message descriptor as JSON String*/
  private String serializeDescriptor(Descriptor descriptor) {
    DescriptorProtos.DescriptorProto asProto = descriptor.toProto();
    return TextFormat.printToString(asProto);
  }
}
