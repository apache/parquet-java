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
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Optional.ofNullable;

/**
 * Implementation of {@link WriteSupport} for writing Protocol Buffers.
 */
public class ProtoWriteSupport<T extends MessageOrBuilder> extends WriteSupport<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ProtoWriteSupport.class);
  public static final String PB_CLASS_WRITE = "parquet.proto.writeClass";
  // PARQUET-968 introduces changes to allow writing specs compliant schemas with parquet-protobuf.
  // In the past, collection were not written using the LIST and MAP wrappers and thus were not compliant
  // with the parquet specs. This flag, is set to true, allows to write using spec compliant schemas
  // but is set to false by default to keep backward compatibility
  public static final String PB_SPECS_COMPLIANT_WRITE = "parquet.proto.writeSpecsCompliant";

  private boolean writeSpecsCompliant = false;
  private RecordConsumer recordConsumer;
  private Class<? extends Message> protoMessage;
  private MessageWriter messageWriter;

  public ProtoWriteSupport() {
  }

  public ProtoWriteSupport(Class<? extends Message> protobufClass) {
    this.protoMessage = protobufClass;
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
      messageWriter.writeTopLevelMessage(record);
    } catch (RuntimeException e) {
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

    // if no protobuf descriptor was given in constructor, load descriptor from configuration (set with setProtobufClass)
    if (protoMessage == null) {
      Class<? extends Message> pbClass = configuration.getClass(PB_CLASS_WRITE, null, Message.class);
      if (pbClass != null) {
        protoMessage = pbClass;
      } else {
        String msg = "Protocol buffer class not specified.";
        String hint = " Please use method ProtoParquetOutputFormat.setProtobufClass(...) or other similar method.";
        throw new BadConfigurationException(msg + hint);
      }
    }

    writeSpecsCompliant = configuration.getBoolean(PB_SPECS_COMPLIANT_WRITE, writeSpecsCompliant);
    MessageType rootSchema = new ProtoSchemaConverter(writeSpecsCompliant).convert(protoMessage);
    Descriptor messageDescriptor = Protobufs.getMessageDescriptor(protoMessage);
    validatedMapping(messageDescriptor, rootSchema);

    this.messageWriter = new MessageWriter(messageDescriptor, rootSchema);

    Map<String, String> extraMetaData = new HashMap<String, String>();
    extraMetaData.put(ProtoReadSupport.PB_CLASS, protoMessage.getName());
    extraMetaData.put(ProtoReadSupport.PB_DESCRIPTOR, serializeDescriptor(protoMessage));
    extraMetaData.put(PB_SPECS_COMPLIANT_WRITE, String.valueOf(writeSpecsCompliant));
    return new WriteContext(rootSchema, extraMetaData);
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
      recordConsumer.startField(fieldName, index);
      writeRawValue(value);
      recordConsumer.endField(fieldName, index);
    }
  }

  class MessageWriter extends FieldWriter {

    final FieldWriter[] fieldWriters;

    @SuppressWarnings("unchecked")
    MessageWriter(Descriptor descriptor, GroupType schema) {
      List<FieldDescriptor> fields = descriptor.getFields();
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

        fieldWriters[fieldDescriptor.getIndex()] = writer;
      }
    }

    private FieldWriter createWriter(FieldDescriptor fieldDescriptor, Type type) {

      switch (fieldDescriptor.getJavaType()) {
        case STRING: return new StringWriter() ;
        case MESSAGE: return createMessageWriter(fieldDescriptor, type);
        case INT: return new IntWriter();
        case LONG: return new LongWriter();
        case FLOAT: return new FloatWriter();
        case DOUBLE: return new DoubleWriter();
        case ENUM: return new EnumWriter();
        case BOOLEAN: return new BooleanWriter();
        case BYTE_STRING: return new BinaryWriter();
      }

      return unknownType(fieldDescriptor);//should not be executed, always throws exception.
    }

    private FieldWriter createMessageWriter(FieldDescriptor fieldDescriptor, Type type) {
      if (fieldDescriptor.isMapField() && writeSpecsCompliant) {
        return createMapWriter(fieldDescriptor, type);
      }

      return new MessageWriter(fieldDescriptor.getMessageType(), getGroupType(type));
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
      //returns changed fields with values. Map is ordered by id.
      Map<FieldDescriptor, Object> changedPbFields = pb.getAllFields();

      for (Map.Entry<FieldDescriptor, Object> entry : changedPbFields.entrySet()) {
        FieldDescriptor fieldDescriptor = entry.getKey();

        if(fieldDescriptor.isExtension()) {
          // Field index of an extension field might overlap with a base field.
          throw new UnsupportedOperationException(
                  "Cannot convert Protobuf message with extension field(s)");
        }

        int fieldIndex = fieldDescriptor.getIndex();
        fieldWriters[fieldIndex].writeField(entry.getValue());
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
      recordConsumer.startField(fieldName, index);
      recordConsumer.startGroup();
      List<?> list = (List<?>) value;

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
      recordConsumer.startField(fieldName, index);
      List<?> list = (List<?>) value;

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


  class StringWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
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
      recordConsumer.startGroup();

      recordConsumer.startField("key_value", 0); // This is the wrapper group for the map field
      for (Message msg : (Collection<Message>) value) {
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
    @Override
    final void writeRawValue(Object value) {
      Binary binary = Binary.fromString(((Descriptors.EnumValueDescriptor) value).getName());
      recordConsumer.addBinary(binary);
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
  private String serializeDescriptor(Class<? extends Message> protoClass) {
    Descriptor descriptor = Protobufs.getMessageDescriptor(protoClass);
    DescriptorProtos.DescriptorProto asProto = descriptor.toProto();
    return TextFormat.printToString(asProto);
  }
}
