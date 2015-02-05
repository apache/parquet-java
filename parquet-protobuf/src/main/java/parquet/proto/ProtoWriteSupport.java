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

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.TextFormat;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.hadoop.conf.Configuration;
import parquet.Log;
import parquet.hadoop.BadConfigurationException;
import parquet.hadoop.api.WriteSupport;
import parquet.io.InvalidRecordException;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.IncompatibleSchemaModificationException;
import parquet.schema.MessageType;
import parquet.schema.Type;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link WriteSupport} for writing Protocol Buffers.
 * @author Lukas Nalezenec
 */
public class ProtoWriteSupport<T extends MessageOrBuilder> extends WriteSupport<T> {

  private static final Log LOG = Log.getLog(ProtoWriteSupport.class);
  public static final String PB_CLASS_WRITE = "parquet.proto.writeClass";

  private RecordConsumer recordConsumer;
  private Class<? extends Message> protoMessage;
  private MessageWriter messageWriter;

  public ProtoWriteSupport() {
  }

  public ProtoWriteSupport(Class<? extends Message> protobufClass) {
    this.protoMessage = protobufClass;
  }

  public static void setSchema(Configuration configuration, Class<? extends Message> protoClass) {
    configuration.setClass(PB_CLASS_WRITE, protoClass, Message.class);
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

    MessageType rootSchema = new ProtoSchemaConverter().convert(protoMessage);
    Descriptors.Descriptor messageDescriptor = Protobufs.getMessageDescriptor(protoMessage);
    validatedMapping(messageDescriptor, rootSchema);

    this.messageWriter = new MessageWriter(messageDescriptor, rootSchema);

    Map<String, String> extraMetaData = new HashMap<String, String>();
    extraMetaData.put(ProtoReadSupport.PB_CLASS, protoMessage.getName());
    extraMetaData.put(ProtoReadSupport.PB_DESCRIPTOR, serializeDescriptor(protoMessage));
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
    MessageWriter(Descriptors.Descriptor descriptor, GroupType schema) {
      List<Descriptors.FieldDescriptor> fields = descriptor.getFields();
      fieldWriters = (FieldWriter[]) Array.newInstance(FieldWriter.class, fields.size());

      int i = 0;
      for (Descriptors.FieldDescriptor fieldDescriptor: fields) {
        String name = fieldDescriptor.getName();
        Type type = schema.getType(name);
        FieldWriter writer = createWriter(fieldDescriptor, type);

        if(fieldDescriptor.isRepeated()) {
         writer = new ArrayWriter(writer);
        }

        writer.setFieldName(name);
        writer.setIndex(schema.getFieldIndex(name));

        fieldWriters[i] = writer;
        i++;
      }
    }

    private FieldWriter createWriter(Descriptors.FieldDescriptor fieldDescriptor, Type type) {

      switch (fieldDescriptor.getJavaType()) {
        case STRING: return new StringWriter() ;
        case MESSAGE: return new MessageWriter(fieldDescriptor.getMessageType(), type.asGroupType());
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
      recordConsumer.startGroup();
      writeAllFields((MessageOrBuilder) value);
      recordConsumer.endGroup();
      recordConsumer.endField(fieldName, index);
    }

    private void writeAllFields(MessageOrBuilder pb) {
      //returns changed fields with values. Map is ordered by id.
      Map<Descriptors.FieldDescriptor, Object> changedPbFields = pb.getAllFields();

      for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : changedPbFields.entrySet()) {
        Descriptors.FieldDescriptor fieldDescriptor = entry.getKey();
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
      List<?> list = (List<?>) value;

      for (Object listEntry: list) {
        fieldWriter.writeRawValue(listEntry);
      }

      recordConsumer.endField(fieldName, index);
    }
  }

  /** validates mapping between protobuffer fields and parquet fields.*/
  private void validatedMapping(Descriptors.Descriptor descriptor, GroupType parquetSchema) {
    List<Descriptors.FieldDescriptor> allFields = descriptor.getFields();

    for (Descriptors.FieldDescriptor fieldDescriptor: allFields) {
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
      Binary binary = Binary.fromByteArray(byteString.toByteArray());
      recordConsumer.addBinary(binary);
    }
  }

  private FieldWriter unknownType(Descriptors.FieldDescriptor fieldDescriptor) {
    String exceptionMsg = "Unknown type with descriptor \"" + fieldDescriptor
            + "\" and type \"" + fieldDescriptor.getJavaType() + "\".";
    throw new InvalidRecordException(exceptionMsg);
  }

  /** Returns message descriptor as JSON String*/
  private String serializeDescriptor(Class<? extends Message> protoClass) {
    Descriptors.Descriptor descriptor = Protobufs.getMessageDescriptor(protoClass);
    DescriptorProtos.DescriptorProto asProto = descriptor.toProto();
    return TextFormat.printToString(asProto);
  }

}
