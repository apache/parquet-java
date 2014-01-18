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

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.TextFormat;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.BadConfigurationException;
import parquet.hadoop.api.WriteSupport;
import parquet.io.InvalidRecordException;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.IncompatibleSchemaModificationException;
import parquet.schema.MessageType;
import parquet.schema.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ProtoWriteSupport<T extends MessageOrBuilder> extends WriteSupport<T> {

  private RecordConsumer recordConsumer;
  private MessageType rootSchema;
  static final String PB_CLASS_WRITE = "parquet.proto.writeClass";
  private Class<? extends Message> protoMessage;

  public ProtoWriteSupport() {
  }

  public ProtoWriteSupport(Class<? extends Message> protobufClass) {
    this.protoMessage = protobufClass;
    rootSchema = new ProtoSchemaConverter().convert(protoMessage);
  }

  @Override
  public WriteContext init(Configuration configuration) {

    // if no protobuf descriptor was given in constructor, load descriptor from configuration (set with setProtobufClass)
    if (protoMessage == null) {
      Class<? extends Message> pbClass = configuration.getClass(PB_CLASS_WRITE, null, Message.class);
      if (pbClass != null) {
        protoMessage = pbClass;
        rootSchema = new ProtoSchemaConverter().convert(pbClass);
      } else {
        String msg = "Protocol buffer class not specified.";
        String hint = " Please use method ProtoParquetOutputFormat.setProtobufClass(...) or other similar method.";
        throw new BadConfigurationException(msg + hint);
      }
    }

    validatedMapping(Protobufs.getMessageDescriptor(protoMessage), rootSchema);

    Map<String, String> extraMetaData = new HashMap<String, String>();
    extraMetaData.put(ProtoReadSupport.PB_CLASS, protoMessage.getName());
    extraMetaData.put(ProtoReadSupport.PB_DESCRIPTOR, serializeDescriptor(protoMessage));
    return new WriteContext(rootSchema, extraMetaData);
  }

  private String serializeDescriptor(Class<? extends Message> protoClass) {
    Descriptors.Descriptor descriptor = Protobufs.getMessageDescriptor(protoClass);
    DescriptorProtos.DescriptorProto asProto = descriptor.toProto();
    return TextFormat.printToString(asProto);
  }

  public static void setSchema(Configuration configuration, Class<? extends Message> protoClass) {
    configuration.setClass(PB_CLASS_WRITE, protoClass, Message.class);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(T record) {
    recordConsumer.startMessage();
    writeRecordFields(rootSchema, record);
    recordConsumer.endMessage();
  }

  private void writeMessage(GroupType schema, MessageOrBuilder message) {
    recordConsumer.startGroup();
    writeRecordFields(schema, message);
    recordConsumer.endGroup();
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

  private void writeRecordFields(GroupType parquetSchema, MessageOrBuilder record) {
    List<Type> parquetFields = parquetSchema.getFields();

    //returns changed fields with values. Map is ordered by id.
    Map<Descriptors.FieldDescriptor, Object> changedPbFields = record.getAllFields();

    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : changedPbFields.entrySet()) {
      Object value = entry.getValue();

      if (value != null) {
        Descriptors.FieldDescriptor fieldDescriptor = entry.getKey();
        final int protoIndex = fieldDescriptor.getIndex();
        Type fieldType = parquetFields.get(protoIndex);

        final int parquetIndex = protoIndex; // is same since parquet schema is build from Descriptor.
        String fieldName = fieldType.getName();

        recordConsumer.startField(fieldName, parquetIndex);
        if (fieldDescriptor.isRepeated()) {
          writeArray(fieldType, fieldDescriptor, (List<?>) value);
        } else {
          writeScalarValue(fieldType, fieldDescriptor, value);
        }
        recordConsumer.endField(fieldName, parquetIndex);
      }
    }
  }


  private void writeArray(Type type, Descriptors.FieldDescriptor fieldDescriptor, List<?> array) {

    switch (fieldDescriptor.getJavaType()) {
      case STRING:
        for (Object value : array) {
          Binary binaryString = Binary.fromString((String) value);
          recordConsumer.addBinary(binaryString);
        }
        return;
      case MESSAGE:
        for (Object value : array) {
          writeMessage(type.asGroupType(), (MessageOrBuilder) value);
        }
        return;
      case INT:
        for (Object value : array) {
          recordConsumer.addInteger((Integer) value);
        }
        return;
      case LONG:
        for (Object value : array) {
          recordConsumer.addLong((Long) value);
        }
        return;
      case FLOAT:
        for (Object value : array) {
          recordConsumer.addFloat((Float) value);
        }
        return;
      case DOUBLE:
        for (Object value : array) {
          recordConsumer.addDouble((Double) value);
        }
        return;
      case ENUM:
        for (Object value : array) {
          writeEnum((Descriptors.EnumValueDescriptor) value);
        }
        return;
      case BOOLEAN:
        for (Object value : array) {
          recordConsumer.addBoolean((Boolean) value);
        }
        return;
      case BYTE_STRING:
        for (Object value : array) {
          writeString((ByteString) value);
        }
        return;
    }

    unknownType(fieldDescriptor, array);
  }


  private void writeScalarValue(Type type, Descriptors.FieldDescriptor fieldDescriptor, Object value) {

   switch (fieldDescriptor.getJavaType()) {
      case STRING:
        Binary binaryString = Binary.fromString((String) value);
        recordConsumer.addBinary(binaryString);
        return;
      case MESSAGE:
        writeMessage(type.asGroupType(), (MessageOrBuilder) value);
        return;
      case INT:
        recordConsumer.addInteger((Integer) value);
        return;
      case LONG:
        recordConsumer.addLong((Long) value);
        return;
      case FLOAT:
        recordConsumer.addFloat((Float) value);
        return;
      case DOUBLE:
        recordConsumer.addDouble((Double) value);
        return;
      case ENUM:
        writeEnum((Descriptors.EnumValueDescriptor) value);
        return;
      case BOOLEAN:
        recordConsumer.addBoolean((Boolean) value);
        return;
      case BYTE_STRING:
        writeString((ByteString) value);
        return;
    }

    unknownType(fieldDescriptor, value);
  }

  private void writeEnum(Descriptors.EnumValueDescriptor enumDescriptor) {
    Binary binary = Binary.fromString(enumDescriptor.getName());
    recordConsumer.addBinary(binary);
  }

  private void writeString(ByteString byteString) {
    Binary binary = Binary.fromByteArray(byteString.toByteArray());
    recordConsumer.addBinary(binary);
  }

  private void unknownType(Descriptors.FieldDescriptor fieldDescriptor, Object value) {
    String exceptionMsg = "Cannot write \"" + value + "\" with descriptor \"" + fieldDescriptor
            + "\" and type \"" + fieldDescriptor.getJavaType() + "\".";
    throw new InvalidRecordException(exceptionMsg);
  }

}
