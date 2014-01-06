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
import com.twitter.elephantbird.util.Protobufs;
import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.api.WriteSupport;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType;


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
        throw new RuntimeException(msg + hint);
      }
    }

    Map<String, String> extraMetaData = new HashMap<String, String>();
    extraMetaData.put(ProtoReadSupport.PB_CLASS, protoMessage.getName());
    extraMetaData.put(ProtoReadSupport.PB_DESCRIPTOR, serializeDescriptor(protoMessage));

    // TODO add also pig descriptor
    // see Thrift code  ThriftWriteSupport
    return new WriteContext(rootSchema, extraMetaData);
  }

  private String serializeDescriptor(Class<? extends Message> protoClass) {
    Descriptors.Descriptor descriptor = Protobufs.getMessageDescriptor(protoClass);
    DescriptorProtos.DescriptorProto asProto = descriptor.toProto();
    return asProto.toString();
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

  private void writeRecordFields(GroupType parquetSchema, MessageOrBuilder record) {
    List<Type> fields = parquetSchema.getFields();

    Map<Descriptors.FieldDescriptor, Object> pbFields = record.getAllFields();

    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : pbFields.entrySet()) {

      Descriptors.FieldDescriptor fieldDescriptor = entry.getKey();
      int protoIndex = fieldDescriptor.getIndex();
      Type fieldType = fields.get(protoIndex);

      Object value = entry.getValue();

      if (value != null) {

        int parquetIndex = parquetSchema.getFieldIndex(fieldDescriptor.getName());

        if (fieldDescriptor.isRepeated()) {
          recordConsumer.startField(fieldType.getName(), parquetIndex);
          writeArray(fieldType.asGroupType(), fieldDescriptor, (List<?>) value);
          recordConsumer.endField(fieldType.getName(), parquetIndex);
        } else {
          recordConsumer.startField(fieldType.getName(), parquetIndex);
          writeScalarValue(fieldType, fieldDescriptor, value);
          recordConsumer.endField(fieldType.getName(), parquetIndex);
        }

      } else if (fieldType.isRepetition(Type.Repetition.REQUIRED)) {
        throw new RuntimeException("Null-value for required field: " + fieldDescriptor.getName());
      }
    }
  }

  private <T> void writeArray(GroupType schema, Descriptors.FieldDescriptor fieldDescriptor, List<T> array) {
    recordConsumer.startGroup();
    if (array.iterator().hasNext()) {
      String arrayType = schema.getName();
      recordConsumer.startField(arrayType, 0);
      for (T elt : array) {
        writeScalarValue((schema.getType(0)), fieldDescriptor, elt);
      }
      recordConsumer.endField(arrayType, 0);
    }
    recordConsumer.endGroup();
  }


  private void writeScalarValue(Type type, Descriptors.FieldDescriptor fieldDescriptor, Object value) {

   switch (fieldDescriptor.getJavaType()) {
      case STRING:
        Binary binaryString = Binary.fromString((String) value);
        recordConsumer.addBinary(binaryString);
      break;
      case MESSAGE:
        MessageOrBuilder msg = (MessageOrBuilder) value;
        writeMessage(type.asGroupType(), msg);
        break;
      case INT:
        recordConsumer.addInteger((Integer) value);
        break;
      case LONG:
        recordConsumer.addLong((Long) value);
        break;
      case FLOAT:
        recordConsumer.addFloat((Float) value);
        break;
      case DOUBLE:
        recordConsumer.addDouble((Double) value);
        break;
      case ENUM:
        Descriptors.EnumValueDescriptor enumDescriptor = (Descriptors.EnumValueDescriptor) value;
        recordConsumer.addBinary(Binary.fromString(enumDescriptor.getName()));
        break;
      case BOOLEAN:
        recordConsumer.addBoolean((Boolean) value);
        break;
      case BYTE_STRING:
        ByteString byteString = (ByteString) value;
        Binary binary = Binary.fromByteArray(byteString.toByteArray());
        recordConsumer.addBinary(binary);
        break;
      default:
        String exceptionMsg = "Cannot write \"" + value + "\" with descriptor \"" + fieldDescriptor
                + "\" and type \"" + fieldDescriptor.getJavaType() + "\".";
        throw new RuntimeException(exceptionMsg);
    }
  }

}
