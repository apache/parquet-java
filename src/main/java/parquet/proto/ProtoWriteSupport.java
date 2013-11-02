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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ProtoWriteSupport<T extends MessageOrBuilder> extends WriteSupport<T> {

  private RecordConsumer recordConsumer;
  private MessageType rootSchema;
  private static final String PB_CLASS_WRITE = "parquet.proto.writeClass";
  private Class<? extends Message> protoMessage;

  public ProtoWriteSupport() {
  }

  public ProtoWriteSupport(Class<? extends Message> protobufClass) {
    this.protoMessage = protobufClass;
    rootSchema = new ProtoSchemaConverter().convert(protoMessage);
  }

  @Override
  public WriteContext init(Configuration configuration) {
    if (protoMessage  == null) {
      Class<? extends Message> pbClass = configuration.getClass(PB_CLASS_WRITE, null, Message.class);
      if (pbClass != null) {
        rootSchema = new ProtoSchemaConverter().convert(protoMessage);
      } else {
        String msg = "Protobuffer class not specified.";
        String hint = " Please use method ProtoParquetOutputFormat.setProtobufferClass(...) or other similar method.";
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

  private void writeMessage(GroupType schema, T message) {
    recordConsumer.startGroup();
    writeRecordFields(schema, message);
    recordConsumer.endGroup();
  }

  private void writeRecordFields(GroupType parquetSchema, T record) {
    List<Type> fields = parquetSchema.getFields();

    Map<Descriptors.FieldDescriptor, Object> pbFields = record.getAllFields();

    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : pbFields.entrySet()) {
      Descriptors.FieldDescriptor fieldDescriptor = entry.getKey();
      int protoIndex = fieldDescriptor.getIndex();
      //TODO tohle je preci blbost, tady musi byt mapovani
      // UPDATE: blbost to neni ale mel bych si to pojistit proti zmenam
      Object value = entry.getValue();

      Type fieldType = fields.get(protoIndex);

      String parName = fieldType.getName();//TODO remove me
      String proName = fieldDescriptor.getName();
      if (!parName.equals(proName )) throw new RuntimeException("Field mismatch");


      if (value != null) {

        if (fieldDescriptor.isRepeated()) {
          int parquetIndex = parquetSchema.getFieldIndex(fieldDescriptor.getName());
          recordConsumer.startField(fieldType.getName(), parquetIndex);
          Type subType = parquetSchema.asGroupType().getType(0);
          writeArray(fieldType.asGroupType(), fieldDescriptor, (List<?>) value);
          recordConsumer.endField(fieldType.getName(), parquetIndex);
        } else {
          int parquetIndex = parquetSchema.getFieldIndex(fieldDescriptor.getName());
          recordConsumer.startField(fieldType.getName(), parquetIndex);
          String parquetName = fieldType.getName(); // TODO remove
          String protoName = fieldDescriptor.getName();
          if (parquetName != protoName) throw new RuntimeException("Name mismatch " + parquetName + " != " + protoName);
          writeScalarValue(fieldType, fieldDescriptor, value);
          recordConsumer.endField(fieldType.getName(), parquetIndex);
        }

      } else if (fieldType.isRepetition(Type.Repetition.REQUIRED)) {
        throw new RuntimeException("Null-value for required field: " + fieldDescriptor.getName());
      }
    }
  }

  private <T> void writeArray(GroupType schema, Descriptors.FieldDescriptor fieldDescriptor,
                              List<T> array) {
    if (!schema.getName().equals(fieldDescriptor.getName())) throw  new RuntimeException("Mismatch");//TODO remove me

    recordConsumer.startGroup();
    if (array.iterator().hasNext()) {
      String arrayType = schema.getName();
      recordConsumer.startField(arrayType, 0);
      for (T elt : array) {
        writeScalarValue((schema.getType(0)), fieldDescriptor, elt); // patch
      }
      recordConsumer.endField(arrayType, 0);
    }
    recordConsumer.endGroup();
  }


  private void writeScalarValue(Type type, Descriptors.FieldDescriptor fieldDescriptor, Object value) {

    Descriptors.FieldDescriptor.JavaType javaType = fieldDescriptor.getJavaType();

    if (javaType.equals(Descriptors.FieldDescriptor.JavaType.BOOLEAN)) {
      recordConsumer.addBoolean((Boolean) value);
    } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.INT)) {
      recordConsumer.addInteger(((Number) value).intValue());
    } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.LONG)) {
      recordConsumer.addLong(((Number) value).longValue());
    } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.FLOAT)) {
      recordConsumer.addFloat(((Number) value).floatValue());
    } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.DOUBLE)) {
      recordConsumer.addDouble(((Number) value).doubleValue());
    } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.BYTE_STRING)) {
      recordConsumer.addBinary(Binary.fromByteBuffer((ByteBuffer) value));
    } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.STRING)) {
      recordConsumer.addBinary(stringToBinary(value));
    } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.MESSAGE)) {
      writeMessage(type.asGroupType(), (T) value);// patched
    } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.ENUM)) {
      Descriptors.EnumValueDescriptor enumDescriptor = (Descriptors.EnumValueDescriptor) value;
      recordConsumer.addBinary(Binary.fromString(enumDescriptor.getName()));
    } else {
      String msg = "Cannot write " + value + " with descriptor " + fieldDescriptor + " and type " + javaType;
      throw new RuntimeException(msg);
    }
  }

  private Binary stringToBinary(Object value) {
    return Binary.fromString(value.toString());
  }

}
