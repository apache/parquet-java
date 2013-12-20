/**
 * Copyright 2013 Lukas Nalezenec.
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

package parquet.proto.converters;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.schema.GroupType;
import parquet.schema.Type;

/**
 * @author Lukas Nalezenec
 */
public class ProtoMessageConverter extends GroupConverter {

  private final Converter[] converters;
  private final ParentValueContainer parent;
  private final Message.Builder myBuilder;

  // used in record converter
  public ProtoMessageConverter(ParentValueContainer pvc, Class<? extends Message> protoClass, GroupType parquetSchema) {
    this(pvc, Protobufs.getMessageBuilder(protoClass), parquetSchema);
  }

  /**
   * For usage in MSG arrays
   * buidler je tam protoze predtim tam byl descriptor ale nejde jednoduse vytvorit builder z descriptoru
   */
  public ProtoMessageConverter(ParentValueContainer pvc, Message.Builder builder, GroupType parquetSchema) {

    int schemaSize = parquetSchema.getFieldCount();
    converters = new Converter[schemaSize];

    this.parent = pvc;
    int parquetFieldIndex = 1;

    if (pvc == null) {
      throw new IllegalStateException("Missing parent value container");
    }

    myBuilder = builder;

    Descriptors.Descriptor protoDescriptor = builder.getDescriptorForType();

    for (Type parquetField : parquetSchema.getFields()) { // ziskavat jeden field bych IMHO mel o uroven vis
      Descriptors.FieldDescriptor protoField = protoDescriptor.findFieldByName(parquetField.getName());

      if (parquetField.isRepetition(Type.Repetition.REPEATED)) {
        GroupType groupType = parquetField.asGroupType();
        if (groupType.getFieldCount() != 1) throw new RuntimeException("One field expected but found " + groupType);

        // TODO find this hack in avro and Thrift
        parquetField = groupType.getType(0);
        protoField = protoDescriptor.findFieldByName(parquetField.getName());
      }

      if (protoField == null) {
        throw new RuntimeException("Cant find " + parquetField.getName() + " Scheme mismatch \n\"" + parquetField + "\"\n proto descriptor:\n" + protoDescriptor.toProto());
      }

      converters[parquetFieldIndex - 1] = newMessageConverter(myBuilder, protoField, parquetField);

      parquetFieldIndex++;
    }
  }


  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {

  }

  @Override
  public void end() {
      parent.add(myBuilder.build());
      //todo myBuilder.clear();
      // TODO should be overriden in MessageRecordConverter
  }

  private Converter newMessageConverter(final Message.Builder parentBuilder, final Descriptors.FieldDescriptor fieldDescriptor, Type parquetType) {

    boolean isRepeated = fieldDescriptor.isRepeated();

    ParentValueContainer parent;

    if (isRepeated) {
      parent = new ParentValueContainer() {
        @Override
        public void add(Object value) {
          parentBuilder.addRepeatedField(fieldDescriptor, value);
        }
      };
    } else {
      parent = new ParentValueContainer() {
        @Override
        public void add(Object value) {
          parentBuilder.setField(fieldDescriptor, value);
        }
      };
    }

    if (isRepeated) {
      parquetType = parquetType.asGroupType().getType(0);
    }

    Converter innerConverter = newScalarConverter(parent, parentBuilder, fieldDescriptor, parquetType);

    if (isRepeated) {
      return new ProtoArrayConverter(innerConverter);
    } else {
      return innerConverter;
    }
  }


  private static Converter newScalarConverter(ParentValueContainer pvc, Message.Builder parentBuilder, Descriptors.FieldDescriptor fieldDescriptor, Type parquetType) {

    Descriptors.FieldDescriptor.JavaType javaType = fieldDescriptor.getJavaType();

    boolean isMessage = javaType.equals(Descriptors.FieldDescriptor.JavaType.MESSAGE);

    if (isMessage) {

      if (!fieldDescriptor.getContainingType().equals(parentBuilder.getDescriptorForType())) {
        throw new RuntimeException(fieldDescriptor.getFullName() + " is not inside " + parentBuilder.getDescriptorForType().getFullName());
      }

      GroupType parquetSubType = parquetType.asGroupType();
      Message.Builder subBuilder;
      if (fieldDescriptor.isRepeated()) {
        subBuilder = parentBuilder.newBuilderForField(fieldDescriptor);
      } else {
        subBuilder = parentBuilder.newBuilderForField(fieldDescriptor);
      }

      return new ProtoMessageConverter(pvc, subBuilder, parquetSubType);
    } else {
      if (javaType.equals(Descriptors.FieldDescriptor.JavaType.STRING)) {
        return new ProtobufStringConverter(pvc);
      } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.FLOAT)) {
        return new ProtoFloatConverter(pvc);
      } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.DOUBLE)) {
        return new ProtoDoubleConverter(pvc);
      } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.BOOLEAN)) {
        return new ProtoBooleanConverter(pvc);
      } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.BYTE_STRING)) {
        return new ProtoBinaryConverter(pvc);
      } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.ENUM)) {
        return new ProtoEnumConverter(pvc, fieldDescriptor);
      } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.INT)) {
        return new ProtoIntConverter(pvc);
      } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.LONG)) {
        return new ProtoLongConverter(pvc);
      }
    }

    throw new UnsupportedOperationException(String.format("Cannot convert type: %s" +
            " (Parquet type: %s) ", javaType, parquetType));
  }

  public Message.Builder getBuilder() {
    return myBuilder;
  }
}
