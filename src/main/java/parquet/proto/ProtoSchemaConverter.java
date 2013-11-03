/**
 * Copyright 2012 Twitter, Inc.
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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import parquet.schema.*;

import java.util.ArrayList;
import java.util.List;

import static parquet.schema.OriginalType.ENUM;
import static parquet.schema.OriginalType.UTF8;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.*;

/**
 * <p/>
 * Converts a Protobuffer Descriptor into a Parquet schema.
 *
 * @author Lukas Nalezenec
 */
public class ProtoSchemaConverter {

  public MessageType convert(Class<? extends Message> protobufClass) {
    Descriptors.Descriptor descriptor = Protobufs.getMessageDescriptor(protobufClass);

    MessageType messageType = new MessageType(descriptor.getFullName(), convertFields(descriptor.getFields()));

    System.out.println("Convertor info:\n " + descriptor.toProto() +  " was converted to \n" + messageType);
    return messageType;
  }

  /* Iterates over list of fields. **/
  private List<Type> convertFields(List<Descriptors.FieldDescriptor> fieldDescriptors) {
    List<Type> types = new ArrayList<Type>();

    for (Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {

      String fieldName = fieldDescriptor.getName();
      Type.Repetition repetition = getRepetition(fieldDescriptor);

      Type type;
      if (fieldDescriptor.isRepeated()) {
        Type nestedType = convertScalarField(fieldName + "_tuple", fieldDescriptor, Type.Repetition.REPEATED);
        type = ConversionPatterns.listType(Type.Repetition.OPTIONAL, fieldName, nestedType);
      } else {
        type = convertScalarField(fieldName, fieldDescriptor, repetition);
      }

      types.add(type);
    }
    return types;
  }

  private Type.Repetition getRepetition(Descriptors.FieldDescriptor descriptor) {
    Type.Repetition repetition;
    if (descriptor.isRequired()) {
      repetition = Type.Repetition.REQUIRED;
    } else if (descriptor.isRepeated()) {
      repetition = Type.Repetition.REPEATED;
    } else {
      repetition = Type.Repetition.OPTIONAL;
    }
    return repetition;
  }

  private Type convertScalarField(String fieldName, Descriptors.FieldDescriptor descriptor, Type.Repetition repetition) {
    Descriptors.FieldDescriptor.JavaType javaType = descriptor.getJavaType();

    if (javaType.equals(Descriptors.FieldDescriptor.JavaType.BOOLEAN)) {
      return primitive(fieldName, BOOLEAN, repetition);
    } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.INT)) {
      return primitive(fieldName, INT32, repetition);
    } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.LONG)) {
      return primitive(fieldName, INT64, repetition);
    } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.FLOAT)) {
      return primitive(fieldName, FLOAT, repetition);
    } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.DOUBLE)) {
      return primitive(fieldName, DOUBLE, repetition);
    } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.BYTE_STRING)) {
      return primitive(fieldName, BINARY, repetition);
    } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.STRING)) {
      return primitive(fieldName, BINARY, repetition, UTF8);
    } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.MESSAGE)) {
      Descriptors.Descriptor messageDescriptor = descriptor.getMessageType();
      List<Type> fields = convertFields(messageDescriptor.getFields());
      return new GroupType(repetition, fieldName, fields);
    } else if (javaType.equals(Descriptors.FieldDescriptor.JavaType.ENUM)) {
      return primitive(fieldName, BINARY, repetition, ENUM);
    }

    throw new UnsupportedOperationException("Cannot convert Protobuffer: type " + javaType + " fieldName " + fieldName);
  }

  /**
   * Makes primitive type with additional information. Used for String and Binary types
   */
  private Type primitive(String name, PrimitiveType.PrimitiveTypeName primitive,
                         Type.Repetition repetition, OriginalType originalType) {
    return new PrimitiveType(repetition, primitive, name, originalType);
  }

  private PrimitiveType primitive(String name, PrimitiveType.PrimitiveTypeName
          primitive, Type.Repetition repetition) {
    return new PrimitiveType(repetition, primitive, name, null);
  }

}