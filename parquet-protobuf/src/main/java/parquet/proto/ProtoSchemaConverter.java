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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import parquet.Log;
import parquet.schema.ConversionPatterns;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import java.util.ArrayList;
import java.util.List;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import static parquet.schema.PrimitiveType.PrimitiveTypeName;

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

    MessageType messageType = new MessageType(descriptor.getFullName(), convertFields(descriptor.getFields()));

    LOG.debug("Converter info:\n " + descriptor.toProto() + " was converted to \n" + messageType);
    return messageType;
  }

  /* Iterates over list of fields. **/
  private List<Type> convertFields(List<Descriptors.FieldDescriptor> fieldDescriptors) {
    List<Type> types = new ArrayList<Type>();

    for (Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {
      String fieldName = fieldDescriptor.getName();
      Type.Repetition repetition = getRepetition(fieldDescriptor);
      Type type = convertScalarField(fieldName, fieldDescriptor, repetition);
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

    JavaType javaType = descriptor.getJavaType();

    int number = descriptor.getNumber();
    switch (javaType) {
      case BOOLEAN : return primitive(fieldName, PrimitiveTypeName.BOOLEAN, repetition, number);
      case INT : return primitive(fieldName, PrimitiveTypeName.INT32, repetition, number);
      case LONG : return primitive(fieldName, PrimitiveTypeName.INT64, repetition, number);
      case FLOAT : return primitive(fieldName, PrimitiveTypeName.FLOAT, repetition, number);
      case DOUBLE: return primitive(fieldName, PrimitiveTypeName.DOUBLE, repetition, number);
      case BYTE_STRING: return primitive(fieldName, PrimitiveTypeName.BINARY, repetition, number);
      case STRING: return primitive(fieldName, PrimitiveTypeName.BINARY, repetition, OriginalType.UTF8, number);
      case MESSAGE: {
        Descriptors.Descriptor messageDescriptor = descriptor.getMessageType();
        List<Type> fields = convertFields(messageDescriptor.getFields());
        return new GroupType(repetition, fieldName, fields).withId(number);
      }
      case ENUM: return primitive(fieldName, PrimitiveTypeName.BINARY, repetition, OriginalType.ENUM, number);
    }

    throw new UnsupportedOperationException("Cannot convert Protocol Buffer: unknown type " + javaType + " fieldName " + fieldName);
  }

  /**
   * Makes primitive type with additional information. Used for String and Binary types
   */
  private PrimitiveType primitive(String name, PrimitiveTypeName primitive, Type.Repetition repetition, OriginalType originalType, int number) {
    return new PrimitiveType(repetition, primitive, name, originalType).withId(number);
  }

  private PrimitiveType primitive(String name, PrimitiveTypeName primitive, Type.Repetition repetition, int number) {
    return primitive(name, primitive, repetition, null, number);
  }

}