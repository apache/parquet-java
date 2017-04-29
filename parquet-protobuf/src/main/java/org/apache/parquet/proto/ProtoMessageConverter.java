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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.IncompatibleSchemaModificationException;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

/**
 * Converts Protocol Buffer message (both top level and inner) to parquet.
 * This is internal class, use {@link ProtoRecordConverter}.
 *
 * @see {@link ProtoWriteSupport}
 * @author Lukas Nalezenec
 */
class ProtoMessageConverter extends GroupConverter {

  private final Converter[] converters;
  private final ParentValueContainer parent;
  private final Message.Builder myBuilder;

  // used in record converter
  ProtoMessageConverter(ParentValueContainer pvc, Class<? extends Message> protoClass, GroupType parquetSchema) {
    this(pvc, Protobufs.getMessageBuilder(protoClass), parquetSchema);
  }


  // For usage in message arrays
  ProtoMessageConverter(ParentValueContainer pvc, Message.Builder builder, GroupType parquetSchema) {

    int schemaSize = parquetSchema.getFieldCount();
    converters = new Converter[schemaSize];

    this.parent = pvc;
    int parquetFieldIndex = 1;

    if (pvc == null) {
      throw new IllegalStateException("Missing parent value container");
    }

    myBuilder = builder;

    Descriptors.Descriptor protoDescriptor = builder.getDescriptorForType();

    for (Type parquetField : parquetSchema.getFields()) {
      Descriptors.FieldDescriptor protoField = protoDescriptor.findFieldByName(parquetField.getName());

      if (protoField == null) {
        String description = "Scheme mismatch \n\"" + parquetField + "\"" +
                "\n proto descriptor:\n" + protoDescriptor.toProto();
        throw new IncompatibleSchemaModificationException("Cant find \"" + parquetField.getName() + "\" " + description);
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
    myBuilder.clear();
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

    OriginalType originalType = parquetType.getOriginalType() == null ? OriginalType.UTF8 : parquetType.getOriginalType();
    switch (originalType) {
      case LIST: return new ListConverter(parentBuilder, fieldDescriptor, parquetType);
      case MAP: return new MapConverter(parentBuilder, fieldDescriptor, parquetType);
      default: return newScalarConverter(parent, parentBuilder, fieldDescriptor, parquetType);
    }
  }

  private Converter newScalarConverter(ParentValueContainer pvc, Message.Builder parentBuilder, Descriptors.FieldDescriptor fieldDescriptor, Type parquetType) {

    JavaType javaType = fieldDescriptor.getJavaType();

    switch (javaType) {
      case STRING: return new ProtoStringConverter(pvc);
      case FLOAT: return new ProtoFloatConverter(pvc);
      case DOUBLE: return new ProtoDoubleConverter(pvc);
      case BOOLEAN: return new ProtoBooleanConverter(pvc);
      case BYTE_STRING: return new ProtoBinaryConverter(pvc);
      case ENUM: return new ProtoEnumConverter(pvc, fieldDescriptor);
      case INT: return new ProtoIntConverter(pvc);
      case LONG: return new ProtoLongConverter(pvc);
      case MESSAGE: {
        Message.Builder subBuilder = parentBuilder.newBuilderForField(fieldDescriptor);
        return new ProtoMessageConverter(pvc, subBuilder, parquetType.asGroupType());
      }
    }

    throw new UnsupportedOperationException(String.format("Cannot convert type: %s" +
            " (Parquet type: %s) ", javaType, parquetType));
  }

  public Message.Builder getBuilder() {
    return myBuilder;
  }

  static abstract class ParentValueContainer {

    /**
     * Adds the value to the parent.
     */
    public abstract void add(Object value);

  }

  final class ProtoEnumConverter extends PrimitiveConverter {

    private final Descriptors.FieldDescriptor fieldType;
    private final Map<Binary, Descriptors.EnumValueDescriptor> enumLookup;
    private Descriptors.EnumValueDescriptor[] dict;
    private final ParentValueContainer parent;

    public ProtoEnumConverter(ParentValueContainer parent, Descriptors.FieldDescriptor fieldType) {
      this.parent = parent;
      this.fieldType = fieldType;
      this.enumLookup = makeLookupStructure(fieldType);
    }

    /**
     * Fills lookup structure for translating between parquet enum values and Protocol buffer enum values.
     * */
    private Map<Binary, Descriptors.EnumValueDescriptor> makeLookupStructure(Descriptors.FieldDescriptor enumFieldType) {
      Descriptors.EnumDescriptor enumType = enumFieldType.getEnumType();
      Map<Binary, Descriptors.EnumValueDescriptor> lookupStructure = new HashMap<Binary, Descriptors.EnumValueDescriptor>();

      List<Descriptors.EnumValueDescriptor> enumValues = enumType.getValues();

      for (Descriptors.EnumValueDescriptor value : enumValues) {
        String name = value.getName();
        lookupStructure.put(Binary.fromString(name), enumType.findValueByName(name));
      }

      return lookupStructure;
    }

    /**
     * Translates given parquet enum value to protocol buffer enum value.
     * @throws org.apache.parquet.io.InvalidRecordException is there is no corresponding value.
     * */
    private Descriptors.EnumValueDescriptor translateEnumValue(Binary binaryValue) {
      Descriptors.EnumValueDescriptor protoValue = enumLookup.get(binaryValue);

      if (protoValue == null) {
        Set<Binary> knownValues = enumLookup.keySet();
        String msg = "Illegal enum value \"" + binaryValue + "\""
                + " in protocol buffer \"" + fieldType.getFullName() + "\""
                + " legal values are: \"" + knownValues + "\"";
        throw new InvalidRecordException(msg);
      }
      return protoValue;
    }

    @Override
    final public void addBinary(Binary binaryValue) {
      Descriptors.EnumValueDescriptor protoValue = translateEnumValue(binaryValue);
      parent.add(protoValue);
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
      parent.add(dict[dictionaryId]);
    }

    @Override
    public boolean hasDictionarySupport() {
      return true;
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
      dict = new  Descriptors.EnumValueDescriptor[dictionary.getMaxId() + 1];
      for (int i = 0; i <= dictionary.getMaxId(); i++) {
        Binary binaryValue = dictionary.decodeToBinary(i);
        dict[i] = translateEnumValue(binaryValue);
      }
    }

  }

  final class ProtoBinaryConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoBinaryConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addBinary(Binary binary) {
      ByteString byteString = ByteString.copyFrom(binary.toByteBuffer());
      parent.add(byteString);
    }
  }


  final class ProtoBooleanConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoBooleanConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBoolean(boolean value) {
      parent.add(value);
    }

  }

  final class ProtoDoubleConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoDoubleConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addDouble(double value) {
      parent.add(value);
    }
  }

  final class ProtoFloatConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoFloatConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addFloat(float value) {
      parent.add(value);
    }
  }

  final class ProtoIntConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoIntConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addInt(int value) {
      parent.add(value);
    }
  }

  final class ProtoLongConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoLongConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addLong(long value) {
      parent.add(value);
    }
  }

  final class ProtoStringConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoStringConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addBinary(Binary binary) {
      String str = binary.toStringUsingUTF8();
      parent.add(str);
    }

  }

  /**
   * This class unwraps the additional LIST wrapper and makes it possible to read the underlying data and then convert
   * it to protobuf.
   * <p>
   * Consider the following protobuf schema:
   * message SimpleList {
   *   repeated int64 first_array = 1;
   * }
   * <p>
   * A LIST wrapper is created in parquet for the above mentioned protobuf schema:
   * message SimpleList {
   *   required group first_array (LIST) = 1 {
   *     repeated int32 element;
   *   }
   * }
   * <p>
   * The LIST wrappers are used by 3rd party tools, such as Hive, to read parquet arrays. The wrapper contains
   * one only one field: either a primitive field (like in the example above, where we have an array of ints) or
   * another group (array of messages).
   */
  final class ListConverter extends GroupConverter {
    private final Converter converter;
    private final boolean listOfMessage;

    public ListConverter(Message.Builder parentBuilder, Descriptors.FieldDescriptor fieldDescriptor, Type parquetType) {
      OriginalType originalType = parquetType.getOriginalType();
      if (originalType != OriginalType.LIST) {
        throw new ParquetDecodingException("Expected LIST wrapper. Found: " + originalType + " instead.");
      }

      listOfMessage = fieldDescriptor.getJavaType() == JavaType.MESSAGE;

      Type parquetSchema;
      if (parquetType.asGroupType().containsField("list")) {
        parquetSchema = parquetType.asGroupType().getType("list");
        if (parquetSchema.asGroupType().containsField("element")) {
          parquetSchema.asGroupType().getType("element");
        }
      } else {
        throw new ParquetDecodingException("Expected list but got: " + parquetType);
      }

      converter = newMessageConverter(parentBuilder, fieldDescriptor, parquetSchema);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      if (fieldIndex > 0) {
        throw new ParquetDecodingException("Unexpected multiple fields in the LIST wrapper");
      }

      if (listOfMessage) {
        return converter;
      }

      return new GroupConverter() {
        @Override
        public Converter getConverter(int fieldIndex) {
          return converter;
        }

        @Override
        public void start() {

        }

        @Override
        public void end() {

        }
      };
    }

    @Override
    public void start() {

    }

    @Override
    public void end() {

    }
  }


  final class MapConverter extends GroupConverter {
    private final Converter converter;

    public MapConverter(Message.Builder parentBuilder, Descriptors.FieldDescriptor fieldDescriptor, Type parquetType) {
      OriginalType originalType = parquetType.getOriginalType();
      if (originalType != OriginalType.MAP) {
        throw new ParquetDecodingException("Expected MAP wrapper. Found: " + originalType + " instead.");
      }

      Type parquetSchema;
      if (parquetType.asGroupType().containsField("key_value")){
        parquetSchema = parquetType.asGroupType().getType("key_value");
      } else {
        throw new ParquetDecodingException("Expected map but got: " + parquetType);
      }

      converter = newMessageConverter(parentBuilder, fieldDescriptor, parquetSchema);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      if (fieldIndex > 0) {
        throw new ParquetDecodingException("Unexpected multiple fields in the MAP wrapper");
      }
      return converter;
    }

    @Override
    public void start() {
    }

    @Override
    public void end() {
    }
  }
}
