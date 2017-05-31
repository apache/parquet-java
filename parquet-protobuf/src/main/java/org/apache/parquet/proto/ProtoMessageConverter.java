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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.IncompatibleSchemaModificationException;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.Type;
import org.omg.CORBA.DynAnyPackage.Invalid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(ProtoMessageConverter.class);

  private final Converter[] converters;
  private final ParentValueContainer parent;
  private final Message.Builder messageBuilder;
  private final boolean readFieldById;

  // used in record converter
  ProtoMessageConverter(ParentValueContainer pvc, Class<? extends Message> protoClass, GroupType parquetSchema, boolean readFieldById) {
    this(pvc, Protobufs.getMessageBuilder(protoClass), parquetSchema, readFieldById);
  }

  // For usage in message arrays
  ProtoMessageConverter(ParentValueContainer pvc, Message.Builder builder, GroupType parquetSchema, boolean readFieldById) {
    if (pvc == null) {
      throw new IllegalStateException("Missing parent value container");
    }
    this.parent = pvc;
    this.readFieldById = readFieldById;
    this.messageBuilder = builder;
    this.converters = Lists.transform(parquetSchema.getFields(), convertField())
      .toArray(new Converter[parquetSchema.getFieldCount()]);
  }

  private Function<? super Type,?> convertField() {
    return readFieldById ? new ConvertFieldById() : new ConvertFieldByName();
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
    parent.add(messageBuilder.build());
    messageBuilder.clear();
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

    return newScalarConverter(parent, parentBuilder, fieldDescriptor, parquetType);
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
        return new ProtoMessageConverter(pvc, subBuilder, parquetType.asGroupType(), readFieldById);
      }
    }

    throw new UnsupportedOperationException(String.format("Cannot convert type: %s" +
            " (Parquet type: %s) ", javaType, parquetType));
  }

  public Message.Builder getBuilder() {
    return messageBuilder;
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
   * Map parquet and protobuf schema by field names, error if no field found, this is the old behavior.
   */
  private final class ConvertFieldByName implements Function<Type, Converter> {

    @Override
    public Converter apply(Type parquetField) {
      Descriptors.FieldDescriptor protoField = messageBuilder.getDescriptorForType().findFieldByName(parquetField.getName());
      if (protoField == null) {
        String description = "Scheme mismatch \n\"" + parquetField + "\"" +
                "\n proto descriptor:\n" + messageBuilder.getDescriptorForType().toProto();
        throw new IncompatibleSchemaModificationException("Cant find \"" + parquetField.getName() + "\" " + description);
      }

      return newMessageConverter(messageBuilder, protoField, parquetField);
    }
  }

  /**
   * Map parquet and protobuf schema by field id, if no field found, treat it as unknown field (for the moment, just
   * ignore the field).
   */
  private final class ConvertFieldById implements Function<Type, Converter> {

    @Override
    public Converter apply(Type parquetField) {
      final Type.ID parquetFieldId = parquetField.getId();
      if (parquetFieldId == null) {
        throw new InvalidSchemaException("Schema should have field id, but field <" + parquetField.toString()
          + "> has no id. Need to disable the flag " + ParquetMetadataConverter.PARQUET_SCHEMA_FIELD_WITH_ID
          + " in configuration to read it.");
      }
      Descriptors.FieldDescriptor protoField = messageBuilder.getDescriptorForType().findFieldByNumber(parquetFieldId.intValue());
      if (protoField == null) {
        LOG.warn("Cannot find corresponding field in protobuf schema for: " + parquetField.toString());
        return parquetField.isPrimitive() ? UnknownFieldIgnorePrimitiveConverter.INSTANCE
          : new UnknownFieldIgnoreGroupConverter(parquetField.asGroupType());
      }
      return newMessageConverter(messageBuilder, protoField, parquetField);
    }
  }
}
