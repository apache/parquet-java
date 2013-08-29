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
package parquet.thrift;

import static parquet.schema.OriginalType.ENUM;
import static parquet.schema.OriginalType.UTF8;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static parquet.schema.Type.Repetition.OPTIONAL;
import static parquet.schema.Type.Repetition.REPEATED;
import static parquet.schema.Type.Repetition.REQUIRED;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.TFieldRequirementType;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.protocol.TType;

import parquet.schema.ConversionPatterns;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;
import parquet.thrift.projection.FieldProjectionFilter;
import parquet.thrift.projection.ThriftProjectionException;
import parquet.thrift.struct.ThriftField;
import parquet.thrift.struct.ThriftType;
import parquet.thrift.struct.ThriftField.Requirement;
import parquet.thrift.struct.ThriftType.BoolType;
import parquet.thrift.struct.ThriftType.ByteType;
import parquet.thrift.struct.ThriftType.DoubleType;
import parquet.thrift.struct.ThriftType.EnumType;
import parquet.thrift.struct.ThriftType.EnumValue;
import parquet.thrift.struct.ThriftType.I16Type;
import parquet.thrift.struct.ThriftType.I32Type;
import parquet.thrift.struct.ThriftType.I64Type;
import parquet.thrift.struct.ThriftType.StringType;
import parquet.thrift.struct.ThriftType.StructType;
import parquet.thrift.struct.ThriftTypeID;

import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;

public class ThriftSchemaConverter {

  /**
   * configuration key for thrift read projection schema
   */
  public static final String THRIFT_READ_FILTER = "thrift.read.filter";

  private final FieldProjectionFilter fieldProjectionFilter;

  public ThriftSchemaConverter() {
    this(new FieldProjectionFilter());
  }

  public ThriftSchemaConverter(FieldProjectionFilter fieldProjectionFilter) {
    this.fieldProjectionFilter = fieldProjectionFilter;
  }

  public MessageType convert(Class<? extends TBase<?, ?>> thriftClass) {
    final TStructDescriptor struct = TStructDescriptor.getInstance(thriftClass);
    List<String> currentFieldPath = new ArrayList<String>();
    return new MessageType(
            thriftClass.getSimpleName(),
            toSchema(struct, fieldProjectionFilter, currentFieldPath));
  }

  private Type[] toSchema(TStructDescriptor struct, FieldProjectionFilter filter, List<String> currentFieldPath) {
    List<Field> fields = struct.getFields();
    List<Type> types = new ArrayList<Type>();
    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      FieldMetaData tField = field.getFieldMetaData();
      Type.Repetition rep = getRepetition(tField);
      currentFieldPath.add(field.getName());
      Type currentType = toSchema(field.getName(), field, rep, filter, currentFieldPath);
      if (currentType != null)
        types.add(currentType);
      currentFieldPath.remove(currentFieldPath.size() - 1);
    }
    return types.toArray(new Type[types.size()]);
  }

  /**
   * by default we can make everything optional
   * @param tField
   * @return
   */
  private Type.Repetition getRepetition(FieldMetaData tField) {
    if (tField == null) {
      return OPTIONAL; // TODO: check this
    }
    switch (tField.requirementType) {
    case TFieldRequirementType.REQUIRED:
      return REQUIRED;
    case TFieldRequirementType.OPTIONAL:
      return OPTIONAL;
    case TFieldRequirementType.DEFAULT:
      return OPTIONAL; // ??? TODO: check this
    default:
      throw new IllegalArgumentException("unknown requirement type: " + tField.requirementType);
    }
  }

  private Type toSchema(String name, Field field, Type.Repetition rep, FieldProjectionFilter filter, List<String> currentFieldPath) {
//    System.out.println(currentFieldPath);
    if (field.isList()) {
      final Field listElemField = field.getListElemField();
      Type nestedType = toSchema(name + "_tuple", listElemField, REPEATED, filter, currentFieldPath);
      if (nestedType == null)
        return null;
      return ConversionPatterns.listType(rep, name, nestedType);
    } else if (field.isSet()) {
      final Field setElemField = field.getSetElemField();
      Type nestedType = toSchema(name + "_tuple", setElemField, REPEATED, filter, currentFieldPath);
      if (nestedType == null)
        return null;
      return ConversionPatterns.listType(rep, name, nestedType);
    } else if (field.isStruct()) {
      Type[] fields = toSchema(field.gettStructDescriptor(), filter, currentFieldPath);//if all child nodes dont exist, simply return null for current layer
      if (fields.length == 0)
        return null;
      return new GroupType(rep, name, fields);

    } else if (field.isMap()) {
      final Field mapKeyField = field.getMapKeyField();
      final Field mapValueField = field.getMapValueField();

      currentFieldPath.add("key");
      Type keyType = toSchema("key", mapKeyField, REQUIRED, filter, currentFieldPath);
      currentFieldPath.remove(currentFieldPath.size() - 1);

      currentFieldPath.add("value");
      Type valueType = toSchema("value", mapValueField, OPTIONAL, filter, currentFieldPath);
      currentFieldPath.remove(currentFieldPath.size() - 1);

      //TODO: Throw exception when either is null
      if (keyType == null && valueType == null)
        return null;
      if (keyType == null)
        throw new ThriftProjectionException("key of map is not specified in projection: " + currentFieldPath);
      if (valueType == null)
        throw new ThriftProjectionException("value of map is not specified in projection: " + currentFieldPath);
      return ConversionPatterns.mapType(rep, name,
              keyType,
              valueType);
    } else {
      //following for leaves
      if (!fieldProjectionFilter.isMatched(currentFieldPath))
        return null;

      if (field.isBuffer()) {
        return new PrimitiveType(rep, BINARY, name);
      } else if (field.isEnum()) {
        return new PrimitiveType(rep, BINARY, name, ENUM);
      } else {
        switch (field.getType()) {
          case TType.I64:
            return new PrimitiveType(rep, INT64, name);
          case TType.STRING:
            return new PrimitiveType(rep, BINARY, name, UTF8);
          case TType.BOOL:
            return new PrimitiveType(rep, BOOLEAN, name); // TODO: elephantbird does int
          case TType.I32:
            return new PrimitiveType(rep, INT32, name);
          case TType.BYTE:
            return new PrimitiveType(rep, INT32, name);
          case TType.DOUBLE:
            return new PrimitiveType(rep, DOUBLE, name);
          case TType.I16:
            return new PrimitiveType(rep, INT32, name);
          case TType.MAP:
          case TType.ENUM:
          case TType.SET:
          case TType.LIST:
          case TType.STRUCT:
          case TType.STOP:
          case TType.VOID:
          default:
            throw new RuntimeException("unsupported type " + field.getType() + " " + field.getName());
        }
      }
    }
  }

  public ThriftType.StructType toStructType(Class<? extends TBase<?, ?>> thriftClass) {
    final TStructDescriptor struct = TStructDescriptor.getInstance(thriftClass);
    return toStructType(struct);
  }

  private StructType toStructType(TStructDescriptor struct) {
    List<Field> fields = struct.getFields();
    List<ThriftField> children = new ArrayList<ThriftField>(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      Requirement req =
          field.getFieldMetaData() == null ?
              Requirement.OPTIONAL :
                Requirement.fromType(field.getFieldMetaData().requirementType);
      children.add(toThriftField(field.getName(), field, req));
    }
    return new StructType(children);
  }

  private ThriftField toThriftField(String name, Field field, ThriftField.Requirement requirement) {
    ThriftType type;
    switch (ThriftTypeID.fromByte(field.getType())) {
    case STOP:
    case VOID:
    default:
      throw new UnsupportedOperationException("can't convert type of " + field);
    case BOOL:
      type = new BoolType();
      break;
    case BYTE:
      type = new ByteType();
      break;
    case DOUBLE:
      type = new DoubleType();
      break;
    case I16:
      type = new I16Type();
      break;
    case I32:
      type = new I32Type();
      break;
    case I64:
      type = new I64Type();
      break;
    case STRING:
      type = new StringType();
      break;
    case STRUCT:
      type = toStructType(field.gettStructDescriptor());
      break;
    case MAP:
      final Field mapKeyField = field.getMapKeyField();
      final Field mapValueField = field.getMapValueField();
      type = new ThriftType.MapType(
          toThriftField(mapKeyField.getName(), mapKeyField, requirement),
          toThriftField(mapValueField.getName(), mapValueField, requirement));
      break;
    case SET:
      final Field setElemField = field.getSetElemField();
      type = new ThriftType.SetType(toThriftField(name, setElemField, requirement));
      break;
    case LIST:
      final Field listElemField = field.getListElemField();
      type = new ThriftType.ListType(toThriftField(name, listElemField, requirement));
      break;
    case ENUM:
      Collection<TEnum> enumValues = field.getEnumValues();
      List<EnumValue> values = new ArrayList<ThriftType.EnumValue>();
      for (TEnum tEnum : enumValues) {
        values.add(new EnumValue(tEnum.getValue(), tEnum.toString()));
      }
      type = new EnumType(values);
      break;
    }
    return new ThriftField(name, field.getId(), requirement, type);
  }

}

