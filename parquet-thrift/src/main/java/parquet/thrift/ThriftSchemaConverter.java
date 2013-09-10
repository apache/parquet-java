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

import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;
import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.protocol.TType;
import parquet.schema.*;
import parquet.thrift.projection.FieldProjectionFilter;
import parquet.thrift.projection.FieldsPath;
import parquet.thrift.projection.ThriftProjectionException;
import parquet.thrift.struct.ThriftField;
import parquet.thrift.struct.ThriftField.Requirement;
import parquet.thrift.struct.ThriftType;
import parquet.thrift.struct.ThriftType.*;
import parquet.thrift.struct.ThriftTypeID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static parquet.schema.OriginalType.ENUM;
import static parquet.schema.OriginalType.UTF8;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static parquet.schema.Type.Repetition.*;
import static parquet.thrift.struct.ThriftField.Requirement.fromType;

public class ThriftSchemaConverter {

  private final FieldProjectionFilter fieldProjectionFilter;

  public ThriftSchemaConverter() {
    this(new FieldProjectionFilter());
  }

  public ThriftSchemaConverter(FieldProjectionFilter fieldProjectionFilter) {
    this.fieldProjectionFilter = fieldProjectionFilter;
  }

  //TODO: check TBase??
  public MessageType convert(Class thriftClass) {
    return convert(toStructType(thriftClass));
  }

  public MessageType convert(StructType thriftClass) {
//    final TStructDescriptor struct = TStructDescriptor.getInstance(thriftClass);
    FieldsPath currentFieldPath = new FieldsPath();
    return new MessageType(
            "ParquetSchema",
            toSchema(thriftClass, currentFieldPath));
  }

  private Type[] toSchema(StructType structType, FieldsPath currentFieldPath) {
    List<ThriftField> fields = structType.getChildren();
    List<Type> types = new ArrayList<Type>();
    for (int i = 0; i < fields.size(); i++) {
      ThriftField field = fields.get(i);
      Type.Repetition rep = getRepetition(field);

      currentFieldPath.push(field);
      Type currentType = toSchema(field.getName(), field, rep, currentFieldPath);
      if (currentType!=null) {
        types.add(currentType);
      }
      currentFieldPath.pop();
    }
    return types.toArray(new Type[types.size()]);
  }

  /**
   * by default we can make everything optional
   *
   * @param thriftField
   * @return
   */
  private Type.Repetition getRepetition(ThriftField thriftField) {
    if (thriftField == null) {
      return OPTIONAL; // TODO: check this
    }

    //TODO: confirm with Julien
    switch (thriftField.getRequirement()) {
      case REQUIRED:
        return REQUIRED;
      case OPTIONAL:
        return OPTIONAL;
      case DEFAULT:
        return OPTIONAL; // ??? TODO: check this
      default:
        throw new IllegalArgumentException("unknown requirement type: " + thriftField.getRequirement());
    }
  }

  private Type toSchema(String name, ThriftField thriftField, Type.Repetition rep, FieldsPath currentFieldPath) {
    ThriftType thriftType = thriftField.getType();
    if (thriftType instanceof ThriftType.ListType) {
      return convertList(name, ((ThriftType.ListType) thriftType), rep, currentFieldPath);
    } else if (thriftType instanceof ThriftType.SetType) {
      return convertSet(name, (ThriftType.SetType) thriftType, rep, currentFieldPath);
    } else if (thriftType instanceof ThriftType.StructType) {
      return convertStruct(name, (StructType) thriftType, rep, currentFieldPath);
    } else if (thriftType instanceof ThriftType.MapType) {
      return convertMap(name, (ThriftType.MapType) thriftType, rep, currentFieldPath);
    } else {
      return convertPrimitiveType(name, thriftField, rep, currentFieldPath);

    }
  }

  private Type convertPrimitiveType(String name, ThriftField field, Type.Repetition rep, FieldsPath currentFieldPath) {
    //following for leaves
    if (!fieldProjectionFilter.isMatched(currentFieldPath)) {
//      System.out.println("not matching:" + currentFieldPath.toString());
      return null;
    }
//TODO: Ask julien, what is buffer type
//    if (field.getType() instanceof ThriftType.B) {
//      convertedType.resultTupe = new PrimitiveType(rep, BINARY, name);
//    } else if (field.isEnum()) {
    if (field.getType() instanceof EnumType) {
      return new PrimitiveType(rep, BINARY, name, ENUM);
    } else {
      switch (field.getType().getType().getThriftType()) {
        case TType.I64:
          return new PrimitiveType(rep, INT64, name);
        case TType.STRING:
          return new PrimitiveType(rep, BINARY, name, UTF8);
        case TType.BOOL:
          return new PrimitiveType(rep, BOOLEAN, name);
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

  private Type convertList(String name, ThriftType.ListType listType, Type.Repetition rep, FieldsPath currentFieldPath) {
    final ThriftField listElemField = listType.getValues();
    Type nestedType = toSchema(name + "_tuple", listElemField, REPEATED, currentFieldPath);
    if (nestedType == null) {
      return null;
    }
    return  ConversionPatterns.listType(rep, name, nestedType);
  }

  private Type convertSet(String name, ThriftType.SetType setType, Type.Repetition rep, FieldsPath currentFieldPath) {
    final ThriftField setElemField = setType.getValues();
    Type nestedType = toSchema(name + "_tuple", setElemField, REPEATED, currentFieldPath);
    if (nestedType == null) {
      return null;
    }
    return ConversionPatterns.listType(rep, name, nestedType);
  }

  private Type convertStruct(String name, StructType field, Type.Repetition rep, FieldsPath currentFieldPath) {
    Type[] fields = toSchema(field, currentFieldPath);//if all child nodes dont exist, simply return null for current layer
    //A struct must have at least one required field
    //TODO if fields are empty, return null
    if (fields.length==0) {
      return null;
    }
    return  new GroupType(rep, name,fields);
  }



  private Type convertMap(String name, ThriftType.MapType mapType, Type.Repetition rep, FieldsPath currentFieldPath) {
    final ThriftField mapKeyField = mapType.getKey();
    final ThriftField mapValueField = mapType.getValue();

    currentFieldPath.push(mapKeyField);
    Type keyType = toSchema("key", mapKeyField, REQUIRED, currentFieldPath);
    currentFieldPath.pop();

    currentFieldPath.push(mapValueField);
    Type valueType = toSchema("value", mapValueField, OPTIONAL, currentFieldPath);
    currentFieldPath.pop();

    if (keyType == null && valueType == null)
      return null;
    if (keyType==null && valueType!=null)
      throw new ThriftProjectionException("key of map is not specified in projection: " + currentFieldPath);

    //TODO if not matched, then don't convert, maybe check in leaf node??
    //TODO: refactor, null is also checked in mapType method
    return  ConversionPatterns.mapType(rep, name,
            keyType,
            valueType);
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
                      fromType(field.getFieldMetaData().requirementType);
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


//  static class MatchAndRequiredFields {
//    List<Type> fields;
//    boolean hasMatched;
//
//    MatchAndRequiredFields(List<Type> fields, boolean hasMatched) {
//      this.fields = fields;
//      this.hasMatched = hasMatched;
//    }
//  }

}

