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
            getMatchedFields(toSchema(thriftClass, currentFieldPath)).fields);
  }

  private ConvertedType[] toSchema(StructType structType, FieldsPath currentFieldPath) {
    List<ThriftField> fields = structType.getChildren();
    List<ConvertedType> types = new ArrayList<ConvertedType>();
    boolean hasMatched = false;
    for (int i = 0; i < fields.size(); i++) {
      ThriftField field = fields.get(i);
//      FieldMetaData tField = field.getFieldMetaData();
//      Type.Repetition rep = getRepetition(tField);
      Type.Repetition rep = getRepetition(field);

      currentFieldPath.push(field);
      ConvertedType currentType = toSchema(field.getName(), field, rep, currentFieldPath);
      types.add(currentType);
      currentFieldPath.pop();
    }

    return types.toArray(new ConvertedType[types.size()]);
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

  private ConvertedType toSchema(String name, ThriftField thriftField, Type.Repetition rep, FieldsPath currentFieldPath) {
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

  private ConvertedType convertPrimitiveType(String name, ThriftField field, Type.Repetition rep, FieldsPath currentFieldPath) {
    //following for leaves
    ConvertedType convertedType = new ConvertedType();
    if (!fieldProjectionFilter.isMatched(currentFieldPath)) {
      System.out.println("not matching:" + currentFieldPath.toString());
      convertedType.isMatchedFilter = false;
//      return null;
    } else {
      convertedType.isMatchedFilter = true;
    }
//TODO: Ask julien, what is buffer type
//    if (field.getType() instanceof ThriftType.B) {
//      convertedType.resultTupe = new PrimitiveType(rep, BINARY, name);
//    } else if (field.isEnum()) {
    if (field.getType() instanceof EnumType) {
      convertedType.resultTupe = new PrimitiveType(rep, BINARY, name, ENUM);
    } else {
      switch (field.getType().getType().getThriftType()) {
        case TType.I64:
          convertedType.resultTupe = new PrimitiveType(rep, INT64, name);
          break;
        case TType.STRING:
          convertedType.resultTupe = new PrimitiveType(rep, BINARY, name, UTF8);
          break;
        case TType.BOOL:
          convertedType.resultTupe = new PrimitiveType(rep, BOOLEAN, name);
          break;
        case TType.I32:
          convertedType.resultTupe = new PrimitiveType(rep, INT32, name);
          break;
        case TType.BYTE:
          convertedType.resultTupe = new PrimitiveType(rep, INT32, name);
          break;
        case TType.DOUBLE:
          convertedType.resultTupe = new PrimitiveType(rep, DOUBLE, name);
          break;
        case TType.I16:
          convertedType.resultTupe = new PrimitiveType(rep, INT32, name);
          break;
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
    return convertedType;
  }

  private ConvertedType convertList(String name, ThriftType.ListType listType, Type.Repetition rep, FieldsPath currentFieldPath) {
    final ThriftField listElemField = listType.getValues();
    ConvertedType nestedType = toSchema(name + "_tuple", listElemField, REPEATED, currentFieldPath);
    if (nestedType == null) {
      return null;
    }
    return new ConvertedType(nestedType.isMatchedFilter, ConversionPatterns.listType(rep, name, nestedType.resultTupe));
  }

  private ConvertedType convertSet(String name, ThriftType.SetType setType, Type.Repetition rep, FieldsPath currentFieldPath) {
    final ThriftField setElemField = setType.getValues();
    ConvertedType nestedType = toSchema(name + "_tuple", setElemField, REPEATED, currentFieldPath);
    if (nestedType == null) {
      return null;
    }
    return new ConvertedType(nestedType.isMatchedFilter, ConversionPatterns.listType(rep, name, nestedType.resultTupe));
  }

  private ConvertedType convertStruct(String name, StructType field, Type.Repetition rep, FieldsPath currentFieldPath) {
    ConvertedType[] fields = toSchema(field, currentFieldPath);//if all child nodes dont exist, simply return null for current layer
    //A struct must have at least one required field

    MatchAndRequiredFields matchedAndRequiredFields = getMatchedFields(fields);
    return new ConvertedType(matchedAndRequiredFields.hasMatched, new GroupType(rep, name, matchedAndRequiredFields.fields));
  }

  private MatchAndRequiredFields getMatchedFields(ConvertedType[] fields) {
    List<Type> matchedAndRequiredFields = new ArrayList<Type>();
    boolean hasMatched = false;
    for (ConvertedType ct : fields) {
      if (ct.isMatchedFilter)
        hasMatched = true;
      if (ct.isMatchedFilter == true) {
        matchedAndRequiredFields.add(ct.resultTupe);
      }
    }
    //struct should has at least one field
//    if (matchedAndRequiredFields.size() == 0) {
//      System.out.println("no mathching, using first one!!!");
//      matchedAndRequiredFields.add(fields[0].resultTupe);
//    }
    return new MatchAndRequiredFields(matchedAndRequiredFields, hasMatched);
  }

  private ConvertedType convertMap(String name, ThriftType.MapType mapType, Type.Repetition rep, FieldsPath currentFieldPath) {
    final ThriftField mapKeyField = mapType.getKey();
    final ThriftField mapValueField = mapType.getValue();

    currentFieldPath.push(mapKeyField);
    ConvertedType keyType = toSchema("key", mapKeyField, REQUIRED, currentFieldPath);
    currentFieldPath.pop();

    currentFieldPath.push(mapValueField);
    ConvertedType valueType = toSchema("value", mapValueField, OPTIONAL, currentFieldPath);
    currentFieldPath.pop();

    if (keyType == null && valueType == null)
      return null;
    if (!keyType.isMatchedFilter && valueType.isMatchedFilter)
      throw new ThriftProjectionException("key of map is not specified in projection: " + currentFieldPath);

    //TODO if not matched, then don't convert, maybe check in leaf node??
    //TODO: refactor, null is also checked in mapType method
    if (!valueType.isMatchedFilter) {
      return new ConvertedType(keyType.isMatchedFilter, ConversionPatterns.mapType(rep, name,
              keyType.resultTupe,
              null));
    }
    return new ConvertedType(keyType.isMatchedFilter, ConversionPatterns.mapType(rep, name,
            keyType.resultTupe,
            valueType.resultTupe));
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

  static class ConvertedType {
    boolean isMatchedFilter;
    boolean isRequired;
    Type resultTupe;

    ConvertedType() {
    }

    ConvertedType(boolean matchedFilter, Type resultTupe) {
      isMatchedFilter = matchedFilter;
      this.resultTupe = resultTupe;
    }
  }

  static class MatchAndRequiredFields {
    List<Type> fields;
    boolean hasMatched;

    MatchAndRequiredFields(List<Type> fields, boolean hasMatched) {
      this.fields = fields;
      this.hasMatched = hasMatched;
    }
  }

}

