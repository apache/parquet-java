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
package parquet.thrift;

import static parquet.schema.ConversionPatterns.listType;
import static parquet.schema.ConversionPatterns.mapType;
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
import static parquet.schema.Types.primitive;

import java.util.ArrayList;
import java.util.List;

import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;
import parquet.schema.Types.PrimitiveBuilder;
import parquet.thrift.projection.FieldProjectionFilter;
import parquet.thrift.projection.FieldsPath;
import parquet.thrift.projection.ThriftProjectionException;
import parquet.thrift.struct.ThriftField;
import parquet.thrift.struct.ThriftType;

/**
 * Visitor Class for converting a thrift definiton to parquet message type.
 * Projection can be done by providing a {@link FieldProjectionFilter}
 *
 * @author Tianshuo Deng
 */
public class ThriftSchemaConvertVisitor implements ThriftType.TypeVisitor {

  FieldProjectionFilter fieldProjectionFilter;
  Type currentType;
  FieldsPath currentFieldPath = new FieldsPath();
  Type.Repetition currentRepetition = Type.Repetition.REPEATED;//MessageType is repeated GroupType
  String currentName = "ParquetSchema";

  public ThriftSchemaConvertVisitor(FieldProjectionFilter fieldProjectionFilter) {
    this.fieldProjectionFilter = fieldProjectionFilter;
  }

  @Override
  public void visit(ThriftType.MapType mapType) {
    final ThriftField mapKeyField = mapType.getKey();
    final ThriftField mapValueField = mapType.getValue();

    //save env for map
    String mapName = currentName;
    Type.Repetition mapRepetition = currentRepetition;

    //=========handle key
    currentFieldPath.push(mapKeyField);
    currentName = "key";
    currentRepetition = REQUIRED;
    mapKeyField.getType().accept(this);
    Type keyType = currentType;//currentType is the already converted type
    currentFieldPath.pop();

    //=========handle value
    currentFieldPath.push(mapValueField);
    currentName = "value";
    currentRepetition = OPTIONAL;
    mapValueField.getType().accept(this);
    Type valueType = currentType;
    currentFieldPath.pop();

    if (keyType == null && valueType == null) {
      currentType = null;
      return;
    }

    if (keyType == null && valueType != null)
      throw new ThriftProjectionException("key of map is not specified in projection: " + currentFieldPath);

    //restore Env
    currentName = mapName;
    currentRepetition = mapRepetition;
    currentType = mapType(currentRepetition, currentName,
            keyType,
            valueType);
  }

  @Override
  public void visit(ThriftType.SetType setType) {
    final ThriftField setElemField = setType.getValues();
    String setName = currentName;
    Type.Repetition setRepetition = currentRepetition;
    currentName = currentName + "_tuple";
    currentRepetition = REPEATED;
    setElemField.getType().accept(this);
    //after conversion, currentType is the nested type
    if (currentType == null) {
      return;
    } else {
      currentType = listType(setRepetition, setName, currentType);
    }
  }

  @Override
  public void visit(ThriftType.ListType listType) {
    final ThriftField setElemField = listType.getValues();
    String listName = currentName;
    Type.Repetition listRepetition = currentRepetition;
    currentName = currentName + "_tuple";
    currentRepetition = REPEATED;
    setElemField.getType().accept(this);
    //after conversion, currentType is the nested type
    if (currentType == null) {
      return;
    } else {
      currentType = listType(listRepetition, listName, currentType);
    }

  }

  public MessageType getConvertedMessageType() {
    // the root should be a GroupType
    if (currentType == null)
      return new MessageType(currentName, new ArrayList<Type>());

    GroupType rootType = currentType.asGroupType();
    return new MessageType(currentName, rootType.getFields());
  }

  @Override
  public void visit(ThriftType.StructType structType) {
    List<ThriftField> fields = structType.getChildren();

    String oldName = currentName;
    Type.Repetition oldRepetition = currentRepetition;

    List<Type> types = getFieldsTypes(fields);

    currentName = oldName;
    currentRepetition = oldRepetition;
    if (types.size() > 0) {
      currentType = new GroupType(currentRepetition, currentName, types);
    } else {
      currentType = null;
    }
  }

  private List<Type> getFieldsTypes(List<ThriftField> fields) {
    List<Type> types = new ArrayList<Type>();
    for (int i = 0; i < fields.size(); i++) {
      ThriftField field = fields.get(i);
      Type.Repetition rep = getRepetition(field);
      currentRepetition = rep;
      currentName = field.getName();
      currentFieldPath.push(field);
      field.getType().accept(this);
      if (currentType != null) {
        // currentType is converted with the currentName(fieldName)
        types.add(currentType.withId(field.getFieldId()));
      }
      currentFieldPath.pop();
    }
    return types;
  }

  private boolean isCurrentlyMatchedFilter(){
     if(!fieldProjectionFilter.isMatched(currentFieldPath)){
       currentType = null;
       return false;
     }
    return true;
  }

  private void primitiveType(PrimitiveTypeName type) {
    primitiveType(type, null);
  }

  private void primitiveType(PrimitiveTypeName type, OriginalType orig) {
    if (isCurrentlyMatchedFilter()) {
      PrimitiveBuilder<PrimitiveType> b = primitive(type, currentRepetition);
      if (orig != null) {
        b = b.as(orig);
      }
      currentType = b.named(currentName);
    }
  }

  @Override
  public void visit(ThriftType.EnumType enumType) {
    primitiveType(BINARY, ENUM);
  }

  @Override
  public void visit(ThriftType.BoolType boolType) {
    primitiveType(BOOLEAN);
  }

  @Override
  public void visit(ThriftType.ByteType byteType) {
    primitiveType(INT32);
  }

  @Override
  public void visit(ThriftType.DoubleType doubleType) {
    primitiveType(DOUBLE);
  }

  @Override
  public void visit(ThriftType.I16Type i16Type) {
    primitiveType(INT32);
  }

  @Override
  public void visit(ThriftType.I32Type i32Type) {
    primitiveType(INT32);
  }

  @Override
  public void visit(ThriftType.I64Type i64Type) {
    primitiveType(INT64);
  }

  @Override
  public void visit(ThriftType.StringType stringType) {
    primitiveType(BINARY, UTF8);
  }

  /**
   * by default we can make everything optional
   *
   * @param thriftField
   * @return
   */
  private Type.Repetition getRepetition(ThriftField thriftField) {
    if (thriftField == null) {
      return OPTIONAL;
    }

    switch (thriftField.getRequirement()) {
      case REQUIRED:
        return REQUIRED;
      case OPTIONAL:
        return OPTIONAL;
      case DEFAULT:
        return OPTIONAL;
      default:
        throw new IllegalArgumentException("unknown requirement type: " + thriftField.getRequirement());
    }
  }
}
