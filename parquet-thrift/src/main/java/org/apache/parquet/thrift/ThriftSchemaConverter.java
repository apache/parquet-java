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
package org.apache.parquet.thrift;

import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;
import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.TUnion;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.thrift.projection.FieldProjectionFilter;
import org.apache.parquet.thrift.struct.ThriftField;
import org.apache.parquet.thrift.struct.ThriftField.Requirement;
import org.apache.parquet.thrift.struct.ThriftType;
import org.apache.parquet.thrift.struct.ThriftType.*;
import org.apache.parquet.thrift.struct.ThriftType.StructType.StructOrUnionType;
import org.apache.parquet.thrift.struct.ThriftTypeID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Given a thrift class, this class converts it to parquet schema,
 * a {@link FieldProjectionFilter} can be specified for projection pushdown.
 */
public class ThriftSchemaConverter {
  private final FieldProjectionFilter fieldProjectionFilter;

  public ThriftSchemaConverter() {
    this(FieldProjectionFilter.ALL_COLUMNS);
  }

  public ThriftSchemaConverter(FieldProjectionFilter fieldProjectionFilter) {
    this.fieldProjectionFilter = fieldProjectionFilter;
  }

  public MessageType convert(Class<? extends TBase<?, ?>> thriftClass) {
    return convert(toStructType(thriftClass));
  }

  public MessageType convert(StructType thriftClass) {
    ThriftSchemaConvertVisitor visitor = new ThriftSchemaConvertVisitor(fieldProjectionFilter);
    thriftClass.accept(visitor);
    fieldProjectionFilter.assertNoUnmatchedPatterns();
    MessageType convertedMessageType = visitor.getConvertedMessageType();
    return convertedMessageType;
  }

  public static <T extends TBase<?,?>> StructOrUnionType structOrUnionType(Class<T> klass) {
    return TUnion.class.isAssignableFrom(klass) ? StructOrUnionType.UNION : StructOrUnionType.STRUCT;
  }

  public static ThriftType.StructType toStructType(Class<? extends TBase<?, ?>> thriftClass) {
    final TStructDescriptor struct = TStructDescriptor.getInstance(thriftClass);
    return toStructType(struct);
  }

  private static StructType toStructType(TStructDescriptor struct) {
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
    return new StructType(children, structOrUnionType(struct.getThriftClass()));
  }

  private static ThriftField toThriftField(String name, Field field, ThriftField.Requirement requirement) {
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

