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

import static org.apache.parquet.Preconditions.checkNotNull;
import static org.apache.parquet.schema.ConversionPatterns.listType;
import static org.apache.parquet.schema.ConversionPatterns.mapType;
import static org.apache.parquet.schema.OriginalType.ENUM;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.apache.parquet.schema.Types.primitive;

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.ShouldNeverHappenException;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types.PrimitiveBuilder;
import org.apache.parquet.thrift.projection.FieldProjectionFilter;
import org.apache.parquet.thrift.projection.FieldsPath;
import org.apache.parquet.thrift.projection.ThriftProjectionException;
import org.apache.parquet.thrift.struct.ThriftField;
import org.apache.parquet.thrift.struct.ThriftType;
import org.apache.parquet.thrift.struct.ThriftType.BoolType;
import org.apache.parquet.thrift.struct.ThriftType.ByteType;
import org.apache.parquet.thrift.struct.ThriftType.DoubleType;
import org.apache.parquet.thrift.struct.ThriftType.EnumType;
import org.apache.parquet.thrift.struct.ThriftType.I16Type;
import org.apache.parquet.thrift.struct.ThriftType.I32Type;
import org.apache.parquet.thrift.struct.ThriftType.I64Type;
import org.apache.parquet.thrift.struct.ThriftType.ListType;
import org.apache.parquet.thrift.struct.ThriftType.MapType;
import org.apache.parquet.thrift.struct.ThriftType.SetType;
import org.apache.parquet.thrift.struct.ThriftType.StringType;
import org.apache.parquet.thrift.struct.ThriftType.StructType;
import org.apache.parquet.thrift.struct.ThriftType.StructType.StructOrUnionType;

/**
 * Visitor Class for converting a thrift definition to parquet message type.
 * Projection can be done by providing a {@link FieldProjectionFilter}
 *
 * @author Tianshuo Deng
 */
public class ThriftSchemaConvertVisitor implements ThriftType.TypeVisitor<ConvertedField, ThriftSchemaConvertVisitor.State> {
  private final FieldProjectionFilter fieldProjectionFilter;

  private ThriftSchemaConvertVisitor(FieldProjectionFilter fieldProjectionFilter) {
    this.fieldProjectionFilter = checkNotNull(fieldProjectionFilter, "fieldProjectionFilter");
  }

  public static MessageType convert(StructType struct, FieldProjectionFilter filter) {
    State state = new State(new FieldsPath(), REPEATED, "ParquetSchema");

    ConvertedField conv = struct.accept(new ThriftSchemaConvertVisitor(filter), state);

    MessageType messageType;

    if (!conv.keep()) {
      // TODO: should this be fatal? Projecting away everything?
      messageType = new MessageType(state.name, new ArrayList<Type>());
    } else {
      messageType = new MessageType(state.name, conv.getType().asGroupType().getFields());
    }

    return messageType;
  }

  @Override
  public ConvertedField visit(MapType mapType, State state) {
    ThriftField keyField = mapType.getKey();
    ThriftField valueField = mapType.getValue();

    ConvertedField convKey = keyField.getType().accept(this,
        new State(state.path.push(keyField), REQUIRED, "key"));

    ConvertedField convValue = valueField.getType().accept(this,
        new State(state.path.push(valueField), OPTIONAL, "value"));

    if (!convKey.keep()) {

      if (convValue.keep()) {
        throw new ThriftProjectionException(
            "Cannot select only the values of a map, you must keep the keys as well: " + state.path);
      }

      // neither key nor value was requested
      return ConvertedField.drop(state.path);
    }

    // we are keeping the key, but how about the value?

    if (convValue.keep()) {
      // keep both key and value

      Type mapField = mapType(
          state.repetition,
          state.name,
          convKey.getType(),
          convValue.getType());

      return ConvertedField.keep(mapField, state.path);
    }

    // keep only the key, not the value

    Type mapField = mapType(
        state.repetition,
        state.name,
        convKey.getType(),
        null); // signals to mapType method to project the value

    return ConvertedField.keep(mapField, state.path);
  }

  private ConvertedField visitListLike(ThriftField listLike, State state) {

    ConvertedField conv = listLike
        .getType()
        .accept(this, new State(state.path, REPEATED, state.name + "_tuple"));

    if (conv.keep()) {
      return ConvertedField.keep(listType(state.repetition, state.name, conv.getType()), state.path);
    }

    return ConvertedField.drop(state.path);
  }


  @Override
  public ConvertedField visit(SetType setType, State state) {
    return visitListLike(setType.getValues(), state);
  }

  @Override
  public ConvertedField visit(ListType listType, State state) {
    return visitListLike(listType.getValues(), state);
  }

  @Override
  public ConvertedField visit(StructType structType, State state) {

    // special care is taken when converting unions,
    // because we are actually both converting + projecting in
    // one pass, and unions need special handling when projecting.
    final boolean isUnion = isUnion(structType.getStructOrUnionType());

    boolean hasSentinelUnionColumns = false;
    boolean hasNonSentinelUnionColumns = false;

    List<Type> convertedChildren = new ArrayList<Type>();

    for (ThriftField child : structType.getChildren()) {

      State childState = new State(state.path.push(child), getRepetition(child), child.getName());

      ConvertedField conv = child.getType().accept(this, childState);

      if (isUnion && !conv.keep()) {
        // user is not keeping this "kind" of union, but we still need
        // to keep at least one of the primitives of this union around.
        // in order to know what "kind" of union each record is.
        // TODO: in the future, we should just filter these records out instead

        // re-do the recursion, with a new projection filter that keeps only
        // the first primitive it encounters
        ConvertedField firstPrimitive = child.getType().accept(
            new ThriftSchemaConvertVisitor(new KeepOnlyFirstPrimitiveFilter()), childState);

        convertedChildren.add(firstPrimitive.getType().withId(child.getFieldId()));
        hasSentinelUnionColumns = true;
      }

      if (conv.isSentinel()) {
        // child field is a sentinel union that we should drop if possible
        if (childState.repetition == REQUIRED) {
          // but this field is required, so we may still need it
          convertedChildren.add(conv.getType().withId(child.getFieldId()));
          hasSentinelUnionColumns = true;
        }
      } else if (conv.keep()) {
        // user has selected this column, so we keep it.
        convertedChildren.add(conv.getType().withId(child.getFieldId()));
        hasNonSentinelUnionColumns = true;
      }

    }

    GroupType groupType = new GroupType(state.repetition, state.name, convertedChildren);

    if (!hasNonSentinelUnionColumns && hasSentinelUnionColumns) {
      // this is a union, and user has not requested any of the children
      // of this union. We should drop this union, if possible, but
      // we may not be able to, so tag this as a sentinel.
      return ConvertedField.sentinelKeep(groupType, state.path);
    }

    if (hasNonSentinelUnionColumns) {
      // user requested some of the fields of this struct, so we keep the struct
      return ConvertedField.keep(groupType, state.path);
    } else {
      // user requested none of the fields of this struct, so we drop it
      return ConvertedField.drop(state.path);
    }
  }

  private ConvertedField visitPrimitiveType(PrimitiveTypeName type, State state) {
    return visitPrimitiveType(type, null, state);
  }

  private ConvertedField visitPrimitiveType(PrimitiveTypeName type, OriginalType orig, State state) {
    PrimitiveBuilder<PrimitiveType> b = primitive(type, state.repetition);

    if (orig != null) {
      b = b.as(orig);
    }

    if (fieldProjectionFilter.keep(state.path)) {
      return ConvertedField.keep(b.named(state.name), state.path);
    } else {
      return ConvertedField.drop(state.path);
    }
  }

  @Override
  public ConvertedField visit(EnumType enumType, State state) {
    return visitPrimitiveType(BINARY, ENUM, state);
  }

  @Override
  public ConvertedField visit(BoolType boolType, State state) {
    return visitPrimitiveType(BOOLEAN, state);
  }

  @Override
  public ConvertedField visit(ByteType byteType, State state) {
    return visitPrimitiveType(INT32, state);
  }

  @Override
  public ConvertedField visit(DoubleType doubleType, State state) {
    return visitPrimitiveType(DOUBLE, state);
  }

  @Override
  public ConvertedField visit(I16Type i16Type, State state) {
    return visitPrimitiveType(INT32, state);
  }

  @Override
  public ConvertedField visit(I32Type i32Type, State state) {
    return visitPrimitiveType(INT32, state);
  }

  @Override
  public ConvertedField visit(I64Type i64Type, State state) {
    return visitPrimitiveType(INT64, state);
  }

  @Override
  public ConvertedField visit(StringType stringType, State state) {
    return visitPrimitiveType(BINARY, UTF8, state);
  }

  private static boolean isUnion(StructOrUnionType s) {
    switch (s) {
      case STRUCT:
        return false;
      case UNION:
        return true;
      case UNKNOWN:
        throw new ShouldNeverHappenException("Encountered UNKNOWN StructOrUnionType");
      default:
        throw new ShouldNeverHappenException("Unrecognized type: " + s);
    }
  }

  private Type.Repetition getRepetition(ThriftField thriftField) {
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

  /**
   * The state passed through the recursion as we traverse a thrift schema.
   */
  public static final class State {
    public final FieldsPath path; // current field path
    public final Type.Repetition repetition; // current repetition (no relation to parent's repetition)
    public final String name; // name of the field located at path

    public State(FieldsPath path, Repetition repetition, String name) {
      this.path = path;
      this.repetition = repetition;
      this.name = name;
    }
  }

}
