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

import static org.apache.parquet.schema.ConversionPatterns.listOfElements;
import static org.apache.parquet.schema.ConversionPatterns.listType;
import static org.apache.parquet.schema.ConversionPatterns.mapType;
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
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ShouldNeverHappenException;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types.PrimitiveBuilder;
import org.apache.parquet.thrift.ConvertedField.Drop;
import org.apache.parquet.thrift.ConvertedField.Keep;
import org.apache.parquet.thrift.ConvertedField.SentinelUnion;
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
 */
class ThriftSchemaConvertVisitor implements ThriftType.StateVisitor<ConvertedField, ThriftSchemaConvertVisitor.State> {
  private final FieldProjectionFilter fieldProjectionFilter;
  private final boolean doProjection;
  private final boolean keepOneOfEachUnion;

  private final boolean writeThreeLevelList;

  private ThriftSchemaConvertVisitor(
      FieldProjectionFilter fieldProjectionFilter,
      boolean doProjection,
      boolean keepOneOfEachUnion,
      Configuration configuration) {
    this(fieldProjectionFilter, doProjection, keepOneOfEachUnion, new HadoopParquetConfiguration(configuration));
  }

  private ThriftSchemaConvertVisitor(
      FieldProjectionFilter fieldProjectionFilter,
      boolean doProjection,
      boolean keepOneOfEachUnion,
      ParquetConfiguration configuration) {
    this.fieldProjectionFilter =
        Objects.requireNonNull(fieldProjectionFilter, "fieldProjectionFilter cannot be null");
    this.doProjection = doProjection;
    this.keepOneOfEachUnion = keepOneOfEachUnion;
    if (configuration != null) {
      this.writeThreeLevelList = configuration.getBoolean(
          ParquetWriteProtocol.WRITE_THREE_LEVEL_LISTS, ParquetWriteProtocol.WRITE_THREE_LEVEL_LISTS_DEFAULT);
    } else {
      writeThreeLevelList = ParquetWriteProtocol.WRITE_THREE_LEVEL_LISTS_DEFAULT;
    }
  }

  private ThriftSchemaConvertVisitor(
      FieldProjectionFilter fieldProjectionFilter, boolean doProjection, boolean keepOneOfEachUnion) {
    this(fieldProjectionFilter, doProjection, keepOneOfEachUnion, new Configuration());
  }

  @Deprecated
  public static MessageType convert(StructType struct, FieldProjectionFilter filter) {
    return convert(struct, filter, true, new Configuration());
  }

  public static MessageType convert(
      StructType struct, FieldProjectionFilter filter, boolean keepOneOfEachUnion, Configuration conf) {
    return convert(struct, filter, keepOneOfEachUnion, new HadoopParquetConfiguration(conf));
  }

  public static MessageType convert(
      StructType struct, FieldProjectionFilter filter, boolean keepOneOfEachUnion, ParquetConfiguration conf) {
    State state = new State(new FieldsPath(), REPEATED, "ParquetSchema");

    ConvertedField converted =
        struct.accept(new ThriftSchemaConvertVisitor(filter, true, keepOneOfEachUnion, conf), state);

    if (!converted.isKeep()) {
      throw new ThriftProjectionException("No columns have been selected");
    }

    return new MessageType(
        state.name, converted.asKeep().getType().asGroupType().getFields());
  }

  /**
   * @deprecated this will be removed in 2.0.0.
   */
  @Deprecated
  public FieldProjectionFilter getFieldProjectionFilter() {
    return fieldProjectionFilter;
  }

  @Override
  public ConvertedField visit(MapType mapType, State state) {
    ThriftField keyField = mapType.getKey();
    ThriftField valueField = mapType.getValue();

    State keyState = new State(state.path.push(keyField), REQUIRED, "key");

    // TODO: This is a bug! this should be REQUIRED but changing this will
    // break the the schema compatibility check against old data
    // Thrift does not support null / missing map values.
    State valueState = new State(state.path.push(valueField), OPTIONAL, "value");

    ConvertedField convertedKey = keyField.getType().accept(this, keyState);
    ConvertedField convertedValue = valueField.getType().accept(this, valueState);

    if (!convertedKey.isKeep()) {

      if (convertedValue.isKeep()) {
        throw new ThriftProjectionException(
            "Cannot select only the values of a map, you must keep the keys as well: " + state.path);
      }

      // neither key nor value was requested
      return new Drop(state.path);
    }

    // we are keeping the key, but we do not allow partial projections on keys
    // as that doesn't make sense when assembling back into a map.
    // NOTE: doProjections prevents us from infinite recursion here.
    if (doProjection) {
      ConvertedField fullConvKey = keyField.getType()
          .accept(
              new ThriftSchemaConvertVisitor(
                  FieldProjectionFilter.ALL_COLUMNS, false, keepOneOfEachUnion),
              keyState);

      if (!fullConvKey.asKeep().getType().equals(convertedKey.asKeep().getType())) {
        throw new ThriftProjectionException(
            "Cannot select only a subset of the fields in a map key, " + "for path " + state.path);
      }
    }

    // now, are we keeping the value?

    if (convertedValue.isKeep()) {
      // keep both key and value

      Type mapField = mapType(
          state.repetition,
          state.name,
          convertedKey.asKeep().getType(),
          convertedValue.asKeep().getType());

      return new Keep(state.path, mapField);
    }

    // keep only the key, not the value

    ConvertedField sentinelValue = valueField
        .getType()
        .accept(
            new ThriftSchemaConvertVisitor(new KeepOnlyFirstPrimitiveFilter(), true, keepOneOfEachUnion),
            valueState);

    Type mapField = mapType(
        state.repetition,
        state.name,
        convertedKey.asKeep().getType(),
        sentinelValue.asKeep().getType()); // signals to mapType method to project the value

    return new Keep(state.path, mapField);
  }

  private ConvertedField visitListLike(ThriftField listLike, State state, boolean isSet) {
    State childState;
    if (writeThreeLevelList) {
      childState = new State(state.path, REQUIRED, "element");
    } else {
      childState = new State(state.path, REPEATED, state.name + "_tuple");
    }

    ConvertedField converted = listLike.getType().accept(this, childState);

    if (converted.isKeep()) {
      // doProjection prevents an infinite recursion here
      if (isSet && doProjection) {
        ConvertedField fullConv = listLike.getType()
            .accept(
                new ThriftSchemaConvertVisitor(
                    FieldProjectionFilter.ALL_COLUMNS, false, keepOneOfEachUnion),
                childState);
        if (!converted.asKeep().getType().equals(fullConv.asKeep().getType())) {
          throw new ThriftProjectionException(
              "Cannot select only a subset of the fields in a set, " + "for path " + state.path);
        }
      }

      if (writeThreeLevelList) {
        return new Keep(
            state.path,
            listOfElements(
                state.repetition, state.name, converted.asKeep().getType()));
      } else {
        return new Keep(
            state.path,
            listType(
                state.repetition, state.name, converted.asKeep().getType()));
      }
    }

    return new Drop(state.path);
  }

  @Override
  public ConvertedField visit(SetType setType, State state) {
    return visitListLike(setType.getValues(), state, true);
  }

  @Override
  public ConvertedField visit(ListType listType, State state) {
    return visitListLike(listType.getValues(), state, false);
  }

  @Override
  public ConvertedField visit(StructType structType, State state) {

    // special care is taken when converting unions,
    // because we are actually both converting + projecting in
    // one pass, and unions need special handling when projecting.
    final boolean needsToKeepOneOfEachUnion = keepOneOfEachUnion && isUnion(structType.getStructOrUnionType());

    boolean hasSentinelUnionColumns = false;
    boolean hasNonSentinelUnionColumns = false;

    List<Type> convertedChildren = new ArrayList<Type>();

    for (ThriftField child : structType.getChildren()) {

      State childState = new State(state.path.push(child), getRepetition(child), child.getName());

      ConvertedField converted = child.getType().accept(this, childState);

      if (!converted.isKeep() && needsToKeepOneOfEachUnion) {
        // user is not keeping this "kind" of union, but we still need
        // to keep at least one of the primitives of this union around.
        // in order to know what "kind" of union each record is.
        // TODO: in the future, we should just filter these records out instead

        // re-do the recursion, with a new projection filter that keeps only
        // the first primitive it encounters
        ConvertedField firstPrimitive = child.getType()
            .accept(
                new ThriftSchemaConvertVisitor(
                    new KeepOnlyFirstPrimitiveFilter(), true, keepOneOfEachUnion),
                childState);

        convertedChildren.add(firstPrimitive.asKeep().getType().withId(child.getFieldId()));
        hasSentinelUnionColumns = true;
      }

      if (converted.isSentinelUnion()) {
        // child field is a sentinel union that we should drop if possible
        if (childState.repetition == REQUIRED) {
          // but this field is required, so we may still need it
          convertedChildren.add(converted.asSentinelUnion().getType().withId(child.getFieldId()));
          hasSentinelUnionColumns = true;
        }
      } else if (converted.isKeep()) {
        // user has selected this column, so we keep it.
        convertedChildren.add(converted.asKeep().getType().withId(child.getFieldId()));
        hasNonSentinelUnionColumns = true;
      }
    }

    if (!hasNonSentinelUnionColumns && hasSentinelUnionColumns) {
      // this is a union, and user has not requested any of the children
      // of this union. We should drop this union, if possible, but
      // we may not be able to, so tag this as a sentinel.
      return new SentinelUnion(state.path, new GroupType(state.repetition, state.name, convertedChildren));
    }

    if (hasNonSentinelUnionColumns) {
      // user requested some of the fields of this struct, so we keep the struct
      return new Keep(state.path, new GroupType(state.repetition, state.name, convertedChildren));
    } else {
      // user requested none of the fields of this struct, so we drop it
      return new Drop(state.path);
    }
  }

  private ConvertedField visitPrimitiveType(PrimitiveTypeName type, State state) {
    return visitPrimitiveType(type, null, state);
  }

  private ConvertedField visitPrimitiveType(PrimitiveTypeName type, LogicalTypeAnnotation orig, State state) {
    PrimitiveBuilder<PrimitiveType> b = primitive(type, state.repetition);

    if (orig != null) {
      b = b.as(orig);
    }

    if (fieldProjectionFilter.keep(state.path)) {
      return new Keep(state.path, b.named(state.name));
    } else {
      return new Drop(state.path);
    }
  }

  @Override
  public ConvertedField visit(EnumType enumType, State state) {
    return visitPrimitiveType(BINARY, LogicalTypeAnnotation.enumType(), state);
  }

  @Override
  public ConvertedField visit(BoolType boolType, State state) {
    return visitPrimitiveType(BOOLEAN, state);
  }

  @Override
  public ConvertedField visit(ByteType byteType, State state) {
    return visitPrimitiveType(INT32, LogicalTypeAnnotation.intType(8, true), state);
  }

  @Override
  public ConvertedField visit(DoubleType doubleType, State state) {
    return visitPrimitiveType(DOUBLE, state);
  }

  @Override
  public ConvertedField visit(I16Type i16Type, State state) {
    return visitPrimitiveType(INT32, LogicalTypeAnnotation.intType(16, true), state);
  }

  @Override
  public ConvertedField visit(I32Type i32Type, State state) {
    return i32Type.hasLogicalTypeAnnotation()
        ? visitPrimitiveType(INT32, i32Type.getLogicalTypeAnnotation(), state)
        : visitPrimitiveType(INT32, state);
  }

  @Override
  public ConvertedField visit(I64Type i64Type, State state) {
    return i64Type.hasLogicalTypeAnnotation()
        ? visitPrimitiveType(INT64, i64Type.getLogicalTypeAnnotation(), state)
        : visitPrimitiveType(INT64, state);
  }

  @Override
  public ConvertedField visit(StringType stringType, State state) {
    if (stringType.isBinary()) {
      return stringType.hasLogicalTypeAnnotation()
          ? visitPrimitiveType(BINARY, stringType.getLogicalTypeAnnotation(), state)
          : visitPrimitiveType(BINARY, state);
    } else {
      return visitPrimitiveType(BINARY, LogicalTypeAnnotation.stringType(), state);
    }
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
