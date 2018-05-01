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
package org.apache.parquet.arrow.schema;

import static java.util.Arrays.asList;
import static org.apache.parquet.schema.OriginalType.DATE;
import static org.apache.parquet.schema.OriginalType.DECIMAL;
import static org.apache.parquet.schema.OriginalType.INTERVAL;
import static org.apache.parquet.schema.OriginalType.INT_16;
import static org.apache.parquet.schema.OriginalType.INT_32;
import static org.apache.parquet.schema.OriginalType.INT_64;
import static org.apache.parquet.schema.OriginalType.INT_8;
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MILLIS;
import static org.apache.parquet.schema.OriginalType.TIME_MILLIS;
import static org.apache.parquet.schema.OriginalType.TIME_MICROS;
import static org.apache.parquet.schema.OriginalType.UINT_16;
import static org.apache.parquet.schema.OriginalType.UINT_32;
import static org.apache.parquet.schema.OriginalType.UINT_64;
import static org.apache.parquet.schema.OriginalType.UINT_8;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeVisitor;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Interval;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.arrow.schema.SchemaMapping.ListTypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.PrimitiveTypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.RepeatedTypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.StructTypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.TypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.UnionTypeMapping;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.GroupBuilder;

/**
 * Logic to convert Parquet and Arrow Schemas back and forth and maintain the mapping
 */
public class SchemaConverter {

  /**
   * For when we'll need this to be configurable
   */
  public SchemaConverter() {
  }

  /**
   * Creates a Parquet Schema from an Arrow one and returns the mapping
   * @param arrowSchema the provided Arrow Schema
   * @return the mapping between the 2
   */
  public SchemaMapping fromArrow(Schema arrowSchema) {
    List<Field> fields = arrowSchema.getFields();
    List<TypeMapping> parquetFields = fromArrow(fields);
    MessageType parquetType = addToBuilder(parquetFields, Types.buildMessage()).named("root");
    return new SchemaMapping(arrowSchema, parquetType, parquetFields);
  }

  private <T> GroupBuilder<T> addToBuilder(List<TypeMapping> parquetFields, GroupBuilder<T> builder) {
    for (TypeMapping type : parquetFields) {
      builder = builder.addField(type.getParquetType());
    }
    return builder;
  }

  private List<TypeMapping> fromArrow(List<Field> fields) {
    List<TypeMapping> result = new ArrayList<>(fields.size());
    for (Field field : fields) {
      result.add(fromArrow(field));
    }
    return result;
  }

  private TypeMapping fromArrow(final Field field) {
    return fromArrow(field, field.getName());
  }

  /**
   * @param field arrow field
   * @param fieldName overrides field.getName()
   * @return mapping
   */
  private TypeMapping fromArrow(final Field field, final String fieldName) {
    final List<Field> children = field.getChildren();
    return field.getType().accept(new ArrowTypeVisitor<TypeMapping>() {

      @Override
      public TypeMapping visit(Null type) {
        // TODO(PARQUET-757): null original type
        return primitive(BINARY);
      }

      @Override
      public TypeMapping visit(Struct type) {
        List<TypeMapping> parquetTypes = fromArrow(children);
        return new StructTypeMapping(field, addToBuilder(parquetTypes, Types.buildGroup(OPTIONAL)).named(fieldName), parquetTypes);
      }

      @Override
      public TypeMapping visit(org.apache.arrow.vector.types.pojo.ArrowType.List type) {
        return createListTypeMapping();
      }

      @Override
      public TypeMapping visit(org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList type) {
        return createListTypeMapping();
      }

      private ListTypeMapping createListTypeMapping() {
        if (children.size() != 1) {
          throw new IllegalArgumentException("list fields must have exactly one child: " + field);
        }
        TypeMapping parquetChild = fromArrow(children.get(0), "element");
        GroupType list = Types.optionalList().element(parquetChild.getParquetType()).named(fieldName);
        return new ListTypeMapping(field, new List3Levels(list), parquetChild);
      }

      @Override
      public TypeMapping visit(Union type) {
        // TODO(PARQUET-756): add Union OriginalType
        List<TypeMapping> parquetTypes = fromArrow(children);
        return new UnionTypeMapping(field, addToBuilder(parquetTypes, Types.buildGroup(OPTIONAL)).named(fieldName), parquetTypes);
      }

      @Override
      public TypeMapping visit(Int type) {
        boolean signed = type.getIsSigned();
        switch (type.getBitWidth()) {
          case 8:
            return primitive(INT32, signed ? INT_8 : UINT_8);
          case 16:
            return primitive(INT32, signed ? INT_16 : UINT_16);
          case 32:
            return primitive(INT32, signed ? INT_32 : UINT_32);
          case 64:
            return primitive(INT64, signed ? INT_64 : UINT_64);
          default:
            throw new IllegalArgumentException("Illegal int type: " + field);
        }
      }

      @Override
      public TypeMapping visit(FloatingPoint type) {
        switch (type.getPrecision()) {
          case HALF:
            // TODO(PARQUET-757): original type HalfFloat
            return primitive(FLOAT);
          case SINGLE:
            return primitive(FLOAT);
          case DOUBLE:
            return primitive(DOUBLE);
          default:
            throw new IllegalArgumentException("Illegal float type: " + field);
        }
      }

      @Override
      public TypeMapping visit(Utf8 type) {
        return primitive(BINARY, UTF8);
      }

      @Override
      public TypeMapping visit(Binary type) {
        return primitive(BINARY);
      }

      @Override
      public TypeMapping visit(Bool type) {
        return primitive(BOOLEAN);
      }

      /**
       * See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal
       * @param type an arrow decimal type
       * @return a mapping from the arrow decimal to the Parquet type
       */
      @Override
      public TypeMapping visit(Decimal type) {
        int precision = type.getPrecision();
        int scale = type.getScale();
        if (1 <= precision && precision <= 9) {
          return decimal(INT32, precision, scale);
        } else if (1 <= precision && precision <= 18) {
          return decimal(INT64, precision, scale);
        } else {
          // Better: FIXED_LENGTH_BYTE_ARRAY with length
          return decimal(BINARY, precision, scale);
        }
      }

      @Override
      public TypeMapping visit(Date type) {
        return primitive(INT32, DATE);
      }

      @Override
      public TypeMapping visit(Time type) {
        int bitWidth = type.getBitWidth();
        TimeUnit timeUnit = type.getUnit();
        if (bitWidth == 32 && timeUnit == TimeUnit.MILLISECOND) {
          return primitive(INT32, TIME_MILLIS);
        } else if (bitWidth == 64 && timeUnit == TimeUnit.MICROSECOND) {
          return primitive(INT64, TIME_MICROS);
        }
        throw new UnsupportedOperationException("Unsupported type " + type);
      }

      @Override
      public TypeMapping visit(Timestamp type) {
        return primitive(INT64, TIMESTAMP_MILLIS);
      }

      /**
       * See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#interval
       */
      @Override
      public TypeMapping visit(Interval type) {
        // TODO(PARQUET-675): fix interval original types
        return primitiveFLBA(12, INTERVAL);
      }

      private TypeMapping mapping(PrimitiveType parquetType) {
        return new PrimitiveTypeMapping(field, parquetType);
      }

      private TypeMapping decimal(PrimitiveTypeName type, int precision, int scale) {
        return mapping(Types.optional(type).as(DECIMAL).precision(precision).scale(scale).named(fieldName));
      }

      private TypeMapping primitive(PrimitiveTypeName type) {
        return mapping(Types.optional(type).named(fieldName));
      }

      private TypeMapping primitive(PrimitiveTypeName type, OriginalType otype) {
        return mapping(Types.optional(type).as(otype).named(fieldName));
      }

      private TypeMapping primitiveFLBA(int length, OriginalType otype) {
        return mapping(Types.optional(FIXED_LEN_BYTE_ARRAY).length(length).as(otype).named(fieldName));
      }
    });
  }

  /**
   * Creates an Arrow Schema from an Parquet one and returns the mapping
   * @param parquetSchema the provided Parquet Schema
   * @return the mapping between the 2
   */
  public SchemaMapping fromParquet(MessageType parquetSchema) {
    List<Type> fields = parquetSchema.getFields();
    List<TypeMapping> mappings = fromParquet(fields);
    List<Field> arrowFields = fields(mappings);
    return new SchemaMapping(new Schema(arrowFields), parquetSchema, mappings);
  }

  private List<Field> fields(List<TypeMapping> mappings) {
    List<Field> result = new ArrayList<>(mappings.size());
    for (TypeMapping typeMapping : mappings) {
      result.add(typeMapping.getArrowField());
    }
    return result;
  }

  private List<TypeMapping> fromParquet(List<Type> fields) {
    List<TypeMapping> result = new ArrayList<>(fields.size());
    for (Type type : fields) {
      result.add(fromParquet(type));
    }
    return result;
  }

  private TypeMapping fromParquet(Type type) {
    return fromParquet(type, type.getName(), type.getRepetition());
  }

  /**
   * @param type parquet type
   * @param name overrides parquet.getName)
   * @param repetition overrides parquet.getRepetition()
   * @return a type mapping from the Parquet type to an Arrow type
   */
  private TypeMapping fromParquet(Type type, String name, Repetition repetition) {
    if (repetition == REPEATED) {
      // case where we have a repeated field that is not in a List/Map
      TypeMapping child = fromParquet(type, null, REQUIRED);
      Field arrowField = new Field(name, false, new ArrowType.List(), asList(child.getArrowField()));
      return new RepeatedTypeMapping(arrowField, type, child);
    }
    if (type.isPrimitive()) {
      return fromParquetPrimitive(type.asPrimitiveType(), name);
    } else {
      return fromParquetGroup(type.asGroupType(), name);
    }
  }

  /**
   * @param type parquet types
   * @param name overrides parquet.getName()
   * @return the mapping
   */
  private TypeMapping fromParquetGroup(GroupType type, String name) {
    OriginalType ot = type.getOriginalType();
    if (ot == null) {
      List<TypeMapping> typeMappings = fromParquet(type.getFields());
      Field arrowField = new Field(name, type.isRepetition(OPTIONAL), new Struct(), fields(typeMappings));
      return new StructTypeMapping(arrowField, type, typeMappings);
    } else {
      switch (ot) {
        case LIST:
          List3Levels list3Levels = new List3Levels(type);
          TypeMapping child = fromParquet(list3Levels.getElement(), null, list3Levels.getElement().getRepetition());
          Field arrowField = new Field(name, type.isRepetition(OPTIONAL), new ArrowType.List(), asList(child.getArrowField()));
          return new ListTypeMapping(arrowField, list3Levels, child);
        default:
          throw new UnsupportedOperationException("Unsupported type " + type);
      }
    }
  }

  /**
   * @param type parquet types
   * @param name overrides parquet.getName()
   * @return the mapping
   */
  private TypeMapping fromParquetPrimitive(final PrimitiveType type, final String name) {
    return type.getPrimitiveTypeName().convert(new PrimitiveType.PrimitiveTypeNameConverter<TypeMapping, RuntimeException>() {

      private TypeMapping field(ArrowType arrowType) {
        Field field = new Field(name, type.isRepetition(OPTIONAL), arrowType, null);
        return new PrimitiveTypeMapping(field, type);
      }

      @Override
      public TypeMapping convertFLOAT(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return field(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
      }

      @Override
      public TypeMapping convertDOUBLE(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return field(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
      }

      @Override
      public TypeMapping convertINT32(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        OriginalType ot = type.getOriginalType();
        if (ot == null) {
          return integer(32, true);
        }
        switch (ot) {
          case INT_8:
            return integer(8, true);
          case INT_16:
            return integer(16, true);
          case INT_32:
            return integer(32, true);
          case UINT_8:
            return integer(8, false);
          case UINT_16:
            return integer(16, false);
          case UINT_32:
            return integer(32, false);
          case DECIMAL:
            return decimal(type.getDecimalMetadata());
          case DATE:
            return field(new ArrowType.Date(DateUnit.DAY));
          case TIMESTAMP_MICROS:
            return field(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"));
          case TIMESTAMP_MILLIS:
            return field(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"));
          case TIME_MILLIS:
            return field(new ArrowType.Time(TimeUnit.MILLISECOND, 32));
          default:
          case TIME_MICROS:
          case INT_64:
          case UINT_64:
          case UTF8:
          case ENUM:
          case BSON:
          case INTERVAL:
          case JSON:
          case LIST:
          case MAP:
          case MAP_KEY_VALUE:
            throw new IllegalArgumentException("illegal type " + type);
        }
      }

      @Override
      public TypeMapping convertINT64(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        OriginalType ot = type.getOriginalType();
        if (ot == null) {
          return integer(64, true);
        }
        switch (ot) {
          case INT_8:
            return integer(8, true);
          case INT_16:
            return integer(16, true);
          case INT_32:
            return integer(32, true);
          case INT_64:
            return integer(64, true);
          case UINT_8:
            return integer(8, false);
          case UINT_16:
            return integer(16, false);
          case UINT_32:
            return integer(32, false);
          case UINT_64:
            return integer(64, false);
          case DECIMAL:
            return decimal(type.getDecimalMetadata());
          case DATE:
            return field(new ArrowType.Date(DateUnit.DAY));
          case TIMESTAMP_MICROS:
            return field(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"));
          case TIMESTAMP_MILLIS:
            return field(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"));
          default:
          case TIME_MICROS:
          case UTF8:
          case ENUM:
          case BSON:
          case INTERVAL:
          case JSON:
          case LIST:
          case MAP:
          case MAP_KEY_VALUE:
          case TIME_MILLIS:
            throw new IllegalArgumentException("illegal type " + type);
        }
      }

      @Override
      public TypeMapping convertINT96(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        // Possibly timestamp
        return field(new ArrowType.Binary());
      }

      @Override
      public TypeMapping convertFIXED_LEN_BYTE_ARRAY(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return field(new ArrowType.Binary());
      }

      @Override
      public TypeMapping convertBOOLEAN(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return field(new ArrowType.Bool());
      }

      @Override
      public TypeMapping convertBINARY(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        OriginalType ot = type.getOriginalType();
        if (ot == null) {
          return field(new ArrowType.Binary());
        }
        switch (ot) {
          case UTF8:
            return field(new ArrowType.Utf8());
          case DECIMAL:
            return decimal(type.getDecimalMetadata());
          default:
            throw new IllegalArgumentException("illegal type " + type);
        }
      }

      private TypeMapping decimal(DecimalMetadata decimalMetadata) {
        return field(new ArrowType.Decimal(decimalMetadata.getPrecision(), decimalMetadata.getScale()));
      }

      private TypeMapping integer(int width, boolean signed) {
        return field(new ArrowType.Int(width, signed));
      }
    });
  }

  /**
   * Maps a Parquet and Arrow Schema
   * For now does not validate primitive type compatibility
   * @param arrowSchema an Arrow schema
   * @param parquetSchema a Parquet message type
   * @return the mapping between the 2
   */
  public SchemaMapping map(Schema arrowSchema, MessageType parquetSchema) {
    List<TypeMapping> children = map(arrowSchema.getFields(), parquetSchema.getFields());
    return new SchemaMapping(arrowSchema, parquetSchema, children);
  }

  private List<TypeMapping> map(List<Field> arrowFields, List<Type> parquetFields) {
    if (arrowFields.size() != parquetFields.size()) {
      throw new IllegalArgumentException("Can not map schemas as sizes differ: " + arrowFields + " != " + parquetFields);
    }
    List<TypeMapping> result = new ArrayList<>(arrowFields.size());
    for (int i = 0; i < arrowFields.size(); i++) {
      Field arrowField = arrowFields.get(i);
      Type parquetField = parquetFields.get(i);
      result.add(map(arrowField, parquetField));
    }
    return result;
  }

  private TypeMapping map(final Field arrowField, final Type parquetField) {
    return arrowField.getType().accept(new ArrowTypeVisitor<TypeMapping>() {

      @Override
      public TypeMapping visit(Null type) {
        if (!parquetField.isRepetition(OPTIONAL)) {
          throw new IllegalArgumentException("Parquet type can't be null: " + parquetField);
        }
        return primitive();
      }

      @Override
      public TypeMapping visit(Struct type) {
        if (parquetField.isPrimitive()) {
          throw new IllegalArgumentException("Parquet type not a group: " + parquetField);
        }
        GroupType groupType = parquetField.asGroupType();
        return new StructTypeMapping(arrowField, groupType, map(arrowField.getChildren(), groupType.getFields()));
      }

      @Override
      public TypeMapping visit(org.apache.arrow.vector.types.pojo.ArrowType.List type) {
        return createListTypeMapping(type);
      }

      @Override
      public TypeMapping visit(org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList type) {
        return createListTypeMapping(type);
      }

      private TypeMapping createListTypeMapping(ArrowType.ComplexType type) {
        if (arrowField.getChildren().size() != 1) {
          throw new IllegalArgumentException("Invalid list type: " + type);
        }
        Field arrowChild = arrowField.getChildren().get(0);
        if (parquetField.isRepetition(REPEATED)) {
          return new RepeatedTypeMapping(arrowField, parquetField, map(arrowChild, parquetField));
        }
        if (parquetField.isPrimitive()) {
          throw new IllegalArgumentException("Parquet type not a group: " + parquetField);
        }
        List3Levels list3Levels = new List3Levels(parquetField.asGroupType());
        if (arrowField.getChildren().size() != 1) {
          throw new IllegalArgumentException("invalid arrow list: " + arrowField);
        }
        return new ListTypeMapping(arrowField, list3Levels, map(arrowChild, list3Levels.getElement()));
      }

      @Override
      public TypeMapping visit(Union type) {
        if (parquetField.isPrimitive()) {
          throw new IllegalArgumentException("Parquet type not a group: " + parquetField);
        }
        GroupType groupType = parquetField.asGroupType();
        return new UnionTypeMapping(arrowField, groupType, map(arrowField.getChildren(), groupType.getFields()));
      }

      @Override
      public TypeMapping visit(Int type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(FloatingPoint type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(Utf8 type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(Binary type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(Bool type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(Decimal type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(Date type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(Time type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(Timestamp type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(Interval type) {
        return primitive();
      }

      private TypeMapping primitive() {
        if (!parquetField.isPrimitive()) {
          throw new IllegalArgumentException("Can not map schemas as one is primitive and the other is not: " + arrowField + " != " + parquetField);
        }
        return new PrimitiveTypeMapping(arrowField, parquetField.asPrimitiveType());
      }
    });
  }
}
