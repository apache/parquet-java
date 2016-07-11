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
package org.apache.parquet.avro;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.avro.JsonProperties.NULL_VALUE;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE_DEFAULT;
import static org.apache.parquet.schema.OriginalType.*;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;

/**
 * <p>
 * Converts an Avro schema into a Parquet schema, or vice versa. See package
 * documentation for details of the mapping.
 * </p>
 */
public class AvroSchemaConverter {

  public static final String ADD_LIST_ELEMENT_RECORDS =
      "parquet.avro.add-list-element-records";
  private static final boolean ADD_LIST_ELEMENT_RECORDS_DEFAULT = false;

  private final boolean assumeRepeatedIsListElement;
  private final boolean writeOldListStructure;

  public AvroSchemaConverter() {
    this.assumeRepeatedIsListElement = ADD_LIST_ELEMENT_RECORDS_DEFAULT;
    this.writeOldListStructure = WRITE_OLD_LIST_STRUCTURE_DEFAULT;
  }

  /**
   * Constructor used by {@link AvroRecordConverter#isElementType}, which always
   * uses the 2-level list conversion.
   *
   * @param assumeRepeatedIsListElement whether to assume 2-level lists
   */
  AvroSchemaConverter(boolean assumeRepeatedIsListElement) {
    this.assumeRepeatedIsListElement = assumeRepeatedIsListElement;
    this.writeOldListStructure = WRITE_OLD_LIST_STRUCTURE_DEFAULT;
  }

  public AvroSchemaConverter(Configuration conf) {
    this.assumeRepeatedIsListElement = conf.getBoolean(
        ADD_LIST_ELEMENT_RECORDS, ADD_LIST_ELEMENT_RECORDS_DEFAULT);
    this.writeOldListStructure = conf.getBoolean(
        WRITE_OLD_LIST_STRUCTURE, WRITE_OLD_LIST_STRUCTURE_DEFAULT);
  }

  /**
   * Given a schema, check to see if it is a union of a null type and a regular schema,
   * and then return the non-null sub-schema. Otherwise, return the given schema.
   *
   * @param schema The schema to check
   * @return The non-null portion of a union schema, or the given schema
   */
  public static Schema getNonNull(Schema schema) {
    if (schema.getType().equals(Schema.Type.UNION)) {
      List<Schema> schemas = schema.getTypes();
      if (schemas.size() == 2) {
        if (schemas.get(0).getType().equals(Schema.Type.NULL)) {
          return schemas.get(1);
        } else if (schemas.get(1).getType().equals(Schema.Type.NULL)) {
          return schemas.get(0);
        } else {
          return schema;
        }
      } else {
        return schema;
      }
    } else {
      return schema;
    }
  }

  public MessageType convert(Schema avroSchema) {
    if (!avroSchema.getType().equals(Schema.Type.RECORD)) {
      throw new IllegalArgumentException("Avro schema must be a record.");
    }
    return new MessageType(avroSchema.getFullName(), convertFields(avroSchema.getFields()));
  }

  private List<Type> convertFields(List<Schema.Field> fields) {
    List<Type> types = new ArrayList<Type>();
    for (Schema.Field field : fields) {
      if (field.schema().getType().equals(Schema.Type.NULL)) {
        continue; // Avro nulls are not encoded, unless they are null unions
      }
      types.add(convertField(field));
    }
    return types;
  }

  private Type convertField(String fieldName, Schema schema) {
    return convertField(fieldName, schema, Type.Repetition.REQUIRED);
  }

  @SuppressWarnings("deprecation")
  private Type convertField(String fieldName, Schema schema, Type.Repetition repetition) {
    Types.PrimitiveBuilder<PrimitiveType> builder;
    Schema.Type type = schema.getType();
    if (type.equals(Schema.Type.BOOLEAN)) {
      builder = Types.primitive(BOOLEAN, repetition);
    } else if (type.equals(Schema.Type.INT)) {
      builder = Types.primitive(INT32, repetition);
    } else if (type.equals(Schema.Type.LONG)) {
      builder = Types.primitive(INT64, repetition);
    } else if (type.equals(Schema.Type.FLOAT)) {
      builder = Types.primitive(FLOAT, repetition);
    } else if (type.equals(Schema.Type.DOUBLE)) {
      builder = Types.primitive(DOUBLE, repetition);
    } else if (type.equals(Schema.Type.BYTES)) {
      builder = Types.primitive(BINARY, repetition);
    } else if (type.equals(Schema.Type.STRING)) {
      builder = Types.primitive(BINARY, repetition).as(UTF8);
    } else if (type.equals(Schema.Type.RECORD)) {
      return new GroupType(repetition, fieldName, convertFields(schema.getFields()));
    } else if (type.equals(Schema.Type.ENUM)) {
      builder = Types.primitive(BINARY, repetition).as(ENUM);
    } else if (type.equals(Schema.Type.ARRAY)) {
      if (writeOldListStructure) {
        return ConversionPatterns.listType(repetition, fieldName,
            convertField("array", schema.getElementType(), REPEATED));
      } else {
        return ConversionPatterns.listOfElements(repetition, fieldName,
            convertField(AvroWriteSupport.LIST_ELEMENT_NAME, schema.getElementType()));
      }
    } else if (type.equals(Schema.Type.MAP)) {
      Type valType = convertField("value", schema.getValueType());
      // avro map key type is always string
      return ConversionPatterns.stringKeyMapType(repetition, fieldName, valType);
    } else if (type.equals(Schema.Type.FIXED)) {
      builder = Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
          .length(schema.getFixedSize());
    } else if (type.equals(Schema.Type.UNION)) {
      return convertUnion(fieldName, schema, repetition);
    } else {
      throw new UnsupportedOperationException("Cannot convert Avro type " + type);
    }

    // schema translation can only be done for known logical types because this
    // creates an equivalence
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null) {
      if (logicalType instanceof LogicalTypes.Decimal) {
        builder = builder.as(DECIMAL)
            .precision(((LogicalTypes.Decimal) logicalType).getPrecision())
            .scale(((LogicalTypes.Decimal) logicalType).getScale());

      } else {
        OriginalType annotation = convertLogicalType(logicalType);
        if (annotation != null) {
          builder.as(annotation);
        }
      }
    }

    return builder.named(fieldName);
  }

  private Type convertUnion(String fieldName, Schema schema, Type.Repetition repetition) {
    List<Schema> nonNullSchemas = new ArrayList<Schema>(schema.getTypes().size());
    for (Schema childSchema : schema.getTypes()) {
      if (childSchema.getType().equals(Schema.Type.NULL)) {
        if (Type.Repetition.REQUIRED == repetition) {
          repetition = Type.Repetition.OPTIONAL;
        }
      } else {
        nonNullSchemas.add(childSchema);
      }
    }
    // If we only get a null and one other type then its a simple optional field
    // otherwise construct a union container
    switch (nonNullSchemas.size()) {
      case 0:
        throw new UnsupportedOperationException("Cannot convert Avro union of only nulls");

      case 1:
        return convertField(fieldName, nonNullSchemas.get(0), repetition);

      default: // complex union type
        List<Type> unionTypes = new ArrayList<Type>(nonNullSchemas.size());
        int index = 0;
        for (Schema childSchema : nonNullSchemas) {
          unionTypes.add( convertField("member" + index++, childSchema, Type.Repetition.OPTIONAL));
        }
        return new GroupType(repetition, fieldName, unionTypes);
    }
  }

  private Type convertField(Schema.Field field) {
    return convertField(field.name(), field.schema());
  }

  public Schema convert(MessageType parquetSchema) {
    return convertFields(parquetSchema.getName(), parquetSchema.getFields());
  }

  Schema convert(GroupType parquetSchema) {
    return convertFields(parquetSchema.getName(), parquetSchema.getFields());
  }

  private Schema convertFields(String name, List<Type> parquetFields) {
    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    for (Type parquetType : parquetFields) {
      Schema fieldSchema = convertField(parquetType);
      if (parquetType.isRepetition(REPEATED)) {
        throw new UnsupportedOperationException("REPEATED not supported outside LIST or MAP. Type: " + parquetType);
      } else if (parquetType.isRepetition(Type.Repetition.OPTIONAL)) {
        fields.add(new Schema.Field(
            parquetType.getName(), optional(fieldSchema), null, NULL_VALUE));
      } else { // REQUIRED
        fields.add(new Schema.Field(
            parquetType.getName(), fieldSchema, null, (Object) null));
      }
    }
    Schema schema = Schema.createRecord(name, null, null, false);
    schema.setFields(fields);
    return schema;
  }

  private Schema convertField(final Type parquetType) {
    if (parquetType.isPrimitive()) {
      final PrimitiveType asPrimitive = parquetType.asPrimitiveType();
      final PrimitiveTypeName parquetPrimitiveTypeName =
          asPrimitive.getPrimitiveTypeName();
      final OriginalType annotation = parquetType.getOriginalType();
      Schema schema = parquetPrimitiveTypeName.convert(
          new PrimitiveType.PrimitiveTypeNameConverter<Schema, RuntimeException>() {
            @Override
            public Schema convertBOOLEAN(PrimitiveTypeName primitiveTypeName) {
              return Schema.create(Schema.Type.BOOLEAN);
            }
            @Override
            public Schema convertINT32(PrimitiveTypeName primitiveTypeName) {
              return Schema.create(Schema.Type.INT);
            }
            @Override
            public Schema convertINT64(PrimitiveTypeName primitiveTypeName) {
              return Schema.create(Schema.Type.LONG);
            }
            @Override
            public Schema convertINT96(PrimitiveTypeName primitiveTypeName) {
              throw new IllegalArgumentException("INT96 not yet implemented.");
            }
            @Override
            public Schema convertFLOAT(PrimitiveTypeName primitiveTypeName) {
              return Schema.create(Schema.Type.FLOAT);
            }
            @Override
            public Schema convertDOUBLE(PrimitiveTypeName primitiveTypeName) {
              return Schema.create(Schema.Type.DOUBLE);
            }
            @Override
            public Schema convertFIXED_LEN_BYTE_ARRAY(PrimitiveTypeName primitiveTypeName) {
              int size = parquetType.asPrimitiveType().getTypeLength();
              return Schema.createFixed(parquetType.getName(), null, null, size);
            }
            @Override
            public Schema convertBINARY(PrimitiveTypeName primitiveTypeName) {
              if (annotation == OriginalType.UTF8 || annotation == OriginalType.ENUM) {
                return Schema.create(Schema.Type.STRING);
              } else {
                return Schema.create(Schema.Type.BYTES);
              }
            }
          });

      LogicalType logicalType = convertOriginalType(
          annotation, asPrimitive.getDecimalMetadata());
      if (logicalType != null && (annotation != DECIMAL ||
          parquetPrimitiveTypeName == BINARY ||
          parquetPrimitiveTypeName == FIXED_LEN_BYTE_ARRAY)) {
        schema = logicalType.addToSchema(schema);
      }

      return schema;

    } else {
      GroupType parquetGroupType = parquetType.asGroupType();
      OriginalType originalType = parquetGroupType.getOriginalType();
      if (originalType != null) {
        switch(originalType) {
          case LIST:
            if (parquetGroupType.getFieldCount()!= 1) {
              throw new UnsupportedOperationException("Invalid list type " + parquetGroupType);
            }
            Type repeatedType = parquetGroupType.getType(0);
            if (!repeatedType.isRepetition(REPEATED)) {
              throw new UnsupportedOperationException("Invalid list type " + parquetGroupType);
            }
            if (isElementType(repeatedType, parquetGroupType.getName())) {
              // repeated element types are always required
              return Schema.createArray(convertField(repeatedType));
            } else {
              Type elementType = repeatedType.asGroupType().getType(0);
              if (elementType.isRepetition(Type.Repetition.OPTIONAL)) {
                return Schema.createArray(optional(convertField(elementType)));
              } else {
                return Schema.createArray(convertField(elementType));
              }
            }
          case MAP_KEY_VALUE: // for backward-compatibility
          case MAP:
            if (parquetGroupType.getFieldCount() != 1 || parquetGroupType.getType(0).isPrimitive()) {
              throw new UnsupportedOperationException("Invalid map type " + parquetGroupType);
            }
            GroupType mapKeyValType = parquetGroupType.getType(0).asGroupType();
            if (!mapKeyValType.isRepetition(REPEATED) ||
                mapKeyValType.getFieldCount()!=2) {
              throw new UnsupportedOperationException("Invalid map type " + parquetGroupType);
            }
            Type keyType = mapKeyValType.getType(0);
            if (!keyType.isPrimitive() ||
                !keyType.asPrimitiveType().getPrimitiveTypeName().equals(PrimitiveTypeName.BINARY) ||
                !keyType.getOriginalType().equals(OriginalType.UTF8)) {
              throw new IllegalArgumentException("Map key type must be binary (UTF8): "
                  + keyType);
            }
            Type valueType = mapKeyValType.getType(1);
            if (valueType.isRepetition(Type.Repetition.OPTIONAL)) {
              return Schema.createMap(optional(convertField(valueType)));
            } else {
              return Schema.createMap(convertField(valueType));
            }
          case ENUM:
            return Schema.create(Schema.Type.STRING);
          case UTF8:
          default:
            throw new UnsupportedOperationException("Cannot convert Parquet type " +
                parquetType);

        }
      } else {
        // if no original type then it's a record
        return convertFields(parquetGroupType.getName(), parquetGroupType.getFields());
      }
    }
  }

  private OriginalType convertLogicalType(LogicalType logicalType) {
    if (logicalType == null) {
      return null;
    } else if (logicalType instanceof LogicalTypes.Decimal) {
      return OriginalType.DECIMAL;
    } else if (logicalType instanceof LogicalTypes.Date) {
      return OriginalType.DATE;
    } else if (logicalType instanceof LogicalTypes.TimeMillis) {
      return OriginalType.TIME_MILLIS;
    } else if (logicalType instanceof LogicalTypes.TimeMicros) {
      return OriginalType.TIME_MICROS;
    } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
      return OriginalType.TIMESTAMP_MILLIS;
    } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
      return OriginalType.TIMESTAMP_MICROS;
    }
    return null;
  }

  private LogicalType convertOriginalType(OriginalType annotation, DecimalMetadata meta) {
    if (annotation == null) {
      return null;
    }
    switch (annotation) {
      case DECIMAL:
        return LogicalTypes.decimal(meta.getPrecision(), meta.getScale());
      case DATE:
        return LogicalTypes.date();
      case TIME_MILLIS:
        return LogicalTypes.timeMillis();
      case TIME_MICROS:
        return LogicalTypes.timeMicros();
      case TIMESTAMP_MILLIS:
        return LogicalTypes.timestampMillis();
      case TIMESTAMP_MICROS:
        return LogicalTypes.timestampMicros();
    }
    return null;
  }

  /**
   * Implements the rules for interpreting existing data from the logical type
   * spec for the LIST annotation. This is used to produce the expected schema.
   * <p>
   * The AvroArrayConverter will decide whether the repeated type is the array
   * element type by testing whether the element schema and repeated type are
   * the same. This ensures that the LIST rules are followed when there is no
   * schema and that a schema can be provided to override the default behavior.
   */
  private boolean isElementType(Type repeatedType, String parentName) {
    return (
        // can't be a synthetic layer because it would be invalid
        repeatedType.isPrimitive() ||
        repeatedType.asGroupType().getFieldCount() > 1 ||
        repeatedType.asGroupType().getType(0).isRepetition(REPEATED) ||
        // known patterns without the synthetic layer
        repeatedType.getName().equals("array") ||
        repeatedType.getName().equals(parentName + "_tuple") ||
        // default assumption
        assumeRepeatedIsListElement
    );
  }

  private static Schema optional(Schema original) {
    // null is first in the union because Parquet's default is always null
    return Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.NULL),
        original));
  }
}
