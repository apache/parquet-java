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
package parquet.avro;

import java.util.*;

import org.apache.avro.Schema;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.node.NullNode;
import parquet.schema.ConversionPatterns;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

import static parquet.schema.OriginalType.*;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.*;

/**
 * <p>
 * Converts an Avro schema into a Parquet schema. See package documentation for details
 * of the mapping.
 * </p>
 */
public class AvroSchemaConverter {

  static final String ADD_LIST_ELEMENT_RECORDS =
      "parquet.avro.add-list-element-records";
  private static final boolean ADD_LIST_ELEMENT_RECORDS_DEFAULT = true;

  private final boolean assumeRepeatedIsListElement;

  public AvroSchemaConverter() {
    this.assumeRepeatedIsListElement = ADD_LIST_ELEMENT_RECORDS_DEFAULT;
  }

  public AvroSchemaConverter(Configuration conf) {
    this.assumeRepeatedIsListElement = conf.getBoolean(
        ADD_LIST_ELEMENT_RECORDS, ADD_LIST_ELEMENT_RECORDS_DEFAULT);
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

  private Type convertField(String fieldName, Schema schema, Type.Repetition repetition) {
    Schema.Type type = schema.getType();
    if (type.equals(Schema.Type.BOOLEAN)) {
      return primitive(fieldName, BOOLEAN, repetition);
    } else if (type.equals(Schema.Type.INT)) {
      return primitive(fieldName, INT32, repetition);
    } else if (type.equals(Schema.Type.LONG)) {
      return primitive(fieldName, INT64, repetition);
    } else if (type.equals(Schema.Type.FLOAT)) {
      return primitive(fieldName, FLOAT, repetition);
    } else if (type.equals(Schema.Type.DOUBLE)) {
      return primitive(fieldName, DOUBLE, repetition);
    } else if (type.equals(Schema.Type.BYTES)) {
      return primitive(fieldName, BINARY, repetition);
    } else if (type.equals(Schema.Type.STRING)) {
      return primitive(fieldName, BINARY, repetition, UTF8);
    } else if (type.equals(Schema.Type.RECORD)) {
      return new GroupType(repetition, fieldName, convertFields(schema.getFields()));
    } else if (type.equals(Schema.Type.ENUM)) {
      return primitive(fieldName, BINARY, repetition, ENUM);
    } else if (type.equals(Schema.Type.ARRAY)) {
      return ConversionPatterns.listType(repetition, fieldName,
          convertField("array", schema.getElementType(), Type.Repetition.REPEATED));
    } else if (type.equals(Schema.Type.MAP)) {
      Type valType = convertField("value", schema.getValueType());
      // avro map key type is always string
      return ConversionPatterns.stringKeyMapType(repetition, fieldName, valType);
    } else if (type.equals(Schema.Type.FIXED)) {
      return primitive(fieldName, FIXED_LEN_BYTE_ARRAY, repetition,
                       schema.getFixedSize(), null);
    } else if (type.equals(Schema.Type.UNION)) {
      return convertUnion(fieldName, schema, repetition);
    }
    throw new UnsupportedOperationException("Cannot convert Avro type " + type);
  }

  private Type convertUnion(String fieldName, Schema schema, Type.Repetition repetition) {
    List<Schema> nonNullSchemas = new ArrayList(schema.getTypes().size());
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
        List<Type> unionTypes = new ArrayList(nonNullSchemas.size());
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

  private PrimitiveType primitive(String name, 
      PrimitiveType.PrimitiveTypeName primitive, Type.Repetition repetition,
      int typeLength, OriginalType originalType) {
    return new PrimitiveType(repetition, primitive, typeLength, name,
                             originalType);
  }

  private PrimitiveType primitive(String name,
      PrimitiveType.PrimitiveTypeName primitive, Type.Repetition repetition, 
      OriginalType originalType) {
    return new PrimitiveType(repetition, primitive, name, originalType);
  }

  private PrimitiveType primitive(String name, 
      PrimitiveType.PrimitiveTypeName primitive, Type.Repetition repetition) {
    return new PrimitiveType(repetition, primitive, name, null);
  }

  public Schema convert(MessageType parquetSchema) {
    return convertFields(parquetSchema.getName(), parquetSchema.getFields());
  }

  private Schema convertFields(String name, List<Type> parquetFields) {
    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    for (Type parquetType : parquetFields) {
      Schema fieldSchema = convertField(parquetType);
      if (parquetType.isRepetition(Type.Repetition.REPEATED)) {
        throw new UnsupportedOperationException("REPEATED not supported outside LIST or MAP. Type: " + parquetType);
      } else if (parquetType.isRepetition(Type.Repetition.OPTIONAL)) {
        fields.add(new Schema.Field(parquetType.getName(), optional(fieldSchema), null,
            NullNode.getInstance()));
      } else { // REQUIRED
        fields.add(new Schema.Field(parquetType.getName(), fieldSchema, null, null));
      }
    }
    Schema schema = Schema.createRecord(name, null, null, false);
    schema.setFields(fields);
    return schema;
  }

  private Schema convertField(final Type parquetType) {
    if (parquetType.isPrimitive()) {
      final PrimitiveTypeName parquetPrimitiveTypeName =
          parquetType.asPrimitiveType().getPrimitiveTypeName();
      final OriginalType originalType = parquetType.getOriginalType();
      return parquetPrimitiveTypeName.convert(
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
              throw new IllegalArgumentException("INT64 not yet implemented.");
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
              if (originalType == OriginalType.UTF8 || originalType == OriginalType.ENUM) {
                return Schema.create(Schema.Type.STRING);
              } else {
                return Schema.create(Schema.Type.BYTES);
              }
            }
          });
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
            if (!repeatedType.isRepetition(Type.Repetition.REPEATED)) {
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
            if (!mapKeyValType.isRepetition(Type.Repetition.REPEATED) ||
                !mapKeyValType.getOriginalType().equals(OriginalType.MAP_KEY_VALUE) ||
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
