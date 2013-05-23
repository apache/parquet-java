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
package parquet.avro;

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import static parquet.schema.OriginalType.*;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.*;

/**
 * <p>
 * Converts an Avro schema into a Parquet schema. See package documentation for details
 * of the mapping.
 * </p>
 */
public class AvroSchemaConverter {

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
      return new GroupType(repetition, fieldName, LIST,
          convertField("array", schema.getElementType(), Type.Repetition.REPEATED));
    } else if (type.equals(Schema.Type.MAP)) {
      Type keyType = convertField("key", Schema.create(Schema.Type.STRING));
      Type valType = convertField("value", schema.getValueType());
      return new GroupType(repetition, fieldName, MAP,
          new GroupType(Type.Repetition.REPEATED, "map", MAP_KEY_VALUE, keyType, valType));
    } else if (type.equals(Schema.Type.FIXED)) {
      return primitive(fieldName, FIXED_LEN_BYTE_ARRAY, repetition);
    } else if (type.equals(Schema.Type.UNION)) {
      List<Schema> schemas = schema.getTypes();
      if (schemas.size() == 2) {
        if (schemas.get(0).getType().equals(Schema.Type.NULL)) {
          return convertField(fieldName, schemas.get(1), Type.Repetition.OPTIONAL);
        } else if (schemas.get(1).getType().equals(Schema.Type.NULL)) {
          return convertField(fieldName, schemas.get(0), Type.Repetition.OPTIONAL);
        }
      }
      throw new UnsupportedOperationException("Cannot convert Avro type " + type);
    }
    throw new UnsupportedOperationException("Cannot convert Avro type " + type);
  }

  private Type convertField(Schema.Field field) {
    return convertField(field.name(), field.schema());
  }

  private Type primitive(String name, PrimitiveType.PrimitiveTypeName primitive,
      Type.Repetition repetition, OriginalType originalType) {
    return new PrimitiveType(repetition, primitive, name, originalType);
  }

  private PrimitiveType primitive(String name, PrimitiveType.PrimitiveTypeName
      primitive, Type.Repetition repetition) {
    return new PrimitiveType(repetition, primitive, name, null);
  }

}