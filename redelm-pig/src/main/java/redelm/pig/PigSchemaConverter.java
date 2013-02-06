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
package redelm.pig;

import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import redelm.schema.GroupType;
import redelm.schema.MessageType;
import redelm.schema.PrimitiveType;
import redelm.schema.PrimitiveType.Primitive;
import redelm.schema.Type;
import redelm.schema.Type.Repetition;

/**
 *
 * Converts a Pig Schema into a RedElm schema
 *
 * Bags are converted into an optional group containing one repeated group field to preserve distinction between empty bag and null.
 * Map are converted into an optional group containing one repeated group field of (key, value).
 * anonymous fields are named field_{index}. (in most cases pig already gives them an alias val_{int}, so this rarely happens)
 *
 * @author Julien Le Dem
 *
 */
public class PigSchemaConverter {

  /**
   *
   * @param pigSchema the pig schema
   * @return the resulting RedElm schema
   */
  public MessageType convert(Schema pigSchema) {
    return new MessageType("pig_schema", convertTypes(pigSchema));
  }

  private Type[] convertTypes(Schema pigSchema) {
    List<FieldSchema> fields = pigSchema.getFields();
    Type[] types = new Type[fields.size()];
    for (int i = 0; i < types.length; i++) {
      types[i] = convert(fields.get(i), i);
    }
    return types;
  }

  private Type convert(FieldSchema fieldSchema, String defaultAlias) {
    try {
      String name = name(fieldSchema.alias, defaultAlias);
      switch (fieldSchema.type) {
      case DataType.BAG:
          return convertBag(name, fieldSchema);
      case DataType.TUPLE:
          return convertTuple(name, fieldSchema, Repetition.OPTIONAL);
      case DataType.MAP:
          return convertMap(name, fieldSchema);
      case DataType.BOOLEAN:
          return primitive(name, Primitive.BOOLEAN);
      case DataType.CHARARRAY:
          return primitive(name, Primitive.BINARY);
      case DataType.INTEGER:
          return primitive(name, Primitive.INT32);
      case DataType.LONG:
          return primitive(name, Primitive.INT64);
      case DataType.FLOAT:
          return primitive(name, Primitive.FLOAT);
      case DataType.DOUBLE:
          return primitive(name, Primitive.DOUBLE);
      case DataType.DATETIME:
          throw new UnsupportedOperationException();
      case DataType.BYTEARRAY:
          return primitive(name, Primitive.BINARY);
      default:
          throw new RuntimeException("Unknown type "+fieldSchema.type+" "+DataType.findTypeName(fieldSchema.type));
      }
    } catch (FrontendException e) {
        throw new RuntimeException("can't convert "+fieldSchema, e);
    } catch (RuntimeException e) {
        throw new RuntimeException("can't convert "+fieldSchema, e);
    }
  }

  private Type convert(FieldSchema fieldSchema, int index) {
    return convert(fieldSchema, "field_"+index);
  }

  /**
   *
   * @param name
   * @param fieldSchema
   * @return an optional group containing one repeated group field
   * @throws FrontendException
   */
  private GroupType convertBag(String name, FieldSchema fieldSchema) throws FrontendException {
    FieldSchema innerField = fieldSchema.schema.getField(0);
    return listWrapper(
        name,
        convertTuple(name(innerField.alias, "bag"), innerField, Repetition.REPEATED));
  }

  private String name(String fieldAlias, String defaultName) {
    return fieldAlias == null ? defaultName : fieldAlias;
  }

  private PrimitiveType primitive(String name, Primitive primitive) {
    return new PrimitiveType(Repetition.OPTIONAL, primitive, name);
  }

  /**
   * to preserve the difference between empty list and null
   * @param alias
   * @param groupType
   * @return an optional group
   */
  private GroupType listWrapper(String alias, GroupType groupType) {
    return new GroupType(Repetition.OPTIONAL, alias, groupType);
  }

  /**
   *
   * @param alias
   * @param fieldSchema
   * @return an optional group containing one repeated group field (key, value)
   * @throws FrontendException
   */
  private GroupType convertMap(String alias, FieldSchema fieldSchema) throws FrontendException {
    Type[] types = new Type[2];
    types[0] = new PrimitiveType(Repetition.REQUIRED, Primitive.BINARY, "key");
    Schema innerSchema = fieldSchema.schema;
    if (innerSchema.size() != 1) {
      throw new FrontendException("Invalid map Schema, schema should contain exactly one field: " + fieldSchema);
    }
    FieldSchema innerField = innerSchema.getField(0);
    switch (innerField.type) {
    case DataType.TUPLE:
      types[1] = convertTuple("value", innerField, Repetition.OPTIONAL);
      break;
    case DataType.MAP:
      types[1] = convertMap("value", innerField);
      break;
    case DataType.BAG:
      types[1] = convertBag("value", innerField);
      break;
    case DataType.INTEGER:
    case DataType.LONG:
    case DataType.BOOLEAN:
    case DataType.FLOAT:
    case DataType.DOUBLE:
    case DataType.CHARARRAY:
      types[1] = convert(innerField, "value");
      break;
    default:
      throw new FrontendException("Invalid map Schema, field type not recognized: " + fieldSchema);
    }
    return listWrapper(alias, new GroupType(Repetition.REPEATED, name(innerField.alias, "map"), types));
  }

  private GroupType convertTuple(String alias, FieldSchema field, Repetition repetition) {
    return new GroupType(repetition, alias, convertTypes(field.schema));
  }
}
