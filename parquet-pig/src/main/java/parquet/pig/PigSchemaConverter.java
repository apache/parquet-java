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
package parquet.pig;

import static parquet.Log.DEBUG;
import static parquet.schema.OriginalType.LIST;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import parquet.Log;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;
import parquet.schema.Type.Repetition;


/**
 *
 * Converts a Pig Schema into a Parquet schema
 *
 * Bags are converted into an optional group containing one repeated group field to preserve distinction between empty bag and null.
 * Map are converted into an optional group containing one repeated group field of (key, value).
 * anonymous fields are named field_{index}. (in most cases pig already gives them an alias val_{int}, so this rarely happens)
 *
 * @author Julien Le Dem
 *
 */
public class PigSchemaConverter {
  private static final Log LOG = Log.getLog(PigSchemaConverter.class);

  /**
   *
   * @param pigSchema the pig schema
   * @return the resulting Parquet schema
   */
  public MessageType convert(Schema pigSchema) {
    return new MessageType("pig_schema", convertTypes(pigSchema));
  }

  public Schema filter(Schema pigSchemaToFilter, MessageType schemaSubset) {
    if (DEBUG) LOG.debug("filtering pig schema:\n" + pigSchemaToFilter + "\nwith:\n " + schemaSubset);
    Schema result = filterTupleSchema(pigSchemaToFilter, schemaSubset);
    if (DEBUG) LOG.debug("pig schema:\n" + pigSchemaToFilter + "\nfiltered to:\n" + result);
    return result;
  }

  /**
   *
   * @param pigSchema the pig schema
   * @param
   * @return the resulting Parquet schema
   */
  private Schema filterTupleSchema(Schema pigSchemaToFilter, GroupType schemaSubset) {
    List<FieldSchema> fields = pigSchemaToFilter.getFields();
    List<FieldSchema> newFields = new ArrayList<Schema.FieldSchema>();
    for (int i = 0; i < fields.size(); i++) {
      FieldSchema fieldSchema = fields.get(i);
      String name = name(fieldSchema.alias, "field_"+i);
      if (schemaSubset.containsField(name)) {
        Type type = schemaSubset.getType(name);
        newFields.add(filter(fieldSchema, type));
      }
    }
    return new Schema(newFields);
  }

  private FieldSchema filter(FieldSchema fieldSchema, Type type) {
    if (DEBUG) LOG.debug("filtering Field pig schema:\n" + fieldSchema + "\nwith:\n " + type);
    try {
      switch (fieldSchema.type) {
      case DataType.BAG:
        return filterBag(fieldSchema, type.asGroupType());
      case DataType.MAP:
        return filterMap(fieldSchema, type.asGroupType());
      case DataType.TUPLE:
        return filterTuple(fieldSchema, type.asGroupType());
      default:
        return fieldSchema;
      }
    } catch (FrontendException e) {
      throw new RuntimeException("can't filter " + fieldSchema, e);
    }
  }

  private FieldSchema filterTuple(FieldSchema fieldSchema, GroupType schemaSubset) throws FrontendException {
    if (DEBUG) LOG.debug("filtering TUPLE pig schema:\n" + fieldSchema + "\nwith:\n " + schemaSubset);
    return new FieldSchema(fieldSchema.alias, filterTupleSchema(fieldSchema.schema, schemaSubset), fieldSchema.type);
  }

  private FieldSchema filterMap(FieldSchema fieldSchema, GroupType type) throws FrontendException {
    if (DEBUG) LOG.debug("filtering MAP pig schema:\n" + fieldSchema + "\nwith:\n " + type);
    GroupType nested = unwrap(type);
    if (nested.getFieldCount() != 2) {
      throw new RuntimeException("this should be a Map Key/Value: " + type);
    }
    FieldSchema innerField = fieldSchema.schema.getField(0);
    return new FieldSchema(fieldSchema.alias, new Schema(filter(innerField, nested.getType(1))), fieldSchema.type);
  }

  private FieldSchema filterBag(FieldSchema fieldSchema, GroupType type) throws FrontendException {
    if (DEBUG) LOG.debug("filtering BAG pig schema:\n" + fieldSchema + "\nwith:\n " + type);
    GroupType nested = unwrap(type);
    FieldSchema innerField = fieldSchema.schema.getField(0);
    return new FieldSchema(fieldSchema.alias, new Schema(filterTuple(innerField, nested.asGroupType())), fieldSchema.type);
  }

  private GroupType unwrap(GroupType type) {
    if (type.getFieldCount() != 1) {
      throw new RuntimeException("not unwrapping the right type, this should be a Map or a Bag: " + type);
    }
    return type.getType(0).asGroupType();
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
        return primitive(name, PrimitiveTypeName.BOOLEAN);
      case DataType.CHARARRAY:
        return primitive(name, PrimitiveTypeName.BINARY, OriginalType.UTF8);
      case DataType.INTEGER:
        return primitive(name, PrimitiveTypeName.INT32);
      case DataType.LONG:
        return primitive(name, PrimitiveTypeName.INT64);
      case DataType.FLOAT:
        return primitive(name, PrimitiveTypeName.FLOAT);
      case DataType.DOUBLE:
        return primitive(name, PrimitiveTypeName.DOUBLE);
      case DataType.DATETIME:
        throw new UnsupportedOperationException();
      case DataType.BYTEARRAY:
        return primitive(name, PrimitiveTypeName.BINARY);
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
        LIST,
        convertTuple(name(innerField.alias, "bag"), innerField, Repetition.REPEATED));
  }

  private String name(String fieldAlias, String defaultName) {
    return fieldAlias == null ? defaultName : fieldAlias;
  }

  private Type primitive(String name, PrimitiveTypeName primitive, OriginalType originalType) {
    return new PrimitiveType(Repetition.OPTIONAL, primitive, name, originalType);
  }

  private PrimitiveType primitive(String name, PrimitiveTypeName primitive) {
    return new PrimitiveType(Repetition.OPTIONAL, primitive, name, null);
  }

  /**
   * to preserve the difference between empty list and null
   * @param alias
   * @param originalType
   * @param groupType
   * @return an optional group
   */
  private GroupType listWrapper(String alias, OriginalType originalType, GroupType groupType) {
    return new GroupType(Repetition.OPTIONAL, alias, originalType, groupType);
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
    types[0] = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "key");
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
    return listWrapper(alias, OriginalType.MAP, new GroupType(Repetition.REPEATED, name(innerField.alias, "map"), OriginalType.MAP_KEY_VALUE, types));
  }

  private GroupType convertTuple(String alias, FieldSchema field, Repetition repetition) {
    return new GroupType(repetition, alias, convertTypes(field.schema));
  }
}
