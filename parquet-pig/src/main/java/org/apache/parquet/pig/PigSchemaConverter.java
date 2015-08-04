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
package org.apache.parquet.pig;

import static org.apache.parquet.Log.DEBUG;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import org.apache.parquet.Log;
import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeNameConverter;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;


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
  static final String ARRAY_VALUE_NAME = "value";
  public static final String WRAP_BAG_PREFIX = "bag_";
  private ColumnAccess columnAccess;

  public PigSchemaConverter() {
    this(false);
  }

  /**
   *
   * @param columnIndexAccess toggle between name and index based access (default: false)
   */
  public PigSchemaConverter(boolean columnIndexAccess) {
    this.columnAccess = columnIndexAccess?new ColumnIndexAccess():new ColumnNameAccess();
  }

  /**
   * @param pigSchemaString the pig schema to parse
   * @return the parsed pig schema
   */
  public static Schema parsePigSchema(String pigSchemaString) {
    try {
      return pigSchemaString == null ? null : Utils.getSchemaFromString(pigSchemaString);
    } catch (ParserException e) {
      throw new SchemaConversionException("could not parse Pig schema: " + pigSchemaString, e);
    }
  }

  interface ColumnAccess {
    List<Type> filterTupleSchema(GroupType schemaToFilter, Schema pigSchema, RequiredFieldList requiredFieldsList);
  }

  class ColumnIndexAccess implements ColumnAccess {
    @Override
    public List<Type> filterTupleSchema(GroupType schemaToFilter, Schema pigSchema, RequiredFieldList requiredFieldsList) {
      List<Type> newFields = new ArrayList<Type>();
      List<Pair<FieldSchema,Integer>> indexedFields = new ArrayList<Pair<FieldSchema,Integer>>();

      try {
        if(requiredFieldsList == null) {
          int index = 0;
          for(FieldSchema fs : pigSchema.getFields()) {
            indexedFields.add(new Pair<FieldSchema, Integer>(fs, index++));
          }
        } else {
          for(RequiredField rf : requiredFieldsList.getFields()) {
            indexedFields.add(new Pair<FieldSchema, Integer>(pigSchema.getField(rf.getAlias()), rf.getIndex()));
          }
        }

        for (Pair<FieldSchema, Integer> p : indexedFields) {
          FieldSchema fieldSchema = pigSchema.getField(p.first.alias);
          if (p.second < schemaToFilter.getFieldCount()) {
            Type type = schemaToFilter.getFields().get(p.second);
            newFields.add(filter(type, fieldSchema));
          }
        }
      } catch (FrontendException e) {
          throw new RuntimeException("Failed to filter requested fields", e);
      }
      return newFields;
    }
  }

  class ColumnNameAccess implements ColumnAccess {
    @Override
    public List<Type> filterTupleSchema(GroupType schemaToFilter, Schema requestedPigSchema, RequiredFieldList requiredFieldsList) {
      List<FieldSchema> fields = requestedPigSchema.getFields();
      List<Type> newFields = new ArrayList<Type>();
      for (int i = 0; i < fields.size(); i++) {
        FieldSchema fieldSchema = fields.get(i);
        String name = name(fieldSchema.alias, "field_"+i);
        if (schemaToFilter.containsField(name)) {
          newFields.add(filter(schemaToFilter.getType(name), fieldSchema));
        } else if (name.startsWith(WRAP_BAG_PREFIX)) {
          String innerFieldName = wrappedName(name);
          if (schemaToFilter.containsField(innerFieldName)) {
              newFields.add(filterWrappedPrimitive(schemaToFilter.getType(innerFieldName)));
          }
        }
      }
      return newFields;
    }


  }



  /**
   * @param pigSchema the pig schema to turn into a string representation
   * @return the sctring representation of the schema
   */
  static String pigSchemaToString(Schema pigSchema) {
    final String pigSchemaString = pigSchema.toString();
    return pigSchemaString.substring(1, pigSchemaString.length() - 1);
  }

  /**
   * Check if the name is the name of the bag which wrap a repeated primitive
   * @param name
   * @return true if it's a wrapped type (encapsulate in a tuple in a bag), false otherwise
   */
  public static boolean isWrappedType(String name) {
    if (name.startsWith(WRAP_BAG_PREFIX)) {
      return true;
    }
    return false;
  }

  /**
   * Check if the name is the name of the bag which wrap a repeated primitive
   * @param name
   * @return the inner field name or the same name as input if not wrapped by a bag
   */
  public static String wrappedName(String name) {
    String resultName = name;
    if (isWrappedType(name)){
      resultName = name.substring(PigSchemaConverter.WRAP_BAG_PREFIX.length(),name.length());
    }
    return resultName;
  }

  public static RequiredFieldList deserializeRequiredFieldList(String requiredFieldString) {
    if(requiredFieldString == null) {
        return null;
    }

    try {
      return (RequiredFieldList) ObjectSerializer.deserialize(requiredFieldString);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize pushProjection", e);
    }
  }

  static String serializeRequiredFieldList(RequiredFieldList requiredFieldList) {
    try {
      return ObjectSerializer.serialize(requiredFieldList);
    } catch (IOException e) {
      throw new RuntimeException("Failed to searlize required fields.", e);
    }
  }

  /**
   * converts a parquet schema into a pig schema
   * @param parquetSchema the parquet schema to convert to Pig schema
   * @return the resulting schema
   */
  public Schema convert(MessageType parquetSchema) {
    return convertFields(parquetSchema.getFields());
  }

  /**
   * @param parquetType the type to convert
   * @return the resulting schema (containing one field)
   */
  public Schema convertField(Type parquetType) {
    return convertFields(Arrays.asList(parquetType));
  }

  private Schema convertFields(List<Type> parquetFields) {
    List<FieldSchema> fields = new ArrayList<Schema.FieldSchema>();
    for (Type parquetType : parquetFields) {
      try{
        FieldSchema innerfieldSchema = getFieldSchema(parquetType);
        if (parquetType.isRepetition(Repetition.REPEATED)) {
            Schema bagSchema;
            if (parquetType.isPrimitive()) {
                bagSchema = new Schema(new FieldSchema(null,new Schema(innerfieldSchema),DataType.TUPLE));
            } else {
                bagSchema = new Schema(Arrays.asList(innerfieldSchema));
            }
          fields.add(new FieldSchema(WRAP_BAG_PREFIX + parquetType.getName(), bagSchema, DataType.BAG));
        } else {
          fields.add(innerfieldSchema);
        }
      }
      catch (FrontendException fe) {
        throw new SchemaConversionException("can't convert "+ parquetType, fe);
      }
    }
    return new Schema(fields);
  }

  private FieldSchema getSimpleFieldSchema(final String fieldName, Type parquetType)
      throws FrontendException {
    final PrimitiveTypeName parquetPrimitiveTypeName =
        parquetType.asPrimitiveType().getPrimitiveTypeName();
    final OriginalType originalType = parquetType.getOriginalType();
    return parquetPrimitiveTypeName.convert(
        new PrimitiveTypeNameConverter<Schema.FieldSchema, FrontendException>() {
      @Override
      public FieldSchema convertFLOAT(PrimitiveTypeName primitiveTypeName)
          throws FrontendException {
        return new FieldSchema(fieldName, null, DataType.FLOAT);
      }

      @Override
      public FieldSchema convertDOUBLE(PrimitiveTypeName primitiveTypeName)
          throws FrontendException {
        return new FieldSchema(fieldName, null, DataType.DOUBLE);
      }

      @Override
      public FieldSchema convertINT32(PrimitiveTypeName primitiveTypeName)
          throws FrontendException {
        return new FieldSchema(fieldName, null, DataType.INTEGER);
      }

      @Override
      public FieldSchema convertINT64(PrimitiveTypeName primitiveTypeName)
          throws FrontendException {
        return new FieldSchema(fieldName, null, DataType.LONG);
      }

      @Override
      public FieldSchema convertINT96(PrimitiveTypeName primitiveTypeName)
          throws FrontendException {
        throw new FrontendException("NYI");
      }

      @Override
      public FieldSchema convertFIXED_LEN_BYTE_ARRAY(
          PrimitiveTypeName primitiveTypeName) throws FrontendException {
        return new FieldSchema(fieldName, null, DataType.BYTEARRAY);
      }

      @Override
      public FieldSchema convertBOOLEAN(PrimitiveTypeName primitiveTypeName)
          throws FrontendException {
        return new FieldSchema(fieldName, null, DataType.BOOLEAN);
      }

      @Override
      public FieldSchema convertBINARY(PrimitiveTypeName primitiveTypeName)
          throws FrontendException {
        if (originalType != null && originalType == OriginalType.UTF8) {
          return new FieldSchema(fieldName, null, DataType.CHARARRAY);
        } else {
          return new FieldSchema(fieldName, null, DataType.BYTEARRAY);
        }
      }
    });
  }

  private FieldSchema getComplexFieldSchema(String fieldName, Type parquetType)
      throws FrontendException {
    GroupType parquetGroupType = parquetType.asGroupType();
    OriginalType originalType = parquetGroupType.getOriginalType();
    if (originalType !=  null) {
      switch(originalType) {
      case MAP:
        // verify that its a map
        if (parquetGroupType.getFieldCount() != 1 || parquetGroupType.getType(0).isPrimitive()) {
          throw new SchemaConversionException("Invalid map type " + parquetGroupType);
        }
        GroupType mapKeyValType = parquetGroupType.getType(0).asGroupType();
        if (!mapKeyValType.isRepetition(Repetition.REPEATED) ||
            !mapKeyValType.getOriginalType().equals(OriginalType.MAP_KEY_VALUE) ||
            mapKeyValType.getFieldCount()!=2) {
          throw new SchemaConversionException("Invalid map type " + parquetGroupType);
        }
        // if value is not primitive wrap it in a tuple
        Type valueType = mapKeyValType.getType(1);
        Schema s = convertField(valueType);
        s.getField(0).alias = null;
        return new FieldSchema(fieldName, s, DataType.MAP);
      case LIST:
        Type type = parquetGroupType.getType(0);
        if (parquetGroupType.getFieldCount()!= 1 || type.isPrimitive()) {
          // an array is effectively a bag
          Schema primitiveSchema = new Schema(getSimpleFieldSchema(parquetGroupType.getFieldName(0), type));
          Schema tupleSchema = new Schema(new FieldSchema(ARRAY_VALUE_NAME, primitiveSchema, DataType.TUPLE));
          return new FieldSchema(fieldName, tupleSchema, DataType.BAG);
        }
        GroupType tupleType = parquetGroupType.getType(0).asGroupType();
        if (!tupleType.isRepetition(Repetition.REPEATED)) {
          throw new SchemaConversionException("Invalid list type " + parquetGroupType);
        }
        Schema tupleSchema = new Schema(new FieldSchema(tupleType.getName(), convertFields(tupleType.getFields()), DataType.TUPLE));
        return new FieldSchema(fieldName, tupleSchema, DataType.BAG);
      case MAP_KEY_VALUE:
      case ENUM:
      case UTF8:
      default:
        throw new SchemaConversionException("Unexpected original type for " + parquetType + ": " + originalType);
      }
    } else {
      // if original type is not set, we assume it to be tuple
      return new FieldSchema(fieldName, convertFields(parquetGroupType.getFields()), DataType.TUPLE);
    }
  }

  private FieldSchema getFieldSchema(Type parquetType) throws FrontendException {
    final String fieldName = parquetType.getName();
    if (parquetType.isPrimitive()) {
      return getSimpleFieldSchema(fieldName, parquetType);
    } else {
      return getComplexFieldSchema(fieldName, parquetType);
    }
  }

  /**
   *
   * @param pigSchema the pig schema
   * @return the resulting Parquet schema
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
    String name = name(fieldSchema.alias, defaultAlias);
    return convertWithName(fieldSchema, name);
  }

  private Type convertWithName(FieldSchema fieldSchema, String name, Repetition repetition) {
    try {
      switch (fieldSchema.type) {
        case DataType.BAG:
          if (name.startsWith(WRAP_BAG_PREFIX)) {
            return convertWrappedPrimitive(wrappedName(name), fieldSchema);
          }
          return convertBag(name, fieldSchema);
        case DataType.TUPLE:
          return convertTuple(name, fieldSchema, Repetition.OPTIONAL);
        case DataType.MAP:
          return convertMap(name, fieldSchema);
        case DataType.BOOLEAN:
          return primitive(name, PrimitiveTypeName.BOOLEAN, null, repetition);
        case DataType.CHARARRAY:
          return primitive(name, PrimitiveTypeName.BINARY, OriginalType.UTF8, repetition);
        case DataType.INTEGER:
          return primitive(name, PrimitiveTypeName.INT32, null, repetition);
        case DataType.LONG:
          return primitive(name, PrimitiveTypeName.INT64, null, repetition);
        case DataType.FLOAT:
          return primitive(name, PrimitiveTypeName.FLOAT, null, repetition);
        case DataType.DOUBLE:
          return primitive(name, PrimitiveTypeName.DOUBLE, null, repetition);
        case DataType.DATETIME:
          throw new UnsupportedOperationException();
        case DataType.BYTEARRAY:
          return primitive(name, PrimitiveTypeName.BINARY, null, repetition);
        default:
          throw new SchemaConversionException("Unknown type " + fieldSchema.type + " " + DataType.findTypeName(fieldSchema.type));
      }
    } catch (FrontendException e) {
      throw new SchemaConversionException("can't convert "+fieldSchema, e);
    }
  }

  private Type convertWithName(FieldSchema fieldSchema, String name) {
    return convertWithName(fieldSchema, name, Repetition.OPTIONAL);
  }

  private Type convert(FieldSchema fieldSchema, int index) {
    return convert(fieldSchema, "field_"+index);
  }

  private Type convertWrappedPrimitive(String wrappedName, FieldSchema fieldSchema) throws FrontendException {
    FieldSchema innerTuple = fieldSchema.schema.getField(0);
    FieldSchema innerType = innerTuple.schema.getField(0);
    return convertWithName(innerType, wrappedName, Repetition.REPEATED);
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
    return ConversionPatterns.listType(
        Repetition.OPTIONAL,
        name,
        convertTuple(name(innerField.alias, "bag"), innerField, Repetition.REPEATED));
  }

  private String name(String fieldAlias, String defaultName) {
    return fieldAlias == null ? defaultName : fieldAlias;
  }


  private Type primitive(String name, PrimitiveTypeName primitive, OriginalType originalType, Repetition repetition) {
    return new PrimitiveType(repetition, primitive, name, originalType);
  }

  private Type primitive(String name, PrimitiveTypeName primitive, OriginalType originalType) {
    return new PrimitiveType(Repetition.OPTIONAL, primitive, name, originalType);
  }

  private PrimitiveType primitive(String name, PrimitiveTypeName primitive) {
    return new PrimitiveType(Repetition.OPTIONAL, primitive, name, null);
  }

  /**
   *
   * @param alias
   * @param fieldSchema
   * @return an optional group containing one repeated group field (key, value)
   * @throws FrontendException
   */
  private GroupType convertMap(String alias, FieldSchema fieldSchema) {
    Schema innerSchema = fieldSchema.schema;
    if (innerSchema == null || innerSchema.size() != 1) {
      throw new SchemaConversionException("Invalid map Schema, schema should contain exactly one field: " + fieldSchema);
    }
    FieldSchema innerField = null;
    try {
      innerField = innerSchema.getField(0);
    } catch (FrontendException fe) {
      throw new SchemaConversionException("Invalid map schema, cannot infer innerschema: ", fe);
    }
    Type convertedValue = convertWithName(innerField, "value");
    return ConversionPatterns.stringKeyMapType(Repetition.OPTIONAL, alias, name(innerField.alias, "map"),
        convertedValue);
  }

  private GroupType convertTuple(String alias, FieldSchema field, Repetition repetition) {
    return new GroupType(repetition, alias, convertTypes(field.schema));
  }

  /**
   * filters a Parquet schema based on a pig schema for projection
   * @param schemaToFilter the schema to be filter
   * @param requestedPigSchema the pig schema to filter it with
   * @return the resulting filtered schema
   */
  public MessageType filter(MessageType schemaToFilter, Schema requestedPigSchema) {
    return filter(schemaToFilter, requestedPigSchema, null);
  }

  /**
   * filters a Parquet schema based on a pig schema for projection
   * @param schemaToFilter the schema to be filter
   * @param requestedPigSchema the pig schema to filter it with
   * @param requiredFieldList projected required fields
   * @return the resulting filtered schema
   */
  public MessageType filter(MessageType schemaToFilter, Schema requestedPigSchema, RequiredFieldList requiredFieldList) {
    try {
      if (DEBUG) LOG.debug("filtering schema:\n" + schemaToFilter + "\nwith requested pig schema:\n " + requestedPigSchema);
      List<Type> result = columnAccess.filterTupleSchema(schemaToFilter, requestedPigSchema, requiredFieldList);
      if (DEBUG) LOG.debug("schema:\n" + schemaToFilter + "\nfiltered to:\n" + result);
      return new MessageType(schemaToFilter.getName(), result);
    } catch (RuntimeException e) {
      throw new RuntimeException("can't filter " + schemaToFilter + " with " + requestedPigSchema, e);
    }
  }

  private Type filter(Type type, FieldSchema fieldSchema) {
    if (DEBUG) LOG.debug("filtering type:\n" + type + "\nwith:\n " + fieldSchema);
    try {
      switch (fieldSchema.type) {
      case DataType.BAG:
        return filterBag(type.asGroupType(), fieldSchema);
      case DataType.MAP:
        return filterMap(type.asGroupType(), fieldSchema);
      case DataType.TUPLE:
        return filterTuple(type.asGroupType(), fieldSchema);
      default:
        return type;
      }
    } catch (FrontendException e) {
      throw new SchemaConversionException("can't filter " + type + " with " + fieldSchema, e);
    } catch (RuntimeException e) {
      throw new RuntimeException("can't filter " + type + " with " + fieldSchema, e);
    }
  }

  private Type filterTuple(GroupType tupleType, FieldSchema tupleFieldSchema) throws FrontendException {
    if (DEBUG) LOG.debug("filtering TUPLE schema:\n" + tupleType + "\nwith:\n " + tupleFieldSchema);
    return tupleType.withNewFields(columnAccess.filterTupleSchema(tupleType, tupleFieldSchema.schema, null));
  }

  private Type filterMap(GroupType mapType, FieldSchema mapFieldSchema) throws FrontendException {
    if (DEBUG) LOG.debug("filtering MAP schema:\n" + mapType + "\nwith:\n " + mapFieldSchema);
    if (mapType.getFieldCount() != 1) {
      throw new RuntimeException("not unwrapping the right type, this should be a Map: " + mapType);
    }
    GroupType nested = mapType.getType(0).asGroupType();
    if (nested.getFieldCount() != 2) {
      throw new RuntimeException("this should be a Map Key/Value: " + mapType);
    }
    FieldSchema innerField = mapFieldSchema.schema.getField(0);
    return mapType.withNewFields(nested.withNewFields(nested.getType(0), filter(nested.getType(1), innerField)));
  }

  private Type filterBag(GroupType bagType, FieldSchema bagFieldSchema) throws FrontendException {
    if (DEBUG) LOG.debug("filtering BAG schema:\n" + bagType + "\nwith:\n " + bagFieldSchema);
    if (bagType.getFieldCount() != 1) {
      throw new RuntimeException("not unwrapping the right type, this should be a Bag: " + bagType);
    }
    Type nested = bagType.getType(0);
    FieldSchema innerField = bagFieldSchema.schema.getField(0);
    if (nested.isPrimitive() || nested.getOriginalType() == OriginalType.MAP || nested.getOriginalType() == OriginalType.LIST) {
      // Bags always contain tuples => we skip the extra tuple that was inserted in that case.
      innerField = innerField.schema.getField(0);
    }
    return bagType.withNewFields(filter(nested, innerField));
  }

  /**
   * Filter wrapped primitives (repeated primitives convert to bag with a tuple for pig)
   * @param innerType the inner type
   * @return
   */
  private Type filterWrappedPrimitive(Type innerType) {
    if (DEBUG) LOG.debug("filtering wrapped schema:\n" + innerType);
    if (innerType.getRepetition() != Repetition.REPEATED) {
      throw new RuntimeException("inner type should be a repeated primitive: " + innerType);
    }
    return innerType;
  }
}
