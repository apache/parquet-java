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
package parquet.pig;

import static parquet.pig.PigSchemaConverter.parsePigSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.ObjectSerializer;
import parquet.Log;
import parquet.hadoop.api.InitContext;
import parquet.hadoop.api.ReadSupport;
import parquet.io.ParquetDecodingException;
import parquet.io.api.RecordMaterializer;
import parquet.pig.convert.TupleRecordMaterializer;
import parquet.schema.IncompatibleSchemaModificationException;
import parquet.schema.MessageType;

/**
 * Read support for Pig Tuple
 * a Pig MetaDataBlock is expected in the initialization call
 *
 * @author Julien Le Dem
 *
 */
public class TupleReadSupport extends ReadSupport<Tuple> {
  static final String PARQUET_PIG_SCHEMA = "parquet.pig.schema";
  static final String PARQUET_COLUMN_INDEX_ACCESS = "parquet.private.pig.column.index.access";
  static final String PARQUET_PIG_REQUIRED_FIELDS = "parquet.private.pig.required.fields";
  static final String PARQUET_PIG_ELEPHANT_BIRD_COMPATIBLE = "parquet.pig.elephantbird.compatible";
  private static final Log LOG = Log.getLog(TupleReadSupport.class);

  private static final PigSchemaConverter pigSchemaConverter = new PigSchemaConverter(false);

  /**
   * @param configuration the configuration for the current job
   * @return the pig schema requested by the user or null if none.
   */
  static Schema getPigSchema(Configuration configuration) {
    return parsePigSchema(configuration.get(PARQUET_PIG_SCHEMA));
  }

  /**
   * @param configuration configuration for the current job
   * @return List of required fields from pushProjection
   */
  static RequiredFieldList getRequiredFields(Configuration configuration) {
    String requiredFieldString = configuration.get(PARQUET_PIG_REQUIRED_FIELDS);

    if(requiredFieldString == null) {
      return null;
    }

    try {
      return (RequiredFieldList) ObjectSerializer.deserialize(requiredFieldString);
    } catch (IOException iOException) {
      throw new RuntimeException("Failed to deserialize pushProjection");
    }
  }

  /**
   * @param fileSchema the parquet schema from the file
   * @param keyValueMetaData the extra meta data from the files
   * @return the pig schema according to the file
   */
  static Schema getPigSchemaFromMultipleFiles(MessageType fileSchema, Map<String, Set<String>> keyValueMetaData) {
    Set<String> pigSchemas = PigMetaData.getPigSchemas(keyValueMetaData);
    if (pigSchemas == null) {
      return pigSchemaConverter.convert(fileSchema);
    }
    Schema mergedPigSchema = null;
    for (String pigSchemaString : pigSchemas) {
      try {
        mergedPigSchema = union(mergedPigSchema, parsePigSchema(pigSchemaString));
      } catch (FrontendException e) {
        throw new ParquetDecodingException("can not merge " + pigSchemaString + " into " + mergedPigSchema, e);
      }
    }
    return mergedPigSchema;
  }

  /**
   * @param fileSchema the parquet schema from the file
   * @param keyValueMetaData the extra meta data from the file
   * @return the pig schema according to the file
   */
  static Schema getPigSchemaFromFile(MessageType fileSchema, Map<String, String> keyValueMetaData) {
    PigMetaData pigMetaData = PigMetaData.fromMetaData(keyValueMetaData);
    if (pigMetaData == null) {
      return pigSchemaConverter.convert(fileSchema);
    }
    return parsePigSchema(pigMetaData.getPigSchema());
  }

  private static Schema union(Schema merged, Schema pigSchema) throws FrontendException {
    List<FieldSchema> fields = new ArrayList<Schema.FieldSchema>();
    if (merged == null) {
      return pigSchema;
    }
    // merging existing fields
    for (FieldSchema fieldSchema : merged.getFields()) {
      FieldSchema newFieldSchema = pigSchema.getField(fieldSchema.alias);
      if (newFieldSchema == null) {
        fields.add(fieldSchema);
      } else {
        fields.add(union(fieldSchema, newFieldSchema));
      }
    }
    // adding new fields
    for (FieldSchema newFieldSchema : pigSchema.getFields()) {
      FieldSchema oldFieldSchema = merged.getField(newFieldSchema.alias);
      if (oldFieldSchema == null) {
        fields.add(newFieldSchema);
      }
    }
    return new Schema(fields);
  }

  private static FieldSchema union(FieldSchema mergedFieldSchema, FieldSchema newFieldSchema) {
    if (!mergedFieldSchema.alias.equals(newFieldSchema.alias)
        || mergedFieldSchema.type != newFieldSchema.type) {
      throw new IncompatibleSchemaModificationException("Incompatible Pig schema change: " + mergedFieldSchema + " can not accept");
    }
    try {
      return new FieldSchema(mergedFieldSchema.alias, union(mergedFieldSchema.schema, newFieldSchema.schema), mergedFieldSchema.type);
    } catch (FrontendException e) {
      throw new SchemaConversionException(e);
    }
  }

  @Override
  public ReadContext init(InitContext initContext) {
    Schema pigSchema = getPigSchema(initContext.getConfiguration());
    RequiredFieldList requiredFields = getRequiredFields(initContext.getConfiguration());
    boolean columnIndexAccess = initContext.getConfiguration().getBoolean(PARQUET_COLUMN_INDEX_ACCESS, false);

    if (pigSchema == null) {
      return new ReadContext(initContext.getFileSchema());
    } else {

      // project the file schema according to the requested Pig schema
      MessageType parquetRequestedSchema = new PigSchemaConverter(columnIndexAccess).filter(initContext.getFileSchema(), pigSchema, requiredFields);
      return new ReadContext(parquetRequestedSchema);
    }
  }

  @Override
  public RecordMaterializer<Tuple> prepareForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext) {
    MessageType requestedSchema = readContext.getRequestedSchema();
    Schema requestedPigSchema = getPigSchema(configuration);

    if (requestedPigSchema == null) {
      throw new ParquetDecodingException("Missing Pig schema: ParquetLoader sets the schema in the job conf");
    }
    boolean elephantBirdCompatible = configuration.getBoolean(PARQUET_PIG_ELEPHANT_BIRD_COMPATIBLE, false);
    boolean columnIndexAccess = configuration.getBoolean(PARQUET_COLUMN_INDEX_ACCESS, false);
    if (elephantBirdCompatible) {
      LOG.info("Numbers will default to 0 instead of NULL; Boolean will be converted to Int");
    }
    return new TupleRecordMaterializer(requestedSchema, requestedPigSchema, elephantBirdCompatible, columnIndexAccess);
  }

}
