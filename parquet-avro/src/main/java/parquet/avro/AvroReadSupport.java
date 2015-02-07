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

import java.util.Iterator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import parquet.hadoop.api.InitContext;
import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

import static org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility;

/**
 * Avro implementation of {@link ReadSupport} for Avro {@link IndexedRecord}s which cover both Avro Specific and
 * Generic. Users should use {@link AvroParquetReader} or {@link AvroParquetInputFormat} rather than using
 * this class directly.
 */
public class AvroReadSupport<T extends IndexedRecord> extends ReadSupport<T> {

  public static String AVRO_REQUESTED_PROJECTION = "parquet.avro.projection";

  static final String AVRO_READ_SCHEMA = "parquet.avro.read.schema";

  static final String AVRO_SCHEMA_COMPATIBILITY_CHECK = "parquet.avro.schema.compatibility";

  static final String AVRO_SCHEMA_METADATA_KEY = "avro.schema";

  static final String AVRO_READ_SCHEMA_METADATA_KEY = "avro.read.schema";

  public static String AVRO_DATA_SUPPLIER = "parquet.avro.data.supplier";

  /**
   * @see parquet.avro.AvroParquetInputFormat#setRequestedProjection(org.apache.hadoop.mapreduce.Job, org.apache.avro.Schema)
   */
  public static void setRequestedProjection(Configuration configuration, Schema requestedProjection) {
    configuration.set(AVRO_REQUESTED_PROJECTION, requestedProjection.toString());
  }

  /**
   * @see parquet.avro.AvroParquetInputFormat#setAvroReadSchema(org.apache.hadoop.mapreduce.Job, org.apache.avro.Schema)
   */
  public static void setAvroReadSchema(Configuration configuration, Schema avroReadSchema) {
    configuration.set(AVRO_READ_SCHEMA, avroReadSchema.toString());
  }

  public static void setAvroDataSupplier(Configuration configuration,
      Class<? extends AvroDataSupplier> clazz) {
    configuration.set(AVRO_DATA_SUPPLIER, clazz.toString());
  }

  /**
   * Enable the resolution of Avro schemas in the metadata against the {@link #AVRO_READ_SCHEMA}.
   */
  public static void enableAvroSchemaCompatibilityCheck(Configuration configuration) {
    configuration.setBoolean(AVRO_SCHEMA_COMPATIBILITY_CHECK, true);
  }

  /**
   * Disable the resolution of Avro schemas in the metadata against the {@link #AVRO_READ_SCHEMA}.
   * Avro schema compatibility checks are disabled by default.
   */
  public static void disableAvroSchemaCompatibilityCheck(Configuration configuration) {
    configuration.setBoolean(AVRO_SCHEMA_COMPATIBILITY_CHECK, false);
  }

  /**
   * Are the two given Avro schemas compatible according to Avro schema resolution guidelines?
   * @param readerSchema the "destination" or updated schema
   * @param writerSchema the "source" or original schema
   */
  static boolean isSchemaCompatible(Schema readerSchema, Schema writerSchema) {
    SchemaCompatibilityType result = checkReaderWriterCompatibility(readerSchema, writerSchema).getType();
    return result == SchemaCompatibilityType.COMPATIBLE;
  }

  /**
   * Are all given Avro schemas compatible with a single updated schema, according to Avro schema resolution guidelines?
   * @param readerSchema the "destination" or updated schema
   * @param writerSchemas a set of "source" or original schemas
   */
  static boolean areSchemasCompatible(Schema readerSchema, Set<Schema> writerSchemas) {
    boolean acc = true;
    Iterator<Schema> schemaIterator = writerSchemas.iterator();
    while (acc && schemaIterator.hasNext()) {
      Schema writerSchema = schemaIterator.next();
      acc = acc && isSchemaCompatible(readerSchema, writerSchema);
    }
    return acc;
  }

  /**
   * Attempt to merge the Avro schemas listed under the {@code AVRO_SCHEMA_METADATA_KEY} by checking
   * if each schema is compatible with the requested "reader" schema.
   *
   * <p>Avro schemas in the metadata may differ, but they may be compatible in terms of Avro schema evolution.</p>
   * <p>For other keys in the metadata, follow the conventions of {@link InitContext#getMergedKeyValueMetaData}.</p>
   *
   * @param context init context that contains unmerged key-value metadata
   * @param readerSchema the reader Avro schema that will be used for validating schema compatibility
   * @return the merged key-value metadata
   * @see AvroReadSupport
   * @see InitContext
   */
  static Map<String, String> mergeKeyValueMetadata(InitContext context, Schema readerSchema) {
    Map<String, Set<String>> unmergedKeyValueMetadata = context.getKeyValueMetadata();
    Map<String, String> mergedKeyValueMetadata = new HashMap<String, String>();
    Iterator<Map.Entry<String, Set<String>>> entryIterator = unmergedKeyValueMetadata.entrySet().iterator();

    while (entryIterator.hasNext()) {
      Map.Entry<String, Set<String>> entry = entryIterator.next();
      String key = entry.getKey();
      if (key.equals(AVRO_SCHEMA_METADATA_KEY)) {
        // check compatibility of avro schemas in metadata
        Set<String> writerSchemaStrings = entry.getValue();
        Set<Schema> writerSchemas = new HashSet<Schema>();
        /* It is necessary to instantiate a Schema.Parser for each schema
         * because we are parsing for the same type with different, "evolved" definitions!
         */
        for (String s : writerSchemaStrings) {
          Schema.Parser parser = new Schema.Parser();
          Schema writerSchema = parser.parse(s);
          writerSchemas.add(writerSchema);
        }
        if (!areSchemasCompatible(readerSchema, writerSchemas)) {
          throw new RuntimeException("could not merge metadata: key " + AVRO_SCHEMA_METADATA_KEY + " contains incompatible schemas");
        }
        // if all writer schemas are compatible with the reader, reassign metadata schema to the reader schema
        mergedKeyValueMetadata.put(AVRO_SCHEMA_METADATA_KEY, readerSchema.toString());
      } else if (entry.getValue().size() > 1) {
        throw new RuntimeException("could not merge metadata: key " + key  + " has conflicting values");
      } else {
        String value = entry.getValue().iterator().next();
        mergedKeyValueMetadata.put(key, value);
      }
    }
    return mergedKeyValueMetadata;
  }

 /**
   * If an {@link #AVRO_READ_SCHEMA} is provided and the {@link #AVRO_SCHEMA_COMPATIBILITY_CHECK}
   * flag has been set, all Avro schemas found in the metadata will be checked for compatibility
   * against the {@code AVRO_READ_SCHEMA}.
   */
  static Map<String, String> getMergedKeyValueMetadata(InitContext context) {
    Configuration configuration = context.getConfiguration();
    String readerSchemaString = configuration.get(AVRO_READ_SCHEMA);
    boolean checkSchemaCompatibility = configuration.getBoolean(AVRO_SCHEMA_COMPATIBILITY_CHECK, false);
    // attempt to resolve/evolve writer schemas to the reader schema
    // iff a reader schema is provided and the compatibility check flag is flipped
    if (readerSchemaString != null && checkSchemaCompatibility) {
      Schema.Parser parser = new Schema.Parser();
      Schema readerSchema = parser.parse(readerSchemaString);
      return mergeKeyValueMetadata(context, readerSchema);
    // otherwise, do not attempt to perform schema resolution/evolution
    } else {
      return context.getMergedKeyValueMetaData();
    }
  }

  @Override
  public ReadContext init(InitContext context) {
    MessageType fileSchema = context.getFileSchema();
    MessageType schema = fileSchema;
    Map<String, String> metadata = getMergedKeyValueMetadata(context);
    Configuration configuration = context.getConfiguration();
    String readerSchemaString = configuration.get(AVRO_READ_SCHEMA);
    if (readerSchemaString != null) {
      metadata.put(AVRO_READ_SCHEMA_METADATA_KEY, readerSchemaString);
    }
    String requestedProjectionString = configuration.get(AVRO_REQUESTED_PROJECTION);
    if (requestedProjectionString != null) {
      Schema avroRequestedProjection = new Schema.Parser().parse(requestedProjectionString);
      schema = new AvroSchemaConverter().convert(avroRequestedProjection);
    }
    String avroReadSchema = configuration.get(AVRO_READ_SCHEMA);
    if (avroReadSchema != null) {
      metadata = new LinkedHashMap<String, String>();
      metadata.put(AVRO_READ_SCHEMA_METADATA_KEY, avroReadSchema);
    }
    // use getSchemaForRead because it checks that the requested schema is a
    // subset of the columns in the file schema
    return new ReadContext(getSchemaForRead(fileSchema, schema), metadata);
  }

  @Override
  public RecordMaterializer<T> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
    MessageType parquetSchema = readContext.getRequestedSchema();
    Schema avroSchema;
    if (readContext.getReadSupportMetadata() != null &&
        readContext.getReadSupportMetadata().get(AVRO_READ_SCHEMA_METADATA_KEY) != null) {
      // use the Avro read schema provided by the user
      avroSchema = new Schema.Parser().parse(readContext.getReadSupportMetadata().get(AVRO_READ_SCHEMA_METADATA_KEY));
    } else if (keyValueMetaData.get(AVRO_SCHEMA_METADATA_KEY) != null) {
      // use the Avro schema from the file metadata if present
      avroSchema = new Schema.Parser().parse(keyValueMetaData.get(AVRO_SCHEMA_METADATA_KEY));
    } else {
      // default to converting the Parquet schema into an Avro schema
      avroSchema = new AvroSchemaConverter().convert(parquetSchema);
    }
    Class<? extends AvroDataSupplier> suppClass = configuration.getClass(AVRO_DATA_SUPPLIER,
        SpecificDataSupplier.class,
        AvroDataSupplier.class);
    AvroDataSupplier supplier =ReflectionUtils.newInstance(suppClass, configuration);
    return new AvroRecordMaterializer<T>(parquetSchema, avroSchema, supplier.get());
  }
}
