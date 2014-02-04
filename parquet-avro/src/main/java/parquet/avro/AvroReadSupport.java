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

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

/**
 * Avro implementation of {@link ReadSupport} for Avro {@link IndexedRecord}s which cover both Avro Specific and
 * Generic. Users should use {@link AvroParquetReader} or {@link AvroParquetInputFormat} rather than using
 * this class directly.
 */
public class AvroReadSupport<T extends IndexedRecord> extends ReadSupport<T> {

  public static final String AVRO_REQUESTED_PROJECTION = "parquet.avro.projection";
  private static final String AVRO_READ_SCHEMA = "parquet.avro.read.schema";

  static final String AVRO_SCHEMA_METADATA_KEY = "avro.schema";
  private static final String AVRO_READ_SCHEMA_METADATA_KEY = "avro.read.schema";

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

  @Override
  public ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
    MessageType schema = fileSchema;
    Map<String, String> metadata = null;

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
    return new ReadContext(schema, metadata);
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
    return new AvroRecordMaterializer<T>(parquetSchema, avroSchema);
  }
}
