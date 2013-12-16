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

  public static String AVRO_REQUESTED_PROJECTION = "parquet.avro.projection";
  public static String AVRO_REQUESTED_SCHEMA = "avro.schema";

  public static void setRequestedProjection(Configuration configuration, Schema requestedProjection) {
    configuration.set(AVRO_REQUESTED_PROJECTION, requestedProjection.toString());
  }

  public static void setRequestedSchema(Configuration configuration,
      Schema requestedProjection) {
    configuration.set(AVRO_REQUESTED_SCHEMA, requestedProjection.toString());
  }

  @Override
  public ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
    String requestedProjectionString = configuration.get(AVRO_REQUESTED_PROJECTION);
    if (requestedProjectionString != null) {
      Schema avroRequestedProjection = new Schema.Parser().parse(requestedProjectionString);
      MessageType requestedProjection = new AvroSchemaConverter().convert(avroRequestedProjection);
      fileSchema.checkContains(requestedProjection);
      ReadContext retValue = new ReadContext(requestedProjection);
      String specAvroSchema = configuration.get(AVRO_REQUESTED_SCHEMA);
      if (specAvroSchema == null) {
        return new ReadContext(fileSchema);
      } else {
        Map<String, String> metadata = new LinkedHashMap<String, String>();
        metadata.put("req.avro.schema", specAvroSchema);
        return new ReadContext(requestedProjection, metadata);
      }
    } else {
      return new ReadContext(fileSchema);
    }
  }

  @Override
  public RecordMaterializer<T> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
    String specAvroSchema = (readContext != null && readContext.getReadSupportMetadata() != null) ? readContext.getReadSupportMetadata().get("req.avro.schema") : null;
    Schema avroSchema = new Schema.Parser().parse(specAvroSchema == null ? keyValueMetaData.get("avro.schema") : specAvroSchema);
    return new AvroRecordMaterializer<T>(readContext.getRequestedSchema(), avroSchema);
  }
}
