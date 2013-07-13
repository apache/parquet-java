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
public class AvroReadSupport extends ReadSupport<IndexedRecord> {

  @Override
  public ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
    return new ReadContext(fileSchema);
  }

  @Override
  public RecordMaterializer<IndexedRecord> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
    Schema avroSchema = new Schema.Parser().parse(keyValueMetaData.get("avro.schema"));
    return new AvroRecordMaterializer(readContext.getRequestedSchema(), avroSchema);
  }
}
