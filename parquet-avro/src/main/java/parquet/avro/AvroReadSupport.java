package parquet.avro;

import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

/**
 * Avro implementation of {@link ReadSupport} for {@link GenericRecord}s. Users should
 * use {@link AvroParquetReader} or {@link AvroParquetInputFormat} rather than using
 * this class directly.
 */
public class AvroReadSupport extends ReadSupport<GenericRecord> {

  @Override
  public ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
    return new ReadContext(fileSchema);
  }

  @Override
  public RecordMaterializer<GenericRecord> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
    Schema avroSchema = new Schema.Parser().parse(keyValueMetaData.get("avro.schema"));
    return new AvroRecordMaterializer(readContext.getRequestedSchema(), avroSchema);
  }
}
