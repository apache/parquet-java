package parquet.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

class AvroRecordMaterializer extends RecordMaterializer<GenericRecord> {

  private AvroGenericRecordConverter root;

  public AvroRecordMaterializer(MessageType requestedSchema, Schema avroSchema) {
    this.root = new AvroGenericRecordConverter(requestedSchema, avroSchema);
  }

  @Override
  public GenericRecord getCurrentRecord() {
    return root.getCurrentRecord();
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }
}
