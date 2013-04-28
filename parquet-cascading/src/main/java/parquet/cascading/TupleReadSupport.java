package parquet.cascading;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import cascading.tuple.Tuple;
import cascading.tuple.Fields;

import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;
import parquet.cascading.convert.TupleRecordMaterializer;

public class TupleReadSupport extends ReadSupport<Tuple> {
  static final String PARQUET_CASCADING_REQUESTED_FIELDS = "parquet.cascading.requested.fields";

  static protected Fields getRequestedFields(Configuration configuration) {
    String fieldsString = configuration.get(PARQUET_CASCADING_REQUESTED_FIELDS);
    return parseFieldsString(fieldsString);
  }

  static protected void setRequestedFields(Configuration configuration, Fields fields) {
    String fieldsString = buildFieldsString(fields);
    configuration.set(PARQUET_CASCADING_REQUESTED_FIELDS, fieldsString);
  }

  static private Fields parseFieldsString(String fieldsString) {
    //TODO
    return null;
  }

  static private String buildFieldsString(Fields fields) {
    //TODO
    return "";
  }

  @Override
  public ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
    Fields requestedFields = getRequestedFields(configuration);
    if (requestedFields == null) {
      return new ReadContext(fileSchema);
    } else {
      return new ReadContext(FilterSchema.filterSchema(fileSchema, requestedFields));
    }
  }

  @Override
  public RecordMaterializer<Tuple> prepareForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext) {
    MessageType requestedSchema = readContext.getRequestedSchema();
    return new TupleRecordMaterializer(requestedSchema);
  }

}
