package parquet.cascading;

import java.util.Map;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import cascading.tuple.Tuple;
import cascading.tuple.Fields;
import cascading.flow.hadoop.util.HadoopUtil;

import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;
import parquet.cascading.convert.TupleRecordMaterializer;


public class TupleReadSupport extends ReadSupport<Tuple> {
  static final String PARQUET_CASCADING_REQUESTED_FIELDS = "parquet.cascading.requested.fields";

  static protected Fields getRequestedFields(Configuration configuration) {
    try {
      String fieldsString = configuration.get(PARQUET_CASCADING_REQUESTED_FIELDS);
      return HadoopUtil.deserializeBase64(fieldsString, configuration, Fields.class);
    } catch(IOException e) {
      throw new RuntimeException(e.toString());
    }
  }

  static protected void setRequestedFields(JobConf configuration, Fields fields) {
    try {
      String fieldsString = HadoopUtil.serializeBase64(fields, configuration);
      configuration.set(PARQUET_CASCADING_REQUESTED_FIELDS, fieldsString);
    }
    catch(IOException e) {
      throw new RuntimeException(e.toString());
    }
  }

  @Override
  public ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
    Fields requestedFields = getRequestedFields(configuration);
    if (requestedFields == null) {
      return new ReadContext(fileSchema);
    } else {
      SchemaIntersection intersection = new SchemaIntersection(fileSchema, requestedFields);
      return new ReadContext(intersection.getRequestedSchema());
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
