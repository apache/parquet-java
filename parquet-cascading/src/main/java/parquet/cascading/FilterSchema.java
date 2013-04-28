package parquet.cascading;

import parquet.schema.MessageType;
import cascading.tuple.Fields;

public class FilterSchema {
  public static MessageType filterSchema(MessageType fileSchema, Fields requestedFields) {
    //TODO
    return fileSchema;
  }

  public static Fields filterFields(MessageType fileSchema, Fields requestedFields) {
    //TODO
    return requestedFields;
  }
}
