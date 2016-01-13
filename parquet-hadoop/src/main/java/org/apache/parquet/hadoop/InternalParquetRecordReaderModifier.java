package org.apache.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.HashMap;
import java.util.Map;

/**
 * Copyright 2015 Tencent Inc.
 *
 * @author lwlin <lwlin@tencent.com>
 */
public class InternalParquetRecordReaderModifier {

  private static PrimitiveType createPrimitiveType(String name) {
    return new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, name);
  }

  public static void main(String[] args) throws Exception {
    final MessageType fileSchema = new MessageType("root",
                                                   createPrimitiveType("a"),
                                                   createPrimitiveType("b"));
    final MessageType requestedSchema = new MessageType("root",
                                                        createPrimitiveType("a"));

    ReadSupport<String> readSupport = new ReadSupport<String>() {
      @Override
      public RecordMaterializer<String> prepareForRead(
          Configuration configuration,
          Map<String, String> keyValueMetaData,
          MessageType fileSchema,
          ReadContext readContext) {
        return null;
      }

      public ReadContext init(InitContext context) {
        return new ReadContext(requestedSchema);
      }
    };

    FilterCompat.Filter filter = FilterCompat.get(FilterApi.eq(FilterApi.intColumn("b"), 1));

    InternalParquetRecordReader<String> reader =
        new InternalParquetRecordReader<String>(readSupport, filter);

    FileMetaData fileMetaData = new FileMetaData(fileSchema, new HashMap<String, String>(),
                                                 "createdBy");

    reader.initialize(fileSchema, fileMetaData, null, null, null);

    System.out.println(reader);
  }
}