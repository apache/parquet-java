package parquet.avro;

import org.apache.avro.generic.GenericRecord;
import parquet.avro.AvroReadSupport;
import parquet.hadoop.ParquetInputFormat;

/**
 * A Hadoop {@link org.apache.hadoop.mapreduce.InputFormat} for Parquet files.
 */
public class AvroParquetInputFormat extends ParquetInputFormat<GenericRecord> {
  public AvroParquetInputFormat() {
    super(AvroReadSupport.class);
  }
}
