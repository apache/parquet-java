package parquet.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.Job;
import parquet.avro.AvroWriteSupport;
import parquet.hadoop.ParquetOutputFormat;

/**
 * A Hadoop {@link org.apache.hadoop.mapreduce.OutputFormat} for Parquet files.
 */
public class AvroParquetOutputFormat extends ParquetOutputFormat<GenericRecord> {

  public static void setSchema(Job job, Schema schema) {
    AvroWriteSupport.setSchema(job.getConfiguration(), schema);
  }

  public AvroParquetOutputFormat() {
    super(new AvroWriteSupport());
  }

}
