package parquet.hadoop.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import parquet.example.data.Group;
import parquet.hadoop.ParquetOutputFormat;
import parquet.schema.MessageType;

/**
 * An example output format
 *
 * must be provided the schema up front
 * @see ExampleOutputFormat#setSchema(Configuration, MessageType)
 * @see GroupWriteSupport#PARQUET_EXAMPLE_SCHEMA
 *
 * @author Julien Le Dem
 *
 */
public class ExampleOutputFormat extends ParquetOutputFormat<Group> {

  /**
   * set the schema being written to the job conf
   * @param schema the schema of the data
   * @param configuration the job configuration
   */
  public static void setSchema(Job job, MessageType schema) {
    GroupWriteSupport.setSchema(schema, job.getConfiguration());
  }

  /**
   * retrieve the schema from the conf
   * @param configuration the job conf
   * @return the schema
   */
  public static MessageType getSchema(Job job) {
    return GroupWriteSupport.getSchema(job.getConfiguration());
  }

  public ExampleOutputFormat() {
    super(new GroupWriteSupport());
  }
}
