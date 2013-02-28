package parquet.hadoop.example;

import parquet.example.data.Group;
import parquet.hadoop.ParquetInputFormat;

/**
 * Example input format to read Parquet files
 *
 * This Input format uses a rather inefficient data model but works independently of higher level abstractions.
 *
 * @author Julien Le Dem
 *
 */
public class ExampleInputFormat extends ParquetInputFormat<Group> {

  public ExampleInputFormat() {
    super(GroupReadSupport.class);
  }

}
