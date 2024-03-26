package org.apache.parquet.arrow.schema;

/**
 * This class configures the Parquet-to-Arrow schema conversion process.
 */
public class SchemaConverterConfig {

  // Indicates if Int96 should be converted to Arrow Timestamp
  private final boolean convertInt96ToArrowTimestamp;

  SchemaConverterConfig(boolean convertInt96ToArrowTimestamp) {
    this.convertInt96ToArrowTimestamp = convertInt96ToArrowTimestamp;
  }

  public boolean getConvertInt96ToArrowTimestamp() {
    return this.convertInt96ToArrowTimestamp;
  }
}
