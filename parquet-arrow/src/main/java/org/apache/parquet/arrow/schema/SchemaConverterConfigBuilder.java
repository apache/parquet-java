package org.apache.parquet.arrow.schema;

/**
 * This class builds {@link SchemaConverterConfig}s.
 */
public class SchemaConverterConfigBuilder {

  private boolean convertInt96ToArrowTimestamp;

  /**
   * Default constructor for the {@link SchemaConverterConfigBuilder}.
   */
  public SchemaConverterConfigBuilder() {
    this.convertInt96ToArrowTimestamp = false;
  }

  public SchemaConverterConfigBuilder setConvertInt96ToArrowTimestamp(boolean convertInt96ToArrowTimestamp) {
    this.convertInt96ToArrowTimestamp = convertInt96ToArrowTimestamp;
    return this;
  }

  /**
   * This builds the {@link SchemaConverterConfig} from the provided params.
   */
  public SchemaConverterConfig build() {
    return new SchemaConverterConfig(
        this.convertInt96ToArrowTimestamp);
  }
}
