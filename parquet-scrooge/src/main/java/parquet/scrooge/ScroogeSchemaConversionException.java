package parquet.scrooge;

import parquet.ParquetRuntimeException;

/**
 * Throw this exception when there is an error converting a Scrooge class to
 * thrift schema
 */
class ScroogeSchemaConversionException extends ParquetRuntimeException {
  public ScroogeSchemaConversionException(String message, Throwable cause) {
    super(message, cause);
  }

  public ScroogeSchemaConversionException(String message) {
    super(message);
  }
}

