package parquet.pojo;

import parquet.ParquetRuntimeException;

public class BadGenericsConfigurationException extends ParquetRuntimeException {
  public BadGenericsConfigurationException(String message) {
    super(message);
  }
}
