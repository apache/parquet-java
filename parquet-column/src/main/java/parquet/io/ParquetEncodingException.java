package parquet.io;

import parquet.ParquetRuntimeException;

public class ParquetEncodingException extends ParquetRuntimeException {
  private static final long serialVersionUID = 1L;

  public ParquetEncodingException() {
  }

  public ParquetEncodingException(String message, Throwable cause) {
    super(message, cause);
  }

  public ParquetEncodingException(String message) {
    super(message);
  }

  public ParquetEncodingException(Throwable cause) {
    super(cause);
  }

}
