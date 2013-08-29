package parquet.thrift.projection;

import parquet.ParquetRuntimeException;

/**
 * thrown if the schema can not be projected/filtered
 *
 * @author Tianshuo Deng
 *
 */

public class ThriftProjectionException extends ParquetRuntimeException {
  private static final long serialVersionUID = 1L;

  public ThriftProjectionException() {
  }

  public ThriftProjectionException(String message, Throwable cause) {
    super(message, cause);
  }

  public ThriftProjectionException(String message) {
    super(message);
  }

  public ThriftProjectionException(Throwable cause) {
    super(cause);
  }
}

