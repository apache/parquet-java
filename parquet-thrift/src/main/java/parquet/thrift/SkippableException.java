package parquet.thrift;

import parquet.ParquetRuntimeException;

/**
 *
 * Thrown when an error happened reading a thrift record
 * Ignoring this exception will skip the bad record
 *
 * @author Julien Le Dem
 *
 */
public class SkippableException extends ParquetRuntimeException {
  private static final long serialVersionUID = 1L;

  SkippableException() {
    super();
  }

  SkippableException(String message, Throwable cause) {
    super(message, cause);
  }

  SkippableException(String message) {
    super(message);
  }

  SkippableException(Throwable cause) {
    super(cause);
  }

}
