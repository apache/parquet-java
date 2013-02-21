package parquet.io;

import parquet.ParquetRuntimeException;

public class CompilationException extends ParquetRuntimeException {
  private static final long serialVersionUID = 1L;

  public CompilationException() {
  }

  public CompilationException(String message, Throwable cause) {
    super(message, cause);
  }

  public CompilationException(String message) {
    super(message);
  }

  public CompilationException(Throwable cause) {
    super(cause);
  }

}
