package redelm.pig;

import redelm.RedelmRuntimeException;

public class TupleConversionException extends RedelmRuntimeException {
  private static final long serialVersionUID = 1L;

  public TupleConversionException() {
    super();
  }

  public TupleConversionException(String message, Throwable cause) {
    super(message, cause);
  }

  public TupleConversionException(String message) {
    super(message);
  }

  public TupleConversionException(Throwable cause) {
    super(cause);
  }

}
