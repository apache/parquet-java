package redelm.io;

import redelm.RedelmRuntimeException;

public class InvalidRecordException extends RedelmRuntimeException {
  private static final long serialVersionUID = 1L;

  public InvalidRecordException() {
    super();
  }

  public InvalidRecordException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidRecordException(String message) {
    super(message);
  }

  public InvalidRecordException(Throwable cause) {
    super(cause);
  }

}
