package redelm;

abstract public class RedelmRuntimeException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public RedelmRuntimeException() {
    super();
  }

  public RedelmRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public RedelmRuntimeException(String message) {
    super(message);
  }

  public RedelmRuntimeException(Throwable cause) {
    super(cause);
  }

}
