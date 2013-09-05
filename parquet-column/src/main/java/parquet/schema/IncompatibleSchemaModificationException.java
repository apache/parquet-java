package parquet.schema;

import parquet.ParquetRuntimeException;

/**
 * thrown when we are trying to read together files with incompatible schemas.
 *
 * @author Julien Le Dem
 *
 */
public class IncompatibleSchemaModificationException extends ParquetRuntimeException {
  private static final long serialVersionUID = 1L;

  public IncompatibleSchemaModificationException() {
    super();
  }

  public IncompatibleSchemaModificationException(String message,
      Throwable cause) {
    super(message, cause);
  }

  public IncompatibleSchemaModificationException(String message) {
    super(message);
  }

  public IncompatibleSchemaModificationException(Throwable cause) {
    super(cause);
  }

}
