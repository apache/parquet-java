package parquet.thrift;

import parquet.io.ParquetEncodingException;

/**
 * Throw this exception when the schema used for decoding is not compatible with the schema in the data.
 */
public class DecodingSchemaMismatchException extends ParquetEncodingException {
  public DecodingSchemaMismatchException(String s) {
    super(s);
  }
}
