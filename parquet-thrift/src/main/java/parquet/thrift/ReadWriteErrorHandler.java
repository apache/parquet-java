package parquet.thrift;

import org.apache.thrift.protocol.TField;

/**
 * Implements this class to handle errors encountered during reading and writing
 * @author Tianshuo Deng
 */
public interface ReadWriteErrorHandler {
  /**
   * handle when a record can not be read due to an exception,
   * in this case the record will be skipped and this method will be called with the exception that caused reading failure
   *
   * @param e
   */
  void handleSkippedCorruptedRecord(SkippableException e);

  /**
   * handle when a record that contains fields that are ignored, meaning that the schema provided does not cover all the columns in data,
   * the record will still be written but with fields that are not defined in the schema ignored.
   * For each record, this method will be called at most once.
   */
  void handleRecordHasFieldIgnored();

  /**
   * handle when a field gets ignored,
   * notice the difference between this method and {@link #handleRecordHasFieldIgnored()} is that:
   * for one record, this method maybe called many times when there are multiple fields not defined in the schema.
   *
   * @param field
   */
  void handleFieldIgnored(TField field);

  /**
   * handle when there is a record that has incompatible schema
   * @param e
   */
  void handleSkipRecordDueToSchemaMismatch(DecodingSchemaMismatchException e);
}