package parquet.thrift;

import org.apache.thrift.protocol.TField;

/**
 * Implements this class to handle when fields get ignored in {@link BufferedProtocolReadToWrite}
 * @author Tianshuo Deng
 */
public abstract class FieldIgnoredHandler {

  /**
   * handle when a record that contains fields that are ignored, meaning that the schema provided does not cover all the columns in data,
   * the record will still be written but with fields that are not defined in the schema ignored.
   * For each record, this method will be called at most once.
   */
  public void handleRecordHasFieldIgnored() {
  }

  /**
   * handle when a field gets ignored,
   * notice the difference between this method and {@link #handleRecordHasFieldIgnored()} is that:
   * for one record, this method maybe called many times when there are multiple fields not defined in the schema.
   *
   * @param field
   */
  public void handleFieldIgnored(TField field) {
  }
}