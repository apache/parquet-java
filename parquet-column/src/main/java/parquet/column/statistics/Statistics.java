package parquet.column.statistics;

import parquet.column.UnknownColumnTypeException;
import parquet.io.api.Binary;
import parquet.schema.PrimitiveType.PrimitiveTypeName;


/**
 * Statistics class to keep track of statistics in parquet pages and column chunks
 *
 * @author Katya Gonina
 */
public abstract class Statistics {

  private boolean firstValueAccountedFor;
  private long num_nulls;

  public Statistics() {
    firstValueAccountedFor = false;
    num_nulls = 0;
  }

  /**
   * Returns the typed statistics object based on the passed type parameter
   * @param type PrimitiveTypeName type of the column
   * @return instance of a typed statistics class
   */
  public static Statistics getStatsBasedOnType(PrimitiveTypeName type) {
    switch(type) {
    case INT32:
      return new IntStatistics();
    case INT64:
      return new LongStatistics();
    case FLOAT:
      return new FloatStatistics();
    case DOUBLE:
      return new DoubleStatistics();
    case BOOLEAN:
      return new BooleanStatistics();
    case BINARY:
      return new BinaryStatistics();
    case INT96:
      return new BinaryStatistics();
    case FIXED_LEN_BYTE_ARRAY:
      return new BinaryStatistics();
    default:
      throw new UnknownColumnTypeException(type);
    }
  }

  /**
   * updates statistics min and max using the passed value
   * @param value value to use to update min and max
   */
  public void updateStats(int value) {
    throw new UnsupportedOperationException();
  }

  /**
   * updates statistics min and max using the passed value
   * @param value value to use to update min and max
   */
  public void updateStats(long value) {
    throw new UnsupportedOperationException();
  }

  /**
   * updates statistics min and max using the passed value
   * @param value value to use to update min and max
   */
  public void updateStats(float value) {
    throw new UnsupportedOperationException();
  }

  /**
   * updates statistics min and max using the passed value
   * @param value value to use to update min and max
   */
  public void updateStats(double value) {
    throw new UnsupportedOperationException();
  }

  /**
   * updates statistics min and max using the passed value
   * @param value value to use to update min and max
   */
  public void updateStats(boolean value) {
    throw new UnsupportedOperationException();
  }

  /**
   * updates statistics min and max using the passed value
   * @param value value to use to update min and max
   */
  public void updateStats(Binary value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Abstract equals method to compare two statistics objects. Used in tests.
   * @param stats Statistics object to compare against
   * @return true if objects are equal, false otherwise
   */
  abstract public boolean equals(Statistics stats);

  /**
   * Abstract method to merge this statistics object with the object passed
   * as parameter. Merging keeps the smallest of min values, largest of max
   * values and combines the number of null counts.
   * @param stats Statistics object to merge with
   */
  abstract public void mergeStatistics(Statistics stats);

  /**
   * Abstract method to set min and max values from byte arrays.
   * @param minBytes byte array to set the min value to
   * @param maxBytes byte array to set the max value to
   */
  abstract public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes);

  /**
   * Abstract method to return the max value as a byte array
   * @return byte array corresponding to the max value
   */
  abstract public byte[] getMaxBytes();

  /**
   * Abstract method to return the min value as a byte array
   * @return byte array corresponding to the min value
   */
  abstract public byte[] getMinBytes();

  /**
   * Increments the null count by one
   */
  public void incrementNumNulls() {
    num_nulls++ ;
  }

  /**
   * Increments the null count by the parameter value
   * @param increment value to increment the null count by
   */
  public void incrementNumNulls(long increment) {
    num_nulls += increment ;
  }

  /**
   * Returns the null count
   * @return null count
   */
  public long getNumNulls() {
    return num_nulls;
  }

  /**
   * Sets the number of nulls to the parameter value
   * @param nulls null count to set the count to
   */
  public void setNumNulls(long nulls) {
    num_nulls = nulls;
  }

  /**
   * Returns a boolean specifying if the Statistics object is empty,
   * i.e does not contain valid statistics for the page/column yet
   * @return true if object is empty, false otherwise
   */
  public boolean isEmpty() {
    return !firstValueAccountedFor;
  }

  protected void markAsNotEmpty() {
    firstValueAccountedFor = true;
  }
}

