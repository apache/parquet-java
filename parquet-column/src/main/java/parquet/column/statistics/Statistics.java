/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package parquet.column.statistics;

import parquet.column.UnknownColumnTypeException;
import parquet.io.api.Binary;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import java.util.Arrays;


/**
 * Statistics class to keep track of statistics in parquet pages and column chunks
 *
 * @author Katya Gonina
 */
public abstract class Statistics<T extends Comparable<T>> {

  private boolean hasNonNullValue;
  private long num_nulls;

  public Statistics() {
    hasNonNullValue = false;
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
   * Equality comparison method to compare two statistics objects.
   * @param stats Statistics object to compare against
   * @return true if objects are equal, false otherwise
   */
  public boolean equals(Statistics stats) {
    return Arrays.equals(stats.getMaxBytes(), this.getMaxBytes()) &&
           Arrays.equals(stats.getMinBytes(), this.getMinBytes()) &&
           stats.getNumNulls() == this.getNumNulls();
  }

  /**
   * Hash code for the statistics object
   * @return hash code int
   */
  public int hashCode() {
    return 31 * Arrays.hashCode(getMaxBytes()) + 17 * Arrays.hashCode(getMinBytes()) + Long.valueOf(this.getNumNulls()).hashCode();
  }

  /**
   * Method to merge this statistics object with the object passed
   * as parameter. Merging keeps the smallest of min values, largest of max
   * values and combines the number of null counts.
   * @param stats Statistics object to merge with
   */
  public void mergeStatistics(Statistics stats) {
    if (stats.isEmpty()) return;

    if (this.getClass() == stats.getClass()) {
      incrementNumNulls(stats.getNumNulls());
      if (stats.hasNonNullValue()) {
        mergeStatisticsMinMax(stats);
        markAsNotEmpty();
      }
    } else {
      throw new StatisticsClassException(this.getClass().toString(), stats.getClass().toString());
    }
  }

  /**
   * Abstract method to merge this statistics min and max with the values
   * of the parameter object. Does not do any checks, only called internally.
   * @param stats Statistics object to merge with
   */
  abstract protected void mergeStatisticsMinMax(Statistics stats);

  /**
   * Abstract method to set min and max values from byte arrays.
   * @param minBytes byte array to set the min value to
   * @param maxBytes byte array to set the max value to
   */
  abstract public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes);

  abstract public T genericGetMin();
  abstract public T genericGetMax();

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
   * toString() to display min, max, num_nulls in a string
   */
  abstract public String toString();


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
    return !hasNonNullValue && num_nulls == 0;
  }

  /**
   * Returns whether there have been non-null values added to this statistics
   */
  public boolean hasNonNullValue() {
    return hasNonNullValue;
  }
 
  /**
   * Sets the page/column as having a valid non-null value
   * kind of misnomer here
   */ 
  protected void markAsNotEmpty() {
    hasNonNullValue = true;
  }
}

