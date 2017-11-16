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
package org.apache.parquet.column.statistics;

import org.apache.parquet.ShouldNeverHappenException;
import org.apache.parquet.column.UnknownColumnTypeException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveComparator;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;


/**
 * Statistics class to keep track of statistics in parquet pages and column chunks
 *
 * @author Katya Gonina
 */
public abstract class Statistics<T extends Comparable<T>> implements Cloneable {

  private final PrimitiveComparator<T> comparator;
  private boolean hasNonNullValue;
  private long num_nulls;

  Statistics() {
    this(PrimitiveComparator.<T>comparableComparator());
  }

  Statistics(PrimitiveComparator<T> comparator) {
    hasNonNullValue = false;
    num_nulls = 0;
    this.comparator = comparator;
  }

  /**
   * Returns the typed statistics object based on the passed type parameter
   * @param type PrimitiveTypeName type of the column
   * @return instance of a typed statistics class
   * @deprecated Use {@link #createStats(Type)} or {@link #createLegacyStats(PrimitiveTypeName)} instead
   */
  @Deprecated
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
   * Creates an empty {@code Statistics} instance for the specified type to be used for reading/writing the legacy
   * min/max statistics.
   *
   * @param type type of the column
   * @return instance of a typed statistics class
   */
  public static Statistics createLegacyStats(PrimitiveTypeName type) {
    return getStatsBasedOnType(type);
  }

  /**
   * Creates an empty {@code Statistics} instance for the specified type to be used for reading/writing the new min/max
   * statistics used in the V2 format.
   *
   * @param type type of the column
   * @return instance of a typed statistics class
   */
  public static Statistics<?> createStats(Type type) {
    PrimitiveTypeName primitive = type.asPrimitiveType().getPrimitiveTypeName();
    switch (primitive) {
      case INT32:
        return new IntStatistics(type);
      case INT64:
        return new LongStatistics(type);
      case FLOAT:
        return new FloatStatistics(type);
      case DOUBLE:
        return new DoubleStatistics(type);
      case BOOLEAN:
        return new BooleanStatistics(type);
      case BINARY:
      case INT96:
      case FIXED_LEN_BYTE_ARRAY:
        return new BinaryStatistics(type);
      default:
        throw new UnknownColumnTypeException(primitive);
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
   * @param other Object to compare against
   * @return true if objects are equal, false otherwise
   */
  @Override
  public boolean equals(Object other) {
    if (other == this)
      return true;
    if (!(other instanceof Statistics))
      return false;
    Statistics stats = (Statistics) other;
    return Arrays.equals(stats.getMaxBytes(), this.getMaxBytes()) &&
            Arrays.equals(stats.getMinBytes(), this.getMinBytes()) &&
            stats.getNumNulls() == this.getNumNulls();
  }

  /**
   * Hash code for the statistics object
   * @return hash code int
   */
  @Override
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

  /**
   * Returns the min value in the statistics. The java natural order of the returned type defined by {@link
   * T#compareTo(Object)} might not be the proper one. For example, UINT_32 requires unsigned comparison instead of the
   * natural signed one. Use {@link #compareToMin(Comparable)} or the comparator returned by {@link #comparator()} to
   * always get the proper ordering.
   */
  abstract public T genericGetMin();

  /**
   * Returns the max value in the statistics. The java natural order of the returned type defined by {@link
   * T#compareTo(Object)} might not be the proper one. For example, UINT_32 requires unsigned comparison instead of the
   * natural signed one. Use {@link #compareToMax(Comparable)} or the comparator returned by {@link #comparator()} to
   * always get the proper ordering.
   */
  abstract public T genericGetMax();

  /**
   * Returns the comparator to be used to compare two generic values in the proper way (for example, unsigned comparison
   * for UINT_32).
   */
  public final Comparator<T> comparator() {
    return comparator;
  }

  /**
   * Compares the specified value to min in the proper way.
   *
   * @see Comparable#compareTo(Object)
   */
  public final int compareToMin(T value) {
    return comparator.compare(genericGetMin(), value);
  }

  /**
   * Compares the specified value to max in the proper way.
   *
   * @see Comparable#compareTo(Object)
   */
  public final int compareToMax(T value) {
    return comparator.compare(genericGetMax(), value);
  }

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
   * Returns the string representation of min for debugging/logging purposes.
   */
  public String minAsString() {
    return toString(genericGetMin());
  }

  /**
   * Returns the string representation of max for debugging/logging purposes.
   */
  public String maxAsString() {
    return toString(genericGetMax());
  }

  String toString(T value) {
    return Objects.toString(value);
  }

  /**
   * Abstract method to return whether the min and max values fit in the given
   * size.
   * @param size a size in bytes
   * @return true iff the min and max values are less than size bytes
   */
  abstract public boolean isSmallerThan(long size);

  @Override
  public String toString() {
    if (this.hasNonNullValue())
      return String.format("min: %s, max: %s, num_nulls: %d", minAsString(), maxAsString(), this.getNumNulls());
    else if (!this.isEmpty())
      return String.format("num_nulls: %d, min/max not defined", this.getNumNulls());
    else
      return "no stats for this column";
  }

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

  @Override
  public Statistics<T> clone() {
    try {
      return (Statistics<T>) super.clone();
    } catch(CloneNotSupportedException e) {
      throw new ShouldNeverHappenException(e);
    }
  }
}

