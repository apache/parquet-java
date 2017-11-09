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
package org.apache.parquet.column;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Comparator;

/**
 * Utility class to provide {@link java.util.Comparator} implementations for logical types.
 */
public class Comparators {

  public static class BooleanComparator implements Comparator<Boolean> {
    @Override
    public int compare(Boolean o1, Boolean o2) {
      return compare(o1.booleanValue(), o2.booleanValue());
    }

    public int compare(boolean b1, boolean b2) {
      return Boolean.compare(b1, b2);
    }

    /**
     * Returns the string representation of the specified value for debugging/logging purposes
     */
    public String toString(boolean b) {
      return Boolean.toString(b);
    }
  }

  public static class IntComparator implements Comparator<Integer> {
    @Override
    public int compare(Integer o1, Integer o2) {
      return compare(o1.intValue(), o2.intValue());
    }

    public int compare(int i1, int i2) {
      return Integer.compare(i1, i2);
    }

    /**
     * Returns the string representation of the specified value for debugging/logging purposes
     */
    public String toString(int i) {
      return Integer.toString(i);
    }
  }

  public static class LongComparator implements Comparator<Long> {
    @Override
    public int compare(Long o1, Long o2) {
      return compare(o1.longValue(), o2.longValue());
    }

    public int compare(long l1, long l2) {
      return Long.compare(l1, l2);
    }

    /**
     * Returns the string representation of the specified value for debugging/logging purposes
     */
    public String toString(long l) {
      return Long.toString(l);
    }
  }

  public static class FloatComparator implements Comparator<Float> {
    @Override
    public int compare(Float o1, Float o2) {
      return compare(o1.floatValue(), o2.floatValue());
    }

    public int compare(float f1, float f2) {
      return Float.compare(f1, f2);
    }

    /**
     * Returns the string representation of the specified value for debugging/logging purposes
     */
    public String toString(float f) {
      return String.format("%.5f", f);
    }
  }

  public static class DoubleComparator implements Comparator<Double> {
    @Override
    public int compare(Double o1, Double o2) {
      return compare(o1.doubleValue(), o2.doubleValue());
    }

    public int compare(double d1, double d2) {
      return Double.compare(d1, d2);
    }

    /**
     * Returns the string representation of the specified value for debugging/logging purposes
     */
    public String toString(double d) {
      return String.format("%.5f", d);
    }
  }

  public static class BinaryComparator implements Comparator<Binary> {
    @Override
    public int compare(Binary o1, Binary o2) {
      return o1.compareTo(o2);
    }

    /**
     * Returns the string representation of the specified value for debugging/logging purposes
     */
    public String toString(Binary binary) {
      return binary.toStringUsingUTF8();
    }
  }

  private static final BooleanComparator BOOLEAN_COMPARATOR = new BooleanComparator();
  private static final IntComparator INT_COMPARATOR = new IntComparator();
  private static final LongComparator LONG_COMPARATOR = new LongComparator();
  private static final FloatComparator FLOAT_COMPARATOR = new FloatComparator();
  private static final DoubleComparator DOUBLE_COMPARATOR = new DoubleComparator();
  private static final BinaryComparator BINARY_COMPARATOR = new BinaryComparator();

  /**
   * Returns the proper {@link Comparator} implementation for the specified primitive and logical types. {@code
   * logicalType} might be {@code null}. In case of the specification does not allow a logical type for the related
   * primitive type, {@code logicalType} must be {@code null}.
   */
  public static Comparator<?> comparator(PrimitiveType.PrimitiveTypeName type, OriginalType logicalType) {
    switch (type) {
      case BOOLEAN:
        return booleanComparator(logicalType);
      case INT32:
        return int32Comparator(logicalType);
      case INT64:
        return int64Comparator(logicalType);
      case FLOAT:
        return floatComparator(logicalType);
      case DOUBLE:
        return doubleComparator(logicalType);
      case INT96:
      case FIXED_LEN_BYTE_ARRAY:
      case BINARY:
        return binaryComparator(type, logicalType);
      default:
        throw new UnknownColumnTypeException(type);
    }
  }

  public static BooleanComparator booleanComparator(OriginalType logicalType) {
    if (logicalType != null)
      throw new IllegalArgumentException("Invalid logical type for BOOLEAN: " + logicalType);
    return BOOLEAN_COMPARATOR;
  }

  public static LongComparator int64Comparator(OriginalType logicalType) {
    if (logicalType == null)
      return LONG_COMPARATOR;
    switch (logicalType) {
      case INT_64:
      case DECIMAL:
      case TIME_MICROS:
      case TIMESTAMP_MILLIS:
      case TIMESTAMP_MICROS:
        return LONG_COMPARATOR;
      case UINT_64:
        // TODO: return unsigned comparator
        return LONG_COMPARATOR;
      default:
        throw new IllegalArgumentException("Invalid logical type for INT64: " + logicalType);
    }
  }

  public static IntComparator int32Comparator(OriginalType logicalType) {
    if (logicalType == null)
      return INT_COMPARATOR;
    switch (logicalType) {
      case INT_8:
      case INT_16:
      case INT_32:
      case DECIMAL:
      case DATE:
      case TIME_MILLIS:
        return INT_COMPARATOR;
      case UINT_8:
      case UINT_16:
      case UINT_32:
        // TODO: return unsigned comparator
        return INT_COMPARATOR;
      default:
        throw new IllegalArgumentException("Invalid logical type for INT32: " + logicalType);
    }
  }

  public static FloatComparator floatComparator(OriginalType logicalType) {
    if (logicalType != null)
      throw new IllegalArgumentException("Invalid logical type for FLOAT: " + logicalType);
    return FLOAT_COMPARATOR;
  }

  public static DoubleComparator doubleComparator(OriginalType logicalType) {
    if (logicalType != null)
      throw new IllegalArgumentException("Invalid logical type for DOUBLE: " + logicalType);
    return DOUBLE_COMPARATOR;
  }

  public static BinaryComparator binaryComparator(PrimitiveType.PrimitiveTypeName type, OriginalType logicalType) {
    switch (type) {
      case INT96:
        if (logicalType == null)
          // TODO: what to return here?
          return BINARY_COMPARATOR;
        break;
      case FIXED_LEN_BYTE_ARRAY:
        if (logicalType == null)
          // TODO: return lexicographical comparator
          return BINARY_COMPARATOR;
        switch (logicalType) {
          case DECIMAL:
            // TODO: return signed comparator
            return BINARY_COMPARATOR;
          case INTERVAL:
            // TODO: return lexicographical comparator
            return BINARY_COMPARATOR;
        }
        break;
      case BINARY:
        if (logicalType == null)
          // TODO: return lexicographical comparator
          return BINARY_COMPARATOR;
        switch (logicalType) {
          case UTF8:
          case ENUM:
          case INTERVAL:
            // TODO: return lexicographical comparator
            return BINARY_COMPARATOR;
          case JSON:
          case BSON:
            // TODO: Based on specs we do not have ordering for these while we specified lexicographical in ColumnOrder
            return BINARY_COMPARATOR;
          case DECIMAL:
            // TODO: return signed comparator
            return BINARY_COMPARATOR;
        }
        break;
      default:
        throw new IllegalArgumentException("Not a binary type: " + type);
    }
    throw new IllegalArgumentException("Invalid logical type for " + type + ": " + logicalType);
  }

  private Comparators() {
  }
}
