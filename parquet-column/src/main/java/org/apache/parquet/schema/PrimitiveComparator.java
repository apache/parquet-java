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
package org.apache.parquet.schema;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import org.apache.parquet.io.api.Binary;

/**
 * {@link Comparator} implementation that also supports the comparison of the related primitive type to avoid the
 * performance penalty of boxing/unboxing. The {@code compare} methods for the not supported primitive types throw
 * {@link UnsupportedOperationException}.
 * {@link Serializable} implementation that may be a UserDefinedPredicate defined this Comparator is their member variable.
 */
public abstract class PrimitiveComparator<T> implements Comparator<T>, Serializable {

  public int compare(boolean b1, boolean b2) {
    throw new UnsupportedOperationException(
        "compare(boolean, boolean) was called on a non-boolean comparator: " + toString());
  }

  public int compare(int i1, int i2) {
    throw new UnsupportedOperationException("compare(int, int) was called on a non-int comparator: " + toString());
  }

  public int compare(long l1, long l2) {
    throw new UnsupportedOperationException(
        "compare(long, long) was called on a non-long comparator: " + toString());
  }

  public int compare(float f1, float f2) {
    throw new UnsupportedOperationException(
        "compare(float, float) was called on a non-float comparator: " + toString());
  }

  public int compare(double d1, double d2) {
    throw new UnsupportedOperationException(
        "compare(double, double) was called on a non-double comparator: " + toString());
  }

  @Override
  public final int compare(T o1, T o2) {
    if (o1 == null) {
      return o2 == null ? 0 : -1;
    }
    return o2 == null ? 1 : compareNotNulls(o1, o2);
  }

  abstract int compareNotNulls(T o1, T o2);

  static final PrimitiveComparator<Boolean> BOOLEAN_COMPARATOR = new PrimitiveComparator<Boolean>() {
    @Override
    int compareNotNulls(Boolean o1, Boolean o2) {
      return compare(o1.booleanValue(), o2.booleanValue());
    }

    @Override
    public int compare(boolean b1, boolean b2) {
      return Boolean.compare(b1, b2);
    }

    @Override
    public String toString() {
      return "BOOLEAN_COMPARATOR";
    }
  };

  private abstract static class IntComparator extends PrimitiveComparator<Integer> {
    @Override
    int compareNotNulls(Integer o1, Integer o2) {
      return compare(o1.intValue(), o2.intValue());
    }
  }

  static final PrimitiveComparator<Integer> SIGNED_INT32_COMPARATOR = new IntComparator() {
    @Override
    public int compare(int i1, int i2) {
      return Integer.compare(i1, i2);
    }

    @Override
    public String toString() {
      return "SIGNED_INT32_COMPARATOR";
    }
  };

  static final PrimitiveComparator<Integer> UNSIGNED_INT32_COMPARATOR = new IntComparator() {
    @Override
    public int compare(int i1, int i2) {
      // Implemented based on com.google.common.primitives.UnsignedInts.compare(int, int)
      return Integer.compare(i1 ^ Integer.MIN_VALUE, i2 ^ Integer.MIN_VALUE);
    }

    @Override
    public String toString() {
      return "UNSIGNED_INT32_COMPARATOR";
    }
  };

  private abstract static class LongComparator extends PrimitiveComparator<Long> {
    @Override
    int compareNotNulls(Long o1, Long o2) {
      return compare(o1.longValue(), o2.longValue());
    }
  }

  static final PrimitiveComparator<Long> SIGNED_INT64_COMPARATOR = new LongComparator() {
    @Override
    public int compare(long l1, long l2) {
      return Long.compare(l1, l2);
    }

    @Override
    public String toString() {
      return "SIGNED_INT64_COMPARATOR";
    }
  };

  static final PrimitiveComparator<Long> UNSIGNED_INT64_COMPARATOR = new LongComparator() {
    @Override
    public int compare(long l1, long l2) {
      // Implemented based on com.google.common.primitives.UnsignedLongs.compare(long, long)
      return Long.compare(l1 ^ Long.MIN_VALUE, l2 ^ Long.MIN_VALUE);
    }

    @Override
    public String toString() {
      return "UNSIGNED_INT64_COMPARATOR";
    }
  };

  static final PrimitiveComparator<Float> FLOAT_COMPARATOR = new PrimitiveComparator<Float>() {
    @Override
    int compareNotNulls(Float o1, Float o2) {
      return compare(o1.floatValue(), o2.floatValue());
    }

    @Override
    public int compare(float f1, float f2) {
      return Float.compare(f1, f2);
    }

    @Override
    public String toString() {
      return "FLOAT_COMPARATOR";
    }
  };

  static final PrimitiveComparator<Double> DOUBLE_COMPARATOR = new PrimitiveComparator<Double>() {
    @Override
    int compareNotNulls(Double o1, Double o2) {
      return compare(o1.doubleValue(), o2.doubleValue());
    }

    @Override
    public int compare(double d1, double d2) {
      return Double.compare(d1, d2);
    }

    @Override
    public String toString() {
      return "DOUBLE_COMPARATOR";
    }
  };

  private abstract static class BinaryComparator extends PrimitiveComparator<Binary> {
    @Override
    int compareNotNulls(Binary o1, Binary o2) {
      return compareBinary(o1, o2);
    }

    abstract int compareBinary(Binary b1, Binary b2);

    final int toUnsigned(byte b) {
      return b & 0xFF;
    }
  }

  public static final PrimitiveComparator<Binary> UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR =
      new BinaryComparator() {
        @Override
        int compareBinary(Binary b1, Binary b2) {
          return Binary.lexicographicCompare(b1, b2);
        }

        @Override
        public String toString() {
          return "UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR";
        }
      };

  /*
   * This comparator is for comparing two timestamps represented as int96 binary.
   * It is a two level comparison.
   * Days (last 4 bytes compared as unsigned little endian int32),
   * Nanoseconds (first 8 bytes compared as unsigned little endian int64)
   */
  static final PrimitiveComparator<Binary> BINARY_AS_INT96_TIMESTAMP_COMPARATOR = new BinaryComparator() {
    @Override
    int compareBinary(Binary b1, Binary b2) {
      ByteBuffer bb1 = b1.toByteBuffer().slice();
      ByteBuffer bb2 = b2.toByteBuffer().slice();
      bb1.order(java.nio.ByteOrder.LITTLE_ENDIAN);
      bb2.order(java.nio.ByteOrder.LITTLE_ENDIAN);
      int jd1 = bb1.getInt(8);
      int jd2 = bb2.getInt(8);
      if (jd1 != jd2) return Integer.compareUnsigned(jd1, jd2) < 0 ? -1 : 1;
      long s1 = bb1.getLong(0);
      long s2 = bb2.getLong(0);
      if (s1 != s2) return Long.compareUnsigned(s1, s2) < 0 ? -1 : 1;
      return 0;
    }

    @Override
    public String toString() {
      return "BINARY_AS_INT96_TIMESTAMP_COMPARATOR";
    }
  };

  /*
   * This comparator is for comparing two signed decimal values represented in twos-complement binary. In case of the
   * binary length of one value is shorter than the other it will be padded by the corresponding prefix (0xFF for
   * negative, 0x00 for positive values).
   */
  static final PrimitiveComparator<Binary> BINARY_AS_SIGNED_INTEGER_COMPARATOR = new BinaryComparator() {
    private static final int NEGATIVE_PADDING = 0xFF;
    private static final int POSITIVE_PADDING = 0;

    @Override
    int compareBinary(Binary b1, Binary b2) {
      return compare(b1.toByteBuffer(), b2.toByteBuffer());
    }

    private int compare(ByteBuffer b1, ByteBuffer b2) {
      int l1 = b1.remaining();
      int l2 = b2.remaining();
      int p1 = b1.position();
      int p2 = b2.position();

      boolean isNegative1 = l1 > 0 && b1.get(p1) < 0;
      boolean isNegative2 = l2 > 0 && b2.get(p2) < 0;
      if (isNegative1 != isNegative2) {
        return isNegative1 ? -1 : 1;
      }

      int result = 0;

      // Compare the beginning of the longer buffer with the proper padding
      if (l1 < l2) {
        int lengthDiff = l2 - l1;
        result = -compareWithPadding(lengthDiff, b2, p2, isNegative1 ? NEGATIVE_PADDING : POSITIVE_PADDING);
        p2 += lengthDiff;
      } else if (l1 > l2) {
        int lengthDiff = l1 - l2;
        result = compareWithPadding(lengthDiff, b1, p1, isNegative2 ? NEGATIVE_PADDING : POSITIVE_PADDING);
        p1 += lengthDiff;
      }

      // The beginning of the longer buffer equals to the padding or the lengths are equal
      if (result == 0) {
        result = compare(Math.min(l1, l2), b1, p1, b2, p2);
      }
      return result;
    }

    private int compareWithPadding(int length, ByteBuffer b, int p, int paddingByte) {
      for (int i = p, n = p + length; i < n; ++i) {
        int result = toUnsigned(b.get(i)) - paddingByte;
        if (result != 0) {
          return result;
        }
      }
      return 0;
    }

    private int compare(int length, ByteBuffer b1, int p1, ByteBuffer b2, int p2) {
      for (int i = 0; i < length; ++i) {
        int result = toUnsigned(b1.get(p1 + i)) - toUnsigned(b2.get(p2 + i));
        if (result != 0) {
          return result;
        }
      }
      return 0;
    }

    @Override
    public String toString() {
      return "BINARY_AS_SIGNED_INTEGER_COMPARATOR";
    }
  };

  /**
   * This comparator is for comparing two float16 values represented in 2 bytes binary.
   */
  static final PrimitiveComparator<Binary> BINARY_AS_FLOAT16_COMPARATOR = new BinaryComparator() {

    @Override
    int compareBinary(Binary b1, Binary b2) {
      return Float16.compare(b1.get2BytesLittleEndian(), b2.get2BytesLittleEndian());
    }

    @Override
    public String toString() {
      return "BINARY_AS_FLOAT16_COMPARATOR";
    }
  };
}
