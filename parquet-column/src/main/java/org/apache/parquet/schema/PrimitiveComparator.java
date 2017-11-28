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

import org.apache.parquet.io.api.Binary;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * {@link Comparator} implementation that also supports the comparison of the related primitive type to avoid the
 * performance penalty of boxing/unboxing. The {@code compare} methods for the not supported primitive types throw
 * {@link UnsupportedOperationException}.
 */
public abstract class PrimitiveComparator<T> implements Comparator<T> {

  public int compare(boolean b1, boolean b2) {
    throw new UnsupportedOperationException("compare(boolean, boolean) was called on a non-boolean comparator");
  }

  public int compare(int i1, int i2) {
    throw new UnsupportedOperationException("compare(int, int) was called on a non-int comparator");
  }

  public int compare(long l1, long l2) {
    throw new UnsupportedOperationException("compare(long, long) was called on a non-long comparator");
  }

  public int compare(float f1, float f2) {
    throw new UnsupportedOperationException("compare(float, float) was called on a non-float comparator");
  }

  public int compare(double d1, double d2) {
    throw new UnsupportedOperationException("compare(double, double) was called on a non-double comparator");
  }

  static final PrimitiveComparator<Boolean> BOOLEAN_COMPARATOR = new PrimitiveComparator<Boolean>() {
    @Override
    public int compare(Boolean o1, Boolean o2) {
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

  private static abstract class IntComparator extends PrimitiveComparator<Integer> {
    @Override
    public final int compare(Integer o1, Integer o2) {
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

  private static abstract class LongComparator extends PrimitiveComparator<Long> {
    @Override
    public final int compare(Long o1, Long o2) {
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
    public int compare(Float o1, Float o2) {
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
    public int compare(Double o1, Double o2) {
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

  private static abstract class BinaryComparator extends PrimitiveComparator<Binary> {
    @Override
    public final int compare(Binary o1, Binary o2) {
      return compare(o1.toByteBuffer(), o2.toByteBuffer());
    }

    abstract int compare(ByteBuffer b1, ByteBuffer b2);

    final int toUnsigned(byte b) {
      return b & 0xFF;
    }
  }

  static final PrimitiveComparator<Binary> LEXICOGRAPHICAL_BINARY_COMPARATOR = new BinaryComparator() {
    @Override
    int compare(ByteBuffer b1, ByteBuffer b2) {
      int l1 = b1.remaining();
      int l2 = b2.remaining();
      int p1 = b1.position();
      int p2 = b2.position();
      int minL = Math.min(l1, l2);

      for (int i = 0; i < minL; ++i) {
        int result = unsignedCompare(b1.get(p1 + i), b2.get(p2 + i));
        if (result != 0) {
          return result;
        }
      }

      return l1 - l2;
    }

    private int unsignedCompare(byte b1, byte b2) {
      return toUnsigned(b1) - toUnsigned(b2);
    }

    @Override
    public String toString() {
      return "LEXICOGRAPHICAL_BINARY_COMPARATOR";
    }
  };

  static final PrimitiveComparator<Binary> SIGNED_BINARY_COMPARATOR = new BinaryComparator() {
    private static final int NEGATIVE_PREFIX = 0xFF;
    private static final int POSITIVE_PREFIX = 0;

    @Override
    int compare(ByteBuffer b1, ByteBuffer b2) {
      int l1 = b1.remaining();
      int l2 = b2.remaining();
      int p1 = b1.position();
      int p2 = b2.position();

      boolean isNegative1 = l1 > 0 ? b1.get(p1) < 0 : false;
      boolean isNegative2 = l2 > 0 ? b2.get(p2) < 0 : false;
      if (isNegative1 != isNegative2) {
        return isNegative1 ? -1 : 1;
      }

      int maxL = Math.max(l1, l2);
      int iDiff1 = maxL - l1;
      int iDiff2 = maxL - l2;
      int prefix = isNegative1 ? NEGATIVE_PREFIX : POSITIVE_PREFIX;
      for (int i = 0; i < maxL; ++i) {
        int value1 = i < iDiff1 ? prefix : toUnsigned(b1.get(p1 + i - iDiff1));
        int value2 = i < iDiff2 ? prefix : toUnsigned(b2.get(p2 + i - iDiff2));
        int result = value1 - value2;
        if (result != 0) {
          return result;
        }
      }
      return 0;
    }

    @Override
    public String toString() {
      return "SIGNED_BINARY_COMPARATOR";
    }
  };
}
