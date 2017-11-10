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

import java.util.Comparator;

/**
 * {@link Comparator} implementation that also supports the comparison of the related primitive type to avoid the
 * performance penalty of boxing/unboxing. The {@code compare} methods for the not supported primitive types throw
 * {@link UnsupportedOperationException}.
 */
public abstract class PrimitiveComparator<T> implements Comparator<T> {

  public int compare(boolean b1, boolean b2) {
    throw new UnsupportedOperationException();
  }

  public int compare(int i1, int i2) {
    throw new UnsupportedOperationException();
  }

  public int compare(long l1, long l2) {
    throw new UnsupportedOperationException();
  }

  public int compare(float f1, float f2) {
    throw new UnsupportedOperationException();
  }

  public int compare(double d1, double d2) {
    throw new UnsupportedOperationException();
  }

  static PrimitiveComparator<Boolean> BOOLEAN_COMPARATOR = new PrimitiveComparator<Boolean>() {
    @Override
    public int compare(Boolean o1, Boolean o2) {
      return compare(o1.booleanValue(), o2.booleanValue());
    }

    @Override
    public int compare(boolean b1, boolean b2) {
      return Boolean.compare(b1, b2);
    }
  };

  static PrimitiveComparator<Integer> SIGNED_INT32_COMPARATOR = new PrimitiveComparator<Integer>() {
    @Override
    public int compare(Integer o1, Integer o2) {
      return compare(o1.intValue(), o2.intValue());
    }

    @Override
    public int compare(int i1, int i2) {
      return Integer.compare(i1, i2);
    }
  };

  static PrimitiveComparator<Long> SIGNED_INT64_COMPARATOR = new PrimitiveComparator<Long>() {
    @Override
    public int compare(Long o1, Long o2) {
      return compare(o1.longValue(), o2.longValue());
    }

    @Override
    public int compare(long l1, long l2) {
      return Long.compare(l1, l2);
    }
  };

  static PrimitiveComparator<Float> FLOAT_COMPARATOR = new PrimitiveComparator<Float>() {
    @Override
    public int compare(Float o1, Float o2) {
      return compare(o1.floatValue(), o2.floatValue());
    }

    @Override
    public int compare(float f1, float f2) {
      return Float.compare(f1, f2);
    }
  };

  static PrimitiveComparator<Double> DOUBLE_COMPARATOR = new PrimitiveComparator<Double>() {
    @Override
    public int compare(Double o1, Double o2) {
      return compare(o1.doubleValue(), o2.doubleValue());
    }

    @Override
    public int compare(double d1, double d2) {
      return Double.compare(d1, d2);
    }
  };

  // TODO: this one is temporary as the self-comparison of Binary is not proper
  static PrimitiveComparator<Binary> BINARY_COMPARATOR = new PrimitiveComparator<Binary>() {
    @Override
    public int compare(Binary o1, Binary o2) {
      return o1.compareTo(o2);
    }
  };
}
