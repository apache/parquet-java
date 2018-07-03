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
package org.apache.parquet.internal.column.columnindex;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.function.IntPredicate;
import java.util.function.IntUnaryOperator;

/**
 * Iterator implementation for page indexes.
 */
class IndexIterator implements PrimitiveIterator.OfInt {
  private int index;
  private final int length;
  private final IntPredicate filter;

  static PrimitiveIterator.OfInt all(int pageCount) {
    return new IndexIterator(pageCount, i -> true);
  }

  static PrimitiveIterator.OfInt filter(int pageCount, IntPredicate filter) {
    return new IndexIterator(pageCount, filter);
  }

  static PrimitiveIterator.OfInt filterTranslate(int arrayLength, IntPredicate filter, IntUnaryOperator translator) {
    return new IndexIterator(arrayLength, filter) {
      @Override
      public int nextInt() {
        return translator.applyAsInt(super.nextInt());
      }
    };
  }

  private IndexIterator(int pageCount, IntPredicate filter) {
    this.length = pageCount;
    this.filter = filter;
    index = nextPageIndex(0);
  }

  private int nextPageIndex(int startIndex) {
    for (int i = startIndex; i < length; ++i) {
      if (filter.test(i)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public boolean hasNext() {
    return index >= 0;
  }

  @Override
  public int nextInt() {
    if (hasNext()) {
      int ret = index;
      index = nextPageIndex(index + 1);
      return ret;
    }
    throw new NoSuchElementException();
  }
}
