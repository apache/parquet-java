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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.PrimitiveIterator;

import org.apache.parquet.filter2.predicate.FilterPredicate.Visitor;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexFilter;

/**
 * Column index containing min/max and null count values for the pages in a column chunk. It also implements methods of
 * {@link Visitor} to return the indexes of the matching pages. They are used by {@link ColumnIndexFilter}.
 *
 * @see org.apache.parquet.format.ColumnIndex
 */
public interface ColumnIndex extends Visitor<PrimitiveIterator.OfInt> {
  /**
   * @return the boundary order of the min/max values; used for converting to the related thrift object
   */
  public BoundaryOrder getBoundaryOrder();

  /**
   * @return the unmodifiable list of null counts; used for converting to the related thrift object
   */
  public List<Long> getNullCounts();

  /**
   * @return the unmodifiable list of null pages; used for converting to the related thrift object
   */
  public List<Boolean> getNullPages();

  /**
   * @return the list of the min values as {@link ByteBuffer}s; used for converting to the related thrift object
   */
  public List<ByteBuffer> getMinValues();

  /**
   * @return the list of the max values as {@link ByteBuffer}s; used for converting to the related thrift object
   */
  public List<ByteBuffer> getMaxValues();

}
