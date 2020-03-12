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

import java.util.Objects;

/**
 * Class representing the column order with all the related parameters.
 */
public class ColumnOrder {
  /**
   * The enum type of the column order.
   */
  public enum ColumnOrderName {
    /**
     * Representing the case when the defined column order is undefined (e.g. the file is written by a later API and the
     * current one does not support the related column order). No statistics will be written/read in this case.
     */
    UNDEFINED,
    /**
     * Type defined order meaning that the comparison order of the elements are based on its type.
     */
    TYPE_DEFINED_ORDER
  }

  private static final ColumnOrder UNDEFINED_COLUMN_ORDER = new ColumnOrder(ColumnOrderName.UNDEFINED);
  private static final ColumnOrder TYPE_DEFINED_COLUMN_ORDER = new ColumnOrder(ColumnOrderName.TYPE_DEFINED_ORDER);

  /**
   * @return a {@link ColumnOrder} instance representing an undefined order
   * @see ColumnOrderName#UNDEFINED
   */
  public static ColumnOrder undefined() {
    return UNDEFINED_COLUMN_ORDER;
  }

  /**
   * @return a {@link ColumnOrder} instance representing a type defined order
   * @see ColumnOrderName#TYPE_DEFINED_ORDER
   */
  public static ColumnOrder typeDefined() {
    return TYPE_DEFINED_COLUMN_ORDER;
  }

  private final ColumnOrderName columnOrderName;

  private ColumnOrder(ColumnOrderName columnOrderName) {
    this.columnOrderName = Objects.requireNonNull(columnOrderName, "columnOrderName cannot be null");
  }

  public ColumnOrderName getColumnOrderName() {
    return columnOrderName;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ColumnOrder) {
      return columnOrderName == ((ColumnOrder) obj).columnOrderName;
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return columnOrderName.hashCode();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return columnOrderName.toString();
  }
}
