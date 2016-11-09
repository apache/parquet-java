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
package org.apache.parquet.arrow.schema;

import static org.apache.parquet.schema.Type.Repetition.REPEATED;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;

/**
 * Represents a standard 3 levels Parquet list
 * (can be null, can contain nulls)
 * - optional list
 * - repeated content
 * - optional element
 */
class List3Levels {
  private final GroupType list;
  private final GroupType repeated;
  private final Type element;

  /**
   * Will validate the structure of the list
   * @param list the Parquet List
   */
  public List3Levels(GroupType list) {
    if (list.getOriginalType() != OriginalType.LIST || list.getFields().size() != 1) {
      throw new IllegalArgumentException("invalid list type: " + list);
    }
    this.list = list;
    Type repeatedField = list.getFields().get(0);
    if (repeatedField.isPrimitive() || !repeatedField.isRepetition(REPEATED) || repeatedField.asGroupType().getFields().size() != 1) {
      throw new IllegalArgumentException("invalid list type: " + list);
    }
    this.repeated = repeatedField.asGroupType();
    this.element = repeated.getFields().get(0);
  }

  /**
   * @return the root list element (an optional group with one child)
   */
  public GroupType getList() {
    return list;
  }

  /**
   * @return repeated level, single child of list
   */
  public GroupType getRepeated() {
    return repeated;
  }

  /**
   * @return the element level, single child of repeated.
   */
  public Type getElement() {
    return element;
  }

}
