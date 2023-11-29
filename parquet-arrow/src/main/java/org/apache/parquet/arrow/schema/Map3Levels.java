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
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

/**
 * Represents a standard 3 levels Parquet map
 * - optional map
 * - repeated key_value
 * - required key, optional value
 */
class Map3Levels {
  private final GroupType map;
  private final GroupType repeated;
  private final Type key;
  private final Type value;

  /**
   * Will validate the structure of the map
   *
   * @param map the Parquet map
   */
  public Map3Levels(GroupType map) {
    if (map.getLogicalTypeAnnotation() != LogicalTypeAnnotation.mapType()
        || map.getFields().size() != 1) {
      throw new IllegalArgumentException("invalid map type: " + map);
    }
    this.map = map;
    Type repeatedField = map.getFields().get(0);
    if (repeatedField.isPrimitive()
        || !repeatedField.isRepetition(REPEATED)
        || repeatedField.asGroupType().getFields().size() != 2) {
      throw new IllegalArgumentException("invalid map key: " + map);
    }
    this.repeated = repeatedField.asGroupType();
    this.key = repeated.getFields().get(0);
    this.value = repeated.getFields().get(1);
  }

  /**
   * @return the root map element (an optional group with two children)
   */
  public GroupType getMap() {
    return map;
  }

  /**
   * @return repeated level, single child of map
   */
  public GroupType getRepeated() {
    return repeated;
  }

  /**
   * @return the key level
   */
  public Type getKey() {
    return key;
  }

  /**
   * @return the element level, single child of repeated.
   */
  public Type getValue() {
    return value;
  }
}
