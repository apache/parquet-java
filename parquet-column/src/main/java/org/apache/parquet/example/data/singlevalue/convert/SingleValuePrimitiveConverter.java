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
package org.apache.parquet.example.data.singlevalue.convert;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

class SingleValuePrimitiveConverter extends PrimitiveConverter {

  private final SingleValueGroupConverter parent;
  private final int index;

  SingleValuePrimitiveConverter(SingleValueGroupConverter parent, int index) {
    this.parent = parent;
    this.index = index;
  }

  /**
   * {@inheritDoc}
   * @see PrimitiveConverter#addBinary(Binary)
   */
  @Override
  public void addBinary(Binary value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see PrimitiveConverter#addBoolean(boolean)
   */
  @Override
  public void addBoolean(boolean value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see PrimitiveConverter#addDouble(double)
   */
  @Override
  public void addDouble(double value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see PrimitiveConverter#addFloat(float)
   */
  @Override
  public void addFloat(float value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see PrimitiveConverter#addInt(int)
   */
  @Override
  public void addInt(int value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see PrimitiveConverter#addLong(long)
   */
  @Override
  public void addLong(long value) {
    parent.getCurrentRecord().add(index, value);
  }

}
