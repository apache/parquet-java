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
package org.apache.parquet.column.statistics.geometry;

import java.nio.ByteBuffer;
import org.apache.parquet.Preconditions;
import org.locationtech.jts.geom.Geometry;

public abstract class Covering {

  protected String kind;
  protected ByteBuffer value;

  public Covering(ByteBuffer value, String kind) {
    Preconditions.checkArgument(kind != null, "kind cannot be null");
    Preconditions.checkArgument(value != null, "value cannot be null");
    this.value = value;
    this.kind = kind;
  }

  public ByteBuffer getValue() {
    return value;
  }

  public void setValue(ByteBuffer value) {
    this.value = value;
  }

  public String getKind() {
    return kind;
  }

  public void setKind(String kind) {
    Preconditions.checkArgument(kind != null, "kind cannot be null");
    this.kind = kind;
  }

  void update(Geometry geom) {
    throw new UnsupportedOperationException(
        "Update is not supported for " + this.getClass().getSimpleName());
  }

  public void merge(Covering other) {
    throw new UnsupportedOperationException(
        "Merge is not supported for " + this.getClass().getSimpleName());
  }

  public void reset() {
    throw new UnsupportedOperationException(
        "Reset is not supported for " + this.getClass().getSimpleName());
  }

  public void abort() {
    throw new UnsupportedOperationException(
        "Abort is not supported for " + this.getClass().getSimpleName());
  }

  public abstract Covering copy();
}
