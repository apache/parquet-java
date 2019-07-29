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

/**
 * @deprecated use {@link org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation}
 * with proper precision and scale parameters instead
 */
@Deprecated
public class DecimalMetadata {
  private final int precision;
  private final int scale;

  public DecimalMetadata(int precision, int scale) {
    this.precision = precision;
    this.scale = scale;
  }

  public int getPrecision() {
    return precision;
  }

  public int getScale() {
    return scale;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DecimalMetadata that = (DecimalMetadata) o;

    if (precision != that.precision) return false;
    if (scale != that.scale) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = precision;
    result = 31 * result + scale;
    return result;
  }
}
