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
package org.apache.parquet.column;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

public class TestColumnDescriptor {

  private ColumnDescriptor column(String... path) {
    return new ColumnDescriptor(path, PrimitiveType.PrimitiveTypeName.INT32, 0, 0);
  }

  @Test
  public void testComparesTo() {
    assertThat(column("a")).isEqualByComparingTo(column("a"));
    assertThat(column("a", "b")).isEqualByComparingTo(column("a", "b"));

    assertThat(column("a")).isLessThan(column("b"));
    assertThat(column("b")).isGreaterThan(column("a"));
    assertThat(column("a", "a")).isLessThan(column("a", "b"));
    assertThat(column("b", "a")).isGreaterThan(column("a", "a"));

    assertThat(column("a")).isLessThan(column("a", "b"));
    assertThat(column("b")).isGreaterThan(column("a", "b"));

    assertThat(column("a", "b")).isGreaterThan(column("a")).isLessThan(column("b"));

    assertThat(column("")).isEqualByComparingTo(column("")).isLessThan(column("a"));
    assertThat(column("a")).isGreaterThan(column(""));
  }
}
