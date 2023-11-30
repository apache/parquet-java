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

import static junit.framework.Assert.assertEquals;

import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

public class TestColumnDescriptor {

  private ColumnDescriptor column(String... path) {
    return new ColumnDescriptor(path, PrimitiveType.PrimitiveTypeName.INT32, 0, 0);
  }

  @Test
  public void testComparesTo() throws Exception {
    assertEquals(column("a").compareTo(column("a")), 0);
    assertEquals(column("a", "b").compareTo(column("a", "b")), 0);

    assertEquals(column("a").compareTo(column("b")), -1);
    assertEquals(column("b").compareTo(column("a")), 1);
    assertEquals(column("a", "a").compareTo(column("a", "b")), -1);
    assertEquals(column("b", "a").compareTo(column("a", "a")), 1);

    assertEquals(column("a").compareTo(column("a", "b")), -1);
    assertEquals(column("b").compareTo(column("a", "b")), 1);

    assertEquals(column("a", "b").compareTo(column("a")), 1);
    assertEquals(column("a", "b").compareTo(column("b")), -1);

    assertEquals(column("").compareTo(column("")), 0);
    assertEquals(column("").compareTo(column("a")), -1);
    assertEquals(column("a").compareTo(column("")), 1);
  }
}
