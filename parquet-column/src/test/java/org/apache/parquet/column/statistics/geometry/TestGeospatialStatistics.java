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

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestGeospatialStatistics {

  @Test
  public void testAddGeospatialData() {
    PrimitiveType type = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.geometryType())
        .named("a");
    GeospatialStatistics.Builder builder = GeospatialStatistics.newBuilder(type);
    GeospatialStatistics statistics = builder.build();
    statistics.update(Binary.fromString("POINT (1 1)"));
    statistics.update(Binary.fromString("POINT (2 2)"));
    Assert.assertTrue(statistics.isValid());
    Assert.assertNotNull(statistics.getBoundingBox());
    Assert.assertNotNull(statistics.getGeospatialTypes());
  }

  @Test
  public void testMergeGeospatialStatistics() {
    PrimitiveType type = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.geometryType())
        .named("a");
    GeospatialStatistics.Builder builder1 = GeospatialStatistics.newBuilder(type);
    GeospatialStatistics statistics1 = builder1.build();
    statistics1.update(Binary.fromString("POINT (1 1)"));

    GeospatialStatistics.Builder builder2 = GeospatialStatistics.newBuilder(type);
    GeospatialStatistics statistics2 = builder2.build();
    statistics2.update(Binary.fromString("POINT (2 2)"));

    statistics1.merge(statistics2);
    Assert.assertTrue(statistics1.isValid());
    Assert.assertNotNull(statistics1.getBoundingBox());
    Assert.assertNotNull(statistics1.getGeospatialTypes());
  }

  @Test
  public void testCopyGeospatialStatistics() {
    PrimitiveType type = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.geometryType())
        .named("a");
    GeospatialStatistics.Builder builder = GeospatialStatistics.newBuilder(type);
    GeospatialStatistics statistics = builder.build();
    statistics.update(Binary.fromString("POINT (1 1)"));
    GeospatialStatistics copy = statistics.copy();
    Assert.assertTrue(copy.isValid());
    Assert.assertNotNull(copy.getBoundingBox());
    Assert.assertNotNull(copy.getGeospatialTypes());
  }

  @Test
  public void testNoopBuilder() {
    GeospatialStatistics.Builder builder = GeospatialStatistics.noopBuilder("EPSG:4326");
    GeospatialStatistics statistics = builder.build();
    Assert.assertFalse(statistics.isValid());
  }
}
