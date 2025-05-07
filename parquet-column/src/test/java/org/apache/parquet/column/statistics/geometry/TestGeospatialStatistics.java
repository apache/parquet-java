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
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;

public class TestGeospatialStatistics {

  @Test
  public void testAddGeospatialData() throws ParseException {
    PrimitiveType type = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.geometryType(null))
        .named("a");
    GeospatialStatistics.Builder builder = GeospatialStatistics.newBuilder(type);
    WKTReader wktReader = new WKTReader();
    WKBWriter wkbWriter = new WKBWriter();
    // Convert Geometry to WKB and update the builder
    builder.update(Binary.fromConstantByteArray(wkbWriter.write(wktReader.read("POINT (1 1)"))));
    builder.update(Binary.fromConstantByteArray(wkbWriter.write(wktReader.read("POINT (2 2)"))));
    GeospatialStatistics statistics = builder.build();
    Assert.assertTrue(statistics.isValid());
    Assert.assertNotNull(statistics.getBoundingBox());
    Assert.assertNotNull(statistics.getGeospatialTypes());
  }

  @Test
  public void testMergeGeospatialStatistics() throws ParseException {
    PrimitiveType type = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.geometryType(null))
        .named("a");

    WKTReader wktReader = new WKTReader();
    WKBWriter wkbWriter = new WKBWriter();

    GeospatialStatistics.Builder builder1 = GeospatialStatistics.newBuilder(type);
    builder1.update(Binary.fromConstantByteArray(wkbWriter.write(wktReader.read("POINT (1 1)"))));
    GeospatialStatistics statistics1 = builder1.build();

    GeospatialStatistics.Builder builder2 = GeospatialStatistics.newBuilder(type);
    builder2.update(Binary.fromConstantByteArray(wkbWriter.write(wktReader.read("POINT (2 2)"))));
    GeospatialStatistics statistics2 = builder2.build();

    statistics1.merge(statistics2);
    Assert.assertTrue(statistics1.isValid());
    Assert.assertNotNull(statistics1.getBoundingBox());
    Assert.assertNotNull(statistics1.getGeospatialTypes());
  }

  @Test
  public void testCopyGeospatialStatistics() {
    PrimitiveType type = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.geometryType(null))
        .named("a");
    GeospatialStatistics.Builder builder = GeospatialStatistics.newBuilder(type);
    builder.update(Binary.fromString("POINT (1 1)"));
    GeospatialStatistics statistics = builder.build();
    GeospatialStatistics copy = statistics.copy();
    Assert.assertTrue(copy.isValid());
    Assert.assertNotNull(copy.getBoundingBox());
    Assert.assertNotNull(copy.getGeospatialTypes());
  }

  @Test
  public void testInvalidGeometryMakesStatisticsInvalid() {
    PrimitiveType type = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.geometryType(null))
        .named("a");
    GeospatialStatistics.Builder builder = GeospatialStatistics.newBuilder(type);

    // First add a valid geometry
    WKTReader wktReader = new WKTReader();
    WKBWriter wkbWriter = new WKBWriter();
    try {
      builder.update(Binary.fromConstantByteArray(wkbWriter.write(wktReader.read("POINT (1 1)"))));
    } catch (ParseException e) {
      Assert.fail("Failed to parse valid WKT: " + e.getMessage());
    }

    // Valid at this point
    GeospatialStatistics validStats = builder.build();
    Assert.assertTrue(validStats.isValid());

    // Now add invalid data - corrupt WKB bytes
    byte[] invalidBytes = new byte[] {0x01, 0x02, 0x03}; // Invalid WKB format
    builder.update(Binary.fromConstantByteArray(invalidBytes));

    // After adding invalid data, statistics should be invalidated
    GeospatialStatistics invalidStats = builder.build();
    Assert.assertFalse(invalidStats.isValid());

    // The builder should have called abort() internally when parsing failed
    Assert.assertFalse(invalidStats.getBoundingBox().isValid());
    Assert.assertFalse(invalidStats.getGeospatialTypes().isValid());
  }

  @Test
  public void testNoopBuilder() {
    GeospatialStatistics.Builder builder = GeospatialStatistics.noopBuilder();
    GeospatialStatistics statistics = builder.build();
    Assert.assertFalse(statistics.isValid());
  }
}
