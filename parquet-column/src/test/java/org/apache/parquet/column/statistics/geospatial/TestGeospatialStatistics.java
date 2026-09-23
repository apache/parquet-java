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

package org.apache.parquet.column.statistics.geospatial;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
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
    assertThat(statistics.isValid()).isTrue();
    assertThat(statistics.getBoundingBox()).isNotNull();
    assertThat(statistics.getGeospatialTypes()).isNotNull();
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
    assertThat(statistics1.isValid()).isTrue();
    assertThat(statistics1.getBoundingBox()).isNotNull();
    assertThat(statistics1.getGeospatialTypes()).isNotNull();
  }

  @Test
  public void testMergeNullGeospatialStatistics() throws ParseException {
    // Create a valid stats object
    PrimitiveType type = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.geometryType(null))
        .named("a");

    WKTReader wktReader = new WKTReader();
    WKBWriter wkbWriter = new WKBWriter();

    GeospatialStatistics.Builder validBuilder = GeospatialStatistics.newBuilder(type);
    validBuilder.update(Binary.fromConstantByteArray(wkbWriter.write(wktReader.read("POINT (1 1)"))));
    GeospatialStatistics validStats = validBuilder.build();
    assertThat(validStats.isValid()).isTrue();

    // Create stats with null components
    GeospatialStatistics nullStats = new GeospatialStatistics(null, null);
    assertThat(nullStats.isValid()).isFalse();

    // Test merging valid with null
    GeospatialStatistics validCopy = validStats.copy();
    validCopy.merge(nullStats);
    assertThat(validCopy.isValid()).isFalse();
    assertThat(validCopy.getBoundingBox()).isNotNull();
    assertThat(validCopy.getGeospatialTypes()).isNotNull();

    // Test merging null with valid
    nullStats = new GeospatialStatistics(null, null);
    nullStats.merge(validStats);
    assertThat(nullStats.isValid()).isFalse();
    assertThat(nullStats.getBoundingBox()).isNull();
    assertThat(nullStats.getGeospatialTypes()).isNull();

    // Create stats with null bounding box only
    GeospatialStatistics nullBboxStats = new GeospatialStatistics(null, new GeospatialTypes());
    assertThat(nullBboxStats.isValid()).isTrue();

    // Test merging valid with null bounding box
    validCopy = validStats.copy();
    validCopy.merge(nullBboxStats);
    assertThat(validCopy.isValid()).isTrue();
    assertThat(validCopy.getBoundingBox()).isNotNull();
    assertThat(validCopy.getGeospatialTypes()).isNotNull();
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
    assertThat(copy.isValid()).isTrue();
    assertThat(copy.getBoundingBox()).isNotNull();
    assertThat(copy.getGeospatialTypes()).isNotNull();
  }

  @Test
  public void testInvalidGeometryMakesStatisticsInvalid() throws ParseException {
    PrimitiveType type = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.geometryType(null))
        .named("a");
    GeospatialStatistics.Builder builder = GeospatialStatistics.newBuilder(type);

    // First add a valid geometry
    WKTReader wktReader = new WKTReader();
    WKBWriter wkbWriter = new WKBWriter();
    builder.update(Binary.fromConstantByteArray(wkbWriter.write(wktReader.read("POINT (1 1)"))));

    // Valid at this point
    GeospatialStatistics validStats = builder.build();
    assertThat(validStats.isValid()).isTrue();

    // Now add invalid data - corrupt WKB bytes
    byte[] invalidBytes = new byte[] {0x01, 0x02, 0x03}; // Invalid WKB format
    builder.update(Binary.fromConstantByteArray(invalidBytes));

    // After adding invalid data, omit it from stats
    GeospatialStatistics invalidStats = builder.build();
    assertThat(invalidStats.isValid()).isTrue();
  }

  @Test
  public void testNoopBuilder() {
    GeospatialStatistics.Builder builder = GeospatialStatistics.noopBuilder();
    GeospatialStatistics statistics = builder.build();
    assertThat(statistics.isValid()).isFalse();
  }

  @Test
  public void testNoopBuilderIsSingleton() {
    GeospatialStatistics.Builder builder1 = GeospatialStatistics.noopBuilder();
    GeospatialStatistics.Builder builder2 = GeospatialStatistics.noopBuilder();
    Assert.assertSame("noopBuilder() should return the same instance", builder1, builder2);
    Assert.assertSame("build() should return the same instance", builder1.build(), builder2.build());
  }

  @Test
  public void testNoopBuilderUpdateAndAbortAreNoOps() {
    GeospatialStatistics.Builder builder = GeospatialStatistics.noopBuilder();

    // update with non-null value should not throw
    builder.update(Binary.fromConstantByteArray(new byte[] {0x01, 0x02, 0x03}));
    // update with null should not throw
    builder.update(null);
    // abort should not throw
    builder.abort();

    // builder still produces an invalid (empty) result
    GeospatialStatistics statistics = builder.build();
    Assert.assertFalse(statistics.isValid());
    Assert.assertNull(statistics.getBoundingBox());
    Assert.assertNull(statistics.getGeospatialTypes());
  }
}
