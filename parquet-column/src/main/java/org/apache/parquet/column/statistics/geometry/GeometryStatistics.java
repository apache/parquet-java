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
import java.util.Arrays;
import java.util.List;
import org.apache.parquet.Preconditions;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

public class GeometryStatistics {

  // Metadata that may impact the statistics calculation
  private final LogicalTypeAnnotation.Edges edges;
  private final String crs;
  private final ByteBuffer metadata;

  private final BoundingBox boundingBox;
  private final List<Covering> coverings;
  private final GeometryTypes geometryTypes;
  private final WKBReader reader = new WKBReader();

  public GeometryStatistics(
      LogicalTypeAnnotation.Edges edges,
      String crs,
      ByteBuffer metadata,
      BoundingBox boundingBox,
      List<Covering> coverings,
      GeometryTypes geometryTypes) {
    this.edges = edges;
    this.crs = crs;
    this.metadata = metadata;
    this.boundingBox = supportsBoundingBox() ? boundingBox : null;
    this.coverings = supportsCovering() ? coverings : null;
    this.geometryTypes = geometryTypes;
  }

  public GeometryStatistics(LogicalTypeAnnotation.Edges edges, String crs, ByteBuffer metadata) {
    this(
        edges,
        crs,
        metadata,
        new BoundingBox(),
        Arrays.asList(new EnvelopeCovering(edges, crs)),
        new GeometryTypes());
  }

  public BoundingBox getBoundingBox() {
    return boundingBox;
  }

  public List<Covering> getCoverings() {
    return coverings;
  }

  public GeometryTypes getGeometryTypes() {
    return geometryTypes;
  }

  public void update(Binary value) {
    if (value == null) {
      return;
    }
    try {
      Geometry geom = reader.read(value.getBytes());
      update(geom);
    } catch (ParseException e) {
      abort();
    }
  }

  private void update(Geometry geom) {
    if (supportsBoundingBox()) {
      boundingBox.update(geom);
    }
    if (supportsCovering()) {
      coverings.stream().forEach(c -> c.update(geom));
    }
    geometryTypes.update(geom);
  }

  /**
   * A bounding box is a rectangular region defined by two points, the lower left
   * and upper right corners. It is used to represent the minimum and maximum
   * coordinates of a geometry. Only planar geometries can have a bounding box.
   */
  private boolean supportsBoundingBox() {
    // Only planar geometries can have a bounding box
    // based on the current specification
    return edges == LogicalTypeAnnotation.Edges.PLANAR;
  }

  /**
   * A custom WKB-encoded polygon or multi-polygon to represent a covering of
   * geometries. For example, it may be a bounding box, or an envelope of geometries
   * when a bounding box cannot be built (e.g. a geometry has spherical edges, or if
   * an edge of geographic coordinates crosses the antimeridian). In addition, it can
   * also be used to provide vendor-agnostic coverings like S2 or H3 grids.
   */
  private boolean supportsCovering() {
    // This version (POC) assumes only build coverings for planar edges
    // In case of spherical edges, no coverings are built
    //     return edges == LogicalTypeAnnotation.Edges.PLANAR;
    return true;
  }

  public void merge(GeometryStatistics other) {
    Preconditions.checkArgument(other != null, "Cannot merge with null GeometryStatistics");
    Preconditions.checkArgument(coverings.size() == other.coverings.size(), "Coverings size must be the same");

    boundingBox.merge(other.boundingBox);
    for (int i = 0; i < coverings.size(); i++) {
      coverings.get(i).merge(other.coverings.get(i));
    }
    geometryTypes.merge(other.geometryTypes);
  }

  public void reset() {
    boundingBox.reset();
    coverings.stream().forEach(c -> c.reset());
    geometryTypes.reset();
  }

  public void abort() {
    boundingBox.abort();
    coverings.stream().forEach(c -> c.abort());
    geometryTypes.abort();
  }

  // Copy the statistics
  public GeometryStatistics copy() {
    return new GeometryStatistics(
        edges,
        crs,
        metadata,
        boundingBox != null ? boundingBox.copy() : null,
        coverings != null ? coverings : null,
        geometryTypes != null ? geometryTypes.copy() : null);
  }

  @Override
  public String toString() {
    return "GeometryStatistics{" + "boundingBox="
        + boundingBox + ", coverings="
        + coverings + ", geometryTypes="
        + geometryTypes + '}';
  }
}
