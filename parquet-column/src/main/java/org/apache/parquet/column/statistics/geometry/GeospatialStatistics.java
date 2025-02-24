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

import org.apache.parquet.Preconditions;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

/**
 * A structure for capturing metadata for estimating the unencoded,
 * uncompressed size of geospatial data written.
 */
public class GeospatialStatistics {

  public static final String DEFAULT_GEOSPATIAL_STAT_CRS = "OGC:CRS84";
  private static final BoundingBox DUMMY_BOUNDING_BOX = new DummyBoundingBox();

  // Metadata that may impact the statistics calculation
  private final String crs;

  private final BoundingBox boundingBox;
  private final EdgeInterpolationAlgorithm edgeAlgorithm;
  private final GeospatialTypes geospatialTypes;
  //   private final WKBReader reader = new WKBReader();

  /**
   * Whether the statistics has valid value.
   *
   * It is true by default. Only set to false while it fails to merge statistics.
   */
  private boolean valid = true;

  public void mergeStatistics(GeospatialStatistics other) {
    if (other == null) {
      return;
    }
    this.boundingBox.merge(other.boundingBox);
    this.geospatialTypes.merge(other.geospatialTypes);
  }

  /**
   * Builder to create a GeospatialStatistics.
   */
  public static class Builder {
    private final String crs;
    private BoundingBox boundingBox;
    private GeospatialTypes geospatialTypes;
    private EdgeInterpolationAlgorithm edgeAlgorithm;
    private final WKBReader reader = new WKBReader();

    /**
     * Create a builder to create a GeospatialStatistics.
     * For Geometry type, edgeAlgorithm is not required.
     *
     * @param crs the coordinate reference system
     */
    public Builder(String crs) {
      this.crs = crs;
      this.boundingBox = new BoundingBox();
      this.geospatialTypes = new GeospatialTypes();
      this.edgeAlgorithm = null;
    }

    /**
     * Create a builder to create a GeospatialStatistics.
     * For Geography type, optional edgeAlgorithm can be set.
     *
     * @param crs the coordinate reference system
     */
    public Builder(String crs, EdgeInterpolationAlgorithm edgeAlgorithm) {
      this.crs = crs;
      this.boundingBox = new BoundingBox();
      this.geospatialTypes = new GeospatialTypes();
      this.edgeAlgorithm = edgeAlgorithm;
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
      boundingBox.update(geom, crs);
      geospatialTypes.update(geom);
    }

    public void abort() {
      boundingBox.abort();
      geospatialTypes.abort();
    }

    /**
     * Build a GeospatialStatistics from the builder.
     *
     * @return a new GeospatialStatistics object
     */
    public GeospatialStatistics build() {
      return new GeospatialStatistics(crs, boundingBox, geospatialTypes, edgeAlgorithm);
    }
  }

  /**
   * Create a new GeospatialStatistics builder with the specified CRS.
   *
   * @param type the primitive type
   * @return a new GeospatialStatistics builder
   */
  public static GeospatialStatistics.Builder newBuilder(PrimitiveType type) {
    LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
    if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.GeometryLogicalTypeAnnotation) {
      String crs = ((LogicalTypeAnnotation.GeometryLogicalTypeAnnotation) logicalTypeAnnotation).getCrs();
      return new GeospatialStatistics.Builder(crs);
    } else if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.GeographyLogicalTypeAnnotation) {
      String crs = ((LogicalTypeAnnotation.GeographyLogicalTypeAnnotation) logicalTypeAnnotation).getCrs();
      EdgeInterpolationAlgorithm edgeAlgorithm =
          ((LogicalTypeAnnotation.GeographyLogicalTypeAnnotation) logicalTypeAnnotation).getEdgeAlgorithm();
      return new GeospatialStatistics.Builder(crs, edgeAlgorithm);
    } else {
      return noopBuilder();
    }
  }

  /**
   * Constructs a GeospatialStatistics object with the specified CRS, bounding box, and geospatial types.
   *
   * @param crs the coordinate reference system
   * @param boundingBox the bounding box for the geospatial data
   * @param geospatialTypes the geospatial types
   */
  public GeospatialStatistics(
      String crs,
      BoundingBox boundingBox,
      GeospatialTypes geospatialTypes,
      EdgeInterpolationAlgorithm edgeAlgorithm) {
    this.crs = crs;
    this.boundingBox = boundingBox;
    this.geospatialTypes = geospatialTypes;
    this.edgeAlgorithm = edgeAlgorithm;
  }

  /**
   * Constructs a GeospatialStatistics object with the specified CRS.
   *
   * @param crs the coordinate reference system
   */
  public GeospatialStatistics(String crs) {
    this(crs, new BoundingBox(), new GeospatialTypes(), null);
  }

  /**
   * Constructs a GeospatialStatistics object with the specified CRS and edge interpolation algorithm.
   *
   * @param crs the coordinate reference system
   * @param edgeAlgorithm the edge interpolation algorithm
   */
  public GeospatialStatistics(String crs, EdgeInterpolationAlgorithm edgeAlgorithm) {
    this.crs = crs;
    this.boundingBox = DUMMY_BOUNDING_BOX;
    this.geospatialTypes = new GeospatialTypes();
    this.edgeAlgorithm = edgeAlgorithm;
  }

  /** Returns the coordinate reference system. */
  public BoundingBox getBoundingBox() {
    return boundingBox;
  }

  /** Returns the geometry types. */
  public GeospatialTypes getGeospatialTypes() {
    return geospatialTypes;
  }

  /**
   * @return whether the statistics has valid value.
   */
  public boolean isValid() {
    return valid;
  }

  public void merge(GeospatialStatistics other) {
    if (!valid) return;
    Preconditions.checkArgument(other != null, "Cannot merge with null GeometryStatistics");

    if (boundingBox != null && other.boundingBox != null) {
      boundingBox.merge(other.boundingBox);
    }

    if (geospatialTypes != null && other.geospatialTypes != null) {
      geospatialTypes.merge(other.geospatialTypes);
    }
  }

  // Copy the statistics
  public GeospatialStatistics copy() {
    return new GeospatialStatistics(
        crs,
        boundingBox != null ? boundingBox.copy() : null,
        geospatialTypes != null ? geospatialTypes.copy() : null,
        null);
  }

  @Override
  public String toString() {
    return "GeospatialStatistics{" + "boundingBox=" + boundingBox + ", coverings=" + geospatialTypes + '}';
  }

  /**
   * Creates a no-op geospatial statistics builder that collects no data.
   * Used when geospatial statistics collection is disabled.
   */
  private static class NoopBuilder extends Builder {
    private final String crs;

    private NoopBuilder(String crs) {
      super(crs);
      this.crs = crs;
    }

    @Override
    public GeospatialStatistics build() {
      GeospatialStatistics stats = new GeospatialStatistics(crs, null, null, null);
      stats.valid = false; // Mark as invalid since this is a noop builder
      return stats;
    }
  }

  /**
   * Creates a builder that doesn't collect any statistics.
   */
  public static Builder noopBuilder(String crs) {
    return new NoopBuilder(crs);
  }

  /**
   * Creates a builder that doesn't collect any statistics.
   */
  public static Builder noopBuilder() {
    return new NoopBuilder(DEFAULT_GEOSPATIAL_STAT_CRS);
  }
}
