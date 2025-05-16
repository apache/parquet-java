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

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A structure for capturing metadata for estimating the unencoded,
 * uncompressed size of geospatial data written.
 */
public class GeospatialStatistics {
  private static final Logger LOG = LoggerFactory.getLogger(GeospatialStatistics.class);

  private BoundingBox boundingBox;
  private GeospatialTypes geospatialTypes;

  /**
   * Builder to create a GeospatialStatistics.
   */
  public static class Builder {
    private BoundingBox boundingBox;
    private GeospatialTypes geospatialTypes;
    private final WKBReader reader = new WKBReader();

    /**
     * Create a builder to create a GeospatialStatistics.
     */
    public Builder() {
      this.boundingBox = new BoundingBox();
      this.geospatialTypes = new GeospatialTypes();
    }

    public void update(Binary value) {
      if (value == null) {
        return;
      }
      try {
        Geometry geom = reader.read(value.getBytes());
        update(geom);
      } catch (ParseException e) {
        LOG.warn("Failed to parse WKB geometry, omit it from stats", e);
      }
    }

    private void update(Geometry geom) {
      boundingBox.update(geom);
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
      return new GeospatialStatistics(boundingBox, geospatialTypes);
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
      return new GeospatialStatistics.Builder();
    } else if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.GeographyLogicalTypeAnnotation) {
      // For Geography type, we have not implemented the algorithm yet.
      return noopBuilder();
    } else {
      return noopBuilder();
    }
  }

  /**
   * Constructs a GeospatialStatistics object with the specified CRS, bounding box, and geospatial types.
   *
   * @param boundingBox the bounding box for the geospatial data, or null if not applicable, note that
   *    - The bounding box (bbox) is omitted only if there are no X or Y values.
   *    - The Z and/or M statistics are omitted only if there are no Z and/or M values, respectively.
   * @param geospatialTypes the geospatial types
   */
  public GeospatialStatistics(BoundingBox boundingBox, GeospatialTypes geospatialTypes) {
    this.boundingBox = boundingBox;
    this.geospatialTypes = geospatialTypes;
  }

  /**
   * Constructs a GeospatialStatistics object with the specified CRS.
   */
  public GeospatialStatistics() {
    this(new BoundingBox(), new GeospatialTypes());
  }

  /**
   * Constructs a GeospatialStatistics object with the specified CRS.
   *
   * @param crs the coordinate reference system
   */
  public GeospatialStatistics(String crs) {
    this.boundingBox = new BoundingBox();
    this.geospatialTypes = new GeospatialTypes();
  }

  /** Returns the bounding box. */
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
    return (boundingBox != null && boundingBox.isValid()) || (geospatialTypes != null && geospatialTypes.isValid());
  }

  public void merge(GeospatialStatistics other) {
    if (boundingBox != null) {
      boundingBox.merge(other.boundingBox);
    }
    if (geospatialTypes != null) {
      geospatialTypes.merge(other.geospatialTypes);
    }
  }

  private void abort() {
    if (boundingBox != null) {
      boundingBox.abort();
    }
    if (geospatialTypes != null) {
      geospatialTypes.abort();
    }
  }

  // Copy the statistics
  public GeospatialStatistics copy() {
    return new GeospatialStatistics(
        boundingBox != null ? boundingBox.copy() : null,
        geospatialTypes != null ? geospatialTypes.copy() : null);
  }

  @Override
  public String toString() {
    return "GeospatialStatistics{" + "boundingBox=" + boundingBox + ", geospatialTypes=" + geospatialTypes + '}';
  }

  /**
   * Creates a no-op geospatial statistics builder that collects no data.
   * Used when geospatial statistics collection is disabled.
   */
  private static class NoopBuilder extends Builder {
    private NoopBuilder() {}

    @Override
    public GeospatialStatistics build() {
      return new GeospatialStatistics(null, null);
    }

    @Override
    public void update(Binary value) {
      // do nothing
    }

    @Override
    public void abort() {
      // do nothing
    }
  }

  /**
   * Creates a builder that doesn't collect any statistics.
   */
  public static Builder noopBuilder() {
    return new NoopBuilder();
  }
}
