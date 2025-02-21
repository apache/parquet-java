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
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

public class GeospatialStatistics {

  private static final BoundingBox DUMMY_BOUNDING_BOX = new DummyBoundingBox();

  // Metadata that may impact the statistics calculation
  private final String crs;

  private final BoundingBox boundingBox;
  private final EdgeInterpolationAlgorithm edgeAlgorithm;
  private final GeospatialTypes geospatialTypes;
  private final WKBReader reader = new WKBReader();

  public GeospatialStatistics(String crs, BoundingBox boundingBox, GeospatialTypes geospatialTypes) {
    this.crs = crs;
    this.boundingBox = supportsBoundingBox() ? boundingBox : DUMMY_BOUNDING_BOX;
    this.geospatialTypes = geospatialTypes;
    this.edgeAlgorithm = null;
  }

  public GeospatialStatistics(String crs) {
    this(crs, new BoundingBox(), new GeospatialTypes());
  }

  public GeospatialStatistics(String crs, EdgeInterpolationAlgorithm edgeAlgorithm) {
    this.crs = crs;
    this.boundingBox = DUMMY_BOUNDING_BOX;
    this.geospatialTypes = new GeospatialTypes();
    this.edgeAlgorithm = edgeAlgorithm;
  }

  public BoundingBox getBoundingBox() {
    return boundingBox;
  }

  public GeospatialTypes getGeometryTypes() {
    return geospatialTypes;
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
      boundingBox.update(geom, crs);
    }
    geospatialTypes.update(geom);
  }

  /**
   * A bounding box is a rectangular region defined by two points, the lower left
   * and upper right corners. It is used to represent the minimum and maximum
   * coordinates of a geometry. Only planar geometries can have a bounding box.
   */
  private boolean supportsBoundingBox() {
    // Only planar geometries can have a bounding box
    // based on the current specification
    return true;
  }

  public void merge(GeospatialStatistics other) {
    Preconditions.checkArgument(other != null, "Cannot merge with null GeometryStatistics");

    if (boundingBox != null && other.boundingBox != null) {
      boundingBox.merge(other.boundingBox);
    }

    if (geospatialTypes != null && other.geospatialTypes != null) {
      geospatialTypes.merge(other.geospatialTypes);
    }
  }

  public void reset() {
    boundingBox.reset();
    geospatialTypes.reset();
  }

  public void abort() {
    boundingBox.abort();
    geospatialTypes.abort();
  }

  // Copy the statistics
  public GeospatialStatistics copy() {
    return new GeospatialStatistics(
        crs,
        boundingBox != null ? boundingBox.copy() : null,
        geospatialTypes != null ? geospatialTypes.copy() : null);
  }

  @Override
  public String toString() {
    return "GeospatialStatistics{" + "boundingBox=" + boundingBox + ", coverings=" + geospatialTypes + '}';
  }
}
