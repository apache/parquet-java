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
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

public class BoundingBox {

  private double xMin = Double.POSITIVE_INFINITY;
  private double xMax = Double.NEGATIVE_INFINITY;
  private double yMin = Double.POSITIVE_INFINITY;
  private double yMax = Double.NEGATIVE_INFINITY;
  private double zMin = Double.POSITIVE_INFINITY;
  private double zMax = Double.NEGATIVE_INFINITY;
  private double mMin = Double.POSITIVE_INFINITY;
  private double mMax = Double.NEGATIVE_INFINITY;

  public BoundingBox(
      double xMin, double xMax, double yMin, double yMax, double zMin, double zMax, double mMin, double mMax) {
    this.xMin = xMin;
    this.xMax = xMax;
    this.yMin = yMin;
    this.yMax = yMax;
    this.zMin = zMin;
    this.zMax = zMax;
    this.mMin = mMin;
    this.mMax = mMax;
  }

  public BoundingBox() {}

  public double getXMin() {
    return xMin;
  }

  public double getXMax() {
    return xMax;
  }

  public double getYMin() {
    return yMin;
  }

  public double getYMax() {
    return yMax;
  }

  public double getZMin() {
    return zMin;
  }

  public double getZMax() {
    return zMax;
  }

  public double getMMin() {
    return mMin;
  }

  public double getMMax() {
    return mMax;
  }

  /**
   * Updates the bounding box with the coordinates of the given geometry.
   * If the geometry is null, it will abort the update.
   * If the geometry is empty, it will keep the initial -Inf/Inf state for bounds.
   * If the geometry is valid, it will update the bounds accordingly.
   */
  void update(Geometry geometry, String crs) {
    if (geometry == null) {
      // If geometry is null, abort
      abort();
      return;
    }
    if (geometry.isEmpty()) {
      // For empty geometries, keep the initial -Inf/Inf state for bounds
      return;
    }

    if (shouldNormalizeLongitude(crs)) {
      GeospatialUtils.normalizeLongitude(geometry);
    }

    Envelope envelope = geometry.getEnvelopeInternal();
    double minX = envelope.getMinX();
    double maxX = envelope.getMaxX();
    double minY = envelope.getMinY();
    double maxY = envelope.getMaxY();

    // Initialize Z and M values
    double minZ = Double.POSITIVE_INFINITY;
    double maxZ = Double.NEGATIVE_INFINITY;
    double minM = Double.POSITIVE_INFINITY;
    double maxM = Double.NEGATIVE_INFINITY;

    // Find Z and M bounds from coordinates
    Coordinate[] coordinates = geometry.getCoordinates();
    for (Coordinate coord : coordinates) {
      if (!Double.isNaN(coord.getZ())) {
        minZ = Math.min(minZ, coord.getZ());
        maxZ = Math.max(maxZ, coord.getZ());
      }
      if (!Double.isNaN(coord.getM())) {
        minM = Math.min(minM, coord.getM());
        maxM = Math.max(maxM, coord.getM());
      }
    }

    // Use the consolidated updateBounds method for all dimensions
    updateBounds(minX, maxX, minY, maxY, minZ, maxZ, minM, maxM);
  }

  /**
   * Merges another bounding box into this one.
   * If the other bounding box is null, it will be ignored.
   */
  void merge(BoundingBox other) {
    Preconditions.checkArgument(other != null, "Cannot merge with null bounding box");

    // Use the updateBounds method to merge the bounds
    updateBounds(
        other.xMin, other.xMax,
        other.yMin, other.yMax,
        other.zMin, other.zMax,
        other.mMin, other.mMax);
  }

  /**
   * Resets the bounding box to its initial state.
   * All bounds will be set to -Inf/Inf.
   */
  public void reset() {
    xMin = Double.POSITIVE_INFINITY;
    xMax = Double.NEGATIVE_INFINITY;
    yMin = Double.POSITIVE_INFINITY;
    yMax = Double.NEGATIVE_INFINITY;
    zMin = Double.POSITIVE_INFINITY;
    zMax = Double.NEGATIVE_INFINITY;
    mMin = Double.POSITIVE_INFINITY;
    mMax = Double.NEGATIVE_INFINITY;
  }

  /**
   * Aborts the bounding box update.
   * All bounds will be set to NaN.
   */
  public void abort() {
    xMin = Double.NaN;
    xMax = Double.NaN;
    yMin = Double.NaN;
    yMax = Double.NaN;
    zMin = Double.NaN;
    zMax = Double.NaN;
    mMin = Double.NaN;
    mMax = Double.NaN;
  }

  /**
   * Updates the bounding box with the given coordinates.
   * If any coordinate is NaN, the method will abort and set all bounds to NaN.
   */
  private void updateBounds(
      double minX, double maxX, double minY, double maxY, double minZ, double maxZ, double minM, double maxM) {
    boolean foundValidValue = false;

    // Update X bounds if valid
    if (!Double.isNaN(minX) && !Double.isNaN(maxX)) {
      xMin = Math.min(xMin, minX);
      xMax = Math.max(xMax, maxX);
      foundValidValue = true;
    }

    // Update Y bounds if valid
    if (!Double.isNaN(minY) && !Double.isNaN(maxY)) {
      yMin = Math.min(yMin, minY);
      yMax = Math.max(yMax, maxY);
      foundValidValue = true;
    }

    // Update Z bounds if valid
    if (!Double.isNaN(minZ) && minZ != Double.POSITIVE_INFINITY) {
      zMin = Math.min(zMin, minZ);
      foundValidValue = true;
    }
    if (!Double.isNaN(maxZ) && maxZ != Double.NEGATIVE_INFINITY) {
      zMax = Math.max(zMax, maxZ);
      foundValidValue = true;
    }

    // Update M bounds if valid
    if (!Double.isNaN(minM) && minM != Double.POSITIVE_INFINITY) {
      mMin = Math.min(mMin, minM);
      foundValidValue = true;
    }
    if (!Double.isNaN(maxM) && maxM != Double.NEGATIVE_INFINITY) {
      mMax = Math.max(mMax, maxM);
      foundValidValue = true;
    }

    // If no valid values were found, abort
    if (!foundValidValue) {
      abort();
    }
  }

  /**
   * Determines if the longitude should be normalized based on the given CRS (Coordinate Reference System).
   * Normalization is required only when the CRS is set to OGC:CRS84, EPSG:4326, or SRID:4326 (case insensitive).
   *
   * @param crs the Coordinate Reference System string
   * @return true if the longitude should be normalized, false otherwise
   */
  private boolean shouldNormalizeLongitude(String crs) {
    if (crs == null) {
      return false;
    }
    String normalizedCrs = crs.trim().toUpperCase();
    return "OGC:CRS84".equals(normalizedCrs)
        || "EPSG:4326".equals(normalizedCrs)
        || "SRID:4326".equals(normalizedCrs);
  }

  public BoundingBox copy() {
    return new BoundingBox(xMin, xMax, yMin, yMax, zMin, zMax, mMin, mMax);
  }

  @Override
  public String toString() {
    return "BoundingBox{" + ", xMin="
        + xMin + ", xMax="
        + xMax + ", yMin="
        + yMin + ", yMax="
        + yMax + ", zMin="
        + zMin + ", zMax="
        + zMax + ", mMin="
        + mMin + ", mMax="
        + mMax + '}';
  }
}
