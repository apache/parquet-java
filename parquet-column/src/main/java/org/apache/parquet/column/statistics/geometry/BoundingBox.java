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

  // Method to update the bounding box with the coordinates of a Geometry object
  // geometry can be changed by this method
  void update(Geometry geometry, String crs) {
    if (shouldNormalizeLongitude(crs)) {
      GeospatialUtils.normalizeLongitude(geometry);
    }
    Envelope envelope = geometry.getEnvelopeInternal();
    double minX = envelope.getMinX();
    double minY = envelope.getMinY();
    double maxX = envelope.getMaxX();
    double maxY = envelope.getMaxY();

    // Initialize Z and M values
    double minZ = Double.POSITIVE_INFINITY;
    double maxZ = Double.NEGATIVE_INFINITY;
    double minM = Double.POSITIVE_INFINITY;
    double maxM = Double.NEGATIVE_INFINITY;

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

    updateXBounds(minX, maxX);

    yMin = Math.min(yMin, minY);
    yMax = Math.max(yMax, maxY);
    zMin = Math.min(zMin, minZ);
    zMax = Math.max(zMax, maxZ);
    mMin = Math.min(mMin, minM);
    mMax = Math.max(mMax, maxM);
  }

  void merge(BoundingBox other) {
    Preconditions.checkArgument(other != null, "Cannot merge with null bounding box");
    double minX = other.xMin;
    double maxX = other.xMax;

    updateXBounds(minX, maxX);

    yMin = Math.min(yMin, other.yMin);
    yMax = Math.max(yMax, other.yMax);
    zMin = Math.min(zMin, other.zMin);
    zMax = Math.max(zMax, other.zMax);
    mMin = Math.min(mMin, other.mMin);
    mMax = Math.max(mMax, other.mMax);
  }

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

  private boolean isCrossingAntiMeridian(double x1, double x2) {
    return Math.abs(x1 - x2) > 180;
  }

  private void updateXBounds(double minX, double maxX) {
    if (xMin == Double.POSITIVE_INFINITY || xMax == Double.NEGATIVE_INFINITY) {
      xMin = minX;
      xMax = maxX;
    } else {
      if (!isCrossingAntiMeridian(xMax, xMin)) {
        if (!isCrossingAntiMeridian(maxX, minX)) {
          xMin = Math.min(xMin, minX);
          xMax = Math.max(xMax, maxX);
        } else {
          xMin = Math.max(xMin, maxX);
          xMax = Math.min(xMax, minX);
        }
      } else {
        if (!isCrossingAntiMeridian(maxX, minX)) {
          xMin = Math.max(xMin, minX);
          xMax = Math.min(xMax, maxX);
        } else {
          xMin = Math.max(xMin, maxX);
          xMax = Math.min(xMax, minX);
        }
      }
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
    return "OGC:CRS84".equals(normalizedCrs) || "EPSG:4326".equals(normalizedCrs) || "SRID:4326".equals(normalizedCrs);
  }

  public BoundingBox copy() {
    return new BoundingBox(xMin, xMax, yMin, yMax, zMin, zMax, mMin, mMax);
  }

  @Override
  public String toString() {
    return "BoundingBox{" + "xMin="
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
