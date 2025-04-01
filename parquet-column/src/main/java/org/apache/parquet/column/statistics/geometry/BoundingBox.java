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

  private double xMin = Double.NaN;
  private double xMax = Double.NaN;
  private double yMin = Double.NaN;
  private double yMax = Double.NaN;
  private double zMin = Double.NaN;
  private double zMax = Double.NaN;
  private double mMin = Double.NaN;
  private double mMax = Double.NaN;

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
   * If the geometry is empty, it will keep the initial - all NaN state for bounds.
   * If the geometry is valid, it will update the bounds accordingly.
   */
  void update(Geometry geometry, String crs) {
    if (geometry == null || geometry.isEmpty()) {
      // Abort if geometry is null or empty
      // This will keep the bounding box in its initial state (all NaN)
      abort();
      return;
    }

    Envelope envelope = geometry.getEnvelopeInternal();
    double minX = envelope.getMinX();
    double maxX = envelope.getMaxX();
    double minY = envelope.getMinY();
    double maxY = envelope.getMaxY();

    // Initialize Z and M values
    double minZ = Double.NaN;
    double maxZ = Double.NaN;
    double minM = Double.NaN;
    double maxM = Double.NaN;

    // Find Z and M bounds from coordinates
    Coordinate[] coordinates = geometry.getCoordinates();
    for (Coordinate coord : coordinates) {
      if (!Double.isNaN(coord.getZ())) {
        // For the first valid Z value, initialize minZ/maxZ if they're NaN
        if (Double.isNaN(minZ)) {
          minZ = coord.getZ();
          maxZ = coord.getZ();
        } else {
          minZ = Math.min(minZ, coord.getZ());
          maxZ = Math.max(maxZ, coord.getZ());
        }
      }

      if (!Double.isNaN(coord.getM())) {
        // For the first valid M value, initialize minM/maxM if they're NaN
        if (Double.isNaN(minM)) {
          minM = coord.getM();
          maxM = coord.getM();
        } else {
          minM = Math.min(minM, coord.getM());
          maxM = Math.max(maxM, coord.getM());
        }
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
   * All bounds will be set to NaN.
   */
  public void reset() {
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
   * Aborts the bounding box update.
   * All bounds will be set to NaN.
   */
  public void abort() {
    reset();
  }

  /**
   * Updates the bounding box with the given coordinates.
   * If any coordinate is NaN, the method will abort and set all bounds to NaN.
   */
  private void updateBounds(
      double minX, double maxX, double minY, double maxY, double minZ, double maxZ, double minM, double maxM) {

    // Update X bounds if valid
    if (!Double.isNaN(minX) && !Double.isNaN(maxX)) {
      if (Double.isNaN(xMin) || Double.isNaN(xMax)) {
        // First valid X values
        xMin = minX;
        xMax = maxX;
      } else {
        xMin = Math.min(xMin, minX);
        xMax = Math.max(xMax, maxX);
      }
    }

    // Update Y bounds if valid
    if (!Double.isNaN(minY) && !Double.isNaN(maxY)) {
      if (Double.isNaN(yMin) || Double.isNaN(yMax)) {
        // First valid Y values
        yMin = minY;
        yMax = maxY;
      } else {
        yMin = Math.min(yMin, minY);
        yMax = Math.max(yMax, maxY);
      }
    }

    // Update Z bounds if valid
    if (!Double.isNaN(minZ)) {
      if (Double.isNaN(zMin)) {
        zMin = minZ;
      } else {
        zMin = Math.min(zMin, minZ);
      }
    }
    if (!Double.isNaN(maxZ)) {
      if (Double.isNaN(zMax)) {
        zMax = maxZ;
      } else {
        zMax = Math.max(zMax, maxZ);
      }
    }

    // Update M bounds if valid
    if (!Double.isNaN(minM)) {
      if (Double.isNaN(mMin)) {
        mMin = minM;
      } else {
        mMin = Math.min(mMin, minM);
      }
    }
    if (!Double.isNaN(maxM)) {
      if (Double.isNaN(mMax)) {
        mMax = maxM;
      } else {
        mMax = Math.max(mMax, maxM);
      }
    }
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
