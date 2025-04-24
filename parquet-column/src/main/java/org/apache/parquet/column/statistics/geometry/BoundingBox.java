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

  public BoundingBox() {}

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
   * Checks if the bounding box is valid.
   * A bounding box is considered valid if none of the bounds are in their initial state
   * (i.e., xMin = +Infinity, xMax = -Infinity, etc.) and none of the bounds contain NaN.
   *
   * @return true if the bounding box is valid, false otherwise.
   */
  public boolean isValid() {
    return !(Double.isNaN(xMin) || Double.isNaN(xMax) || Double.isNaN(yMin) || Double.isNaN(yMax));
  }

  /**
   * Checks if the Z dimension of the bounding box is valid.
   * The Z dimension is considered valid if zMin and zMax are not infinite
   * and don't contain NaN values.
   *
   * @return true if the Z dimension is valid, false otherwise.
   */
  public boolean isZValid() {
    return !(Double.isInfinite(zMin) || Double.isInfinite(zMax) ||
        Double.isNaN(zMin) || Double.isNaN(zMax));
  }

  /**
   * Checks if the M dimension of the bounding box is valid.
   * The M dimension is considered valid if mMin and mMax are not infinite
   * and don't contain NaN values.
   *
   * @return true if the M dimension is valid, false otherwise.
   */
  public boolean isMValid() {
    return !(Double.isInfinite(mMin) || Double.isInfinite(mMax) ||
        Double.isNaN(mMin) || Double.isNaN(mMax));
  }

  /**
   * Checks if the bounding box is empty.
   * A bounding box is considered empty if any bounds are in their initial state
   *
   * @return true if the bounding box is empty, false otherwise.
   */
  public boolean isEmpty() {
    return (Double.isInfinite(xMin) && Double.isInfinite(xMax))
        || (Double.isInfinite(yMin) && Double.isInfinite(yMax));
  }

  /**
   * Merges the bounds of another bounding box into this one.
   *
   * @param other the other BoundingBox to merge
   */
  public void merge(BoundingBox other) {
    if (other == null || other.isEmpty()) {
      return;
    }
    this.xMin = Math.min(this.xMin, other.xMin);
    this.xMax = Math.max(this.xMax, other.xMax);
    this.yMin = Math.min(this.yMin, other.yMin);
    this.yMax = Math.max(this.yMax, other.yMax);
    this.zMin = Math.min(this.zMin, other.zMin);
    this.zMax = Math.max(this.zMax, other.zMax);
    this.mMin = Math.min(this.mMin, other.mMin);
    this.mMax = Math.max(this.mMax, other.mMax);
  }

  /**
   * Updates the bounding box with the coordinates of the given geometry.
   * If the geometry is null or empty, the update is aborted.
   */
  public void update(Geometry geometry) {
    if (geometry == null || geometry.isEmpty()) {
      return;
    }

    Envelope envelope = geometry.getEnvelopeInternal();
    updateBounds(envelope.getMinX(), envelope.getMaxX(), envelope.getMinY(), envelope.getMaxY());

    for (Coordinate coord : geometry.getCoordinates()) {
      if (!Double.isNaN(coord.getZ())) {
        zMin = Math.min(zMin, coord.getZ());
        zMax = Math.max(zMax, coord.getZ());
      }
      if (!Double.isNaN(coord.getM())) {
        mMin = Math.min(mMin, coord.getM());
        mMax = Math.max(mMax, coord.getM());
      }
    }
  }

  /**
   * Updates the bounding box with the given bounds.
   * Only updates X bounds if both minX and maxX are not NaN.
   * Only updates Y bounds if both minY and maxY are not NaN.
   */
  private void updateBounds(double minX, double maxX, double minY, double maxY) {
    if (!Double.isNaN(minX) && !Double.isNaN(maxX)) {
      xMin = Math.min(xMin, minX);
      xMax = Math.max(xMax, maxX);
    }

    if (!Double.isNaN(minY) && !Double.isNaN(maxY)) {
      yMin = Math.min(yMin, minY);
      yMax = Math.max(yMax, maxY);
    }
  }

  /**
   * Aborts the bounding box by resetting it to its initial state.
   */
  public void abort() {
    reset();
  }

  /**
   * Resets the bounding box to its initial state.
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
   * Creates a copy of the current bounding box.
   *
   * @return a new BoundingBox instance with the same values as this one.
   */
  public BoundingBox copy() {
    return new BoundingBox(
        this.xMin, this.xMax,
        this.yMin, this.yMax,
        this.zMin, this.zMax,
        this.mMin, this.mMax);
  }

  @Override
  public String toString() {
    return "BoundingBox{" + "xMin="
        + xMin + ", xMax=" + xMax + ", yMin="
        + yMin + ", yMax=" + yMax + ", zMin="
        + zMin + ", zMax=" + zMax + ", mMin="
        + mMin + ", mMax=" + mMax + '}';
  }
}
