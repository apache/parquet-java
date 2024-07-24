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

  void update(double minX, double maxX, double minY, double maxY, double minZ, double maxZ) {
    xMin = Math.min(xMin, minX);
    yMin = Math.min(yMin, minY);
    xMax = Math.max(xMax, maxX);
    yMax = Math.max(yMax, maxY);
    zMin = Math.min(zMin, minZ);
    zMax = Math.max(zMax, maxZ);
  }

  void update(Geometry geometry) {
    GeometryUtils.normalizeLongitude(geometry);
    Envelope envelope = geometry.getEnvelopeInternal();
    double minX = envelope.getMinX();
    double minY = envelope.getMinY();
    double maxX = envelope.getMaxX();
    double maxY = envelope.getMaxY();

    // JTS (Java Topology Suite) does not handle Z-coordinates directly in the Envelope class
    // because it's primarily used for 2D geometries. However, we can iterate through the
    // coordinates of the geometry to find the minimum and maximum Z values.
    double minZ = Double.POSITIVE_INFINITY;
    double maxZ = Double.NEGATIVE_INFINITY;

    Coordinate[] coordinates = geometry.getCoordinates();
    for (Coordinate coord : coordinates) {
      if (!Double.isNaN(coord.getZ())) {
        // Update zMin and zMax by iterating through the coordinates.
        minZ = Math.min(minZ, coord.getZ());
        maxZ = Math.max(maxZ, coord.getZ());
      }
    }

    update(minX, maxX, minY, maxY, minZ, maxZ);
  }

  // Method to merge a Geometry object into this bounding box
  public void merge(Geometry geometry) {
    Preconditions.checkArgument(geometry != null, "Cannot merge with null geometry");
    GeometryUtils.normalizeLongitude(geometry);
    update(geometry);
  }

  public void merge(BoundingBox other) {
    Preconditions.checkArgument(other != null, "Cannot merge with null bounding box");
    xMin = Math.min(xMin, other.xMin);
    xMax = Math.max(xMax, other.xMax);
    yMin = Math.min(yMin, other.yMin);
    yMax = Math.max(yMax, other.yMax);
    zMin = Math.min(zMin, other.zMin);
    zMax = Math.max(zMax, other.zMax);
    mMin = Math.min(mMin, other.mMin);
    mMax = Math.max(mMax, other.mMax);
  }

  public void reset() {
    xMin = Double.MAX_VALUE;
    xMax = Double.MIN_VALUE;
    yMin = Double.MAX_VALUE;
    yMax = Double.MIN_VALUE;
    zMin = Double.MAX_VALUE;
    zMax = Double.MIN_VALUE;
    mMin = Double.MAX_VALUE;
    mMax = Double.MIN_VALUE;
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
