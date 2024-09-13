package org.apache.parquet.column.statistics.geometry;

import org.locationtech.jts.geom.Geometry;

// Immutable dummy BoundingBox class
class DummyBoundingBox extends BoundingBox {
  @Override
  public void update(double minX, double maxX, double minY, double maxY, double minZ, double maxZ) {
    // No-op
  }

  @Override
  public void update(Geometry geometry) {
    // No-op
  }

  @Override
  public void merge(BoundingBox other) {
    // No-op
  }

  @Override
  public void reset() {
    // No-op
  }

  @Override
  public void abort() {
    // No-op
  }

  @Override
  public BoundingBox copy() {
    return this; // Return the same instance
  }
}
