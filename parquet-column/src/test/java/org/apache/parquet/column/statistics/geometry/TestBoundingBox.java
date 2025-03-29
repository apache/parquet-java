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

import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

public class TestBoundingBox {

  @Test
  public void testUpdate() {
    GeometryFactory geometryFactory = new GeometryFactory();
    BoundingBox boundingBox = new BoundingBox();

    // Create a 2D point
    Point point2D = geometryFactory.createPoint(new Coordinate(10, 20));
    boundingBox.update(point2D, "EPSG:4326");
    Assert.assertEquals(10.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(10.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMax(), 0.0);
  }

  @Test
  public void testEmptyGeometry() {
    GeometryFactory geometryFactory = new GeometryFactory();
    BoundingBox boundingBox = new BoundingBox();

    // Create an empty point
    Point emptyPoint = geometryFactory.createPoint();
    boundingBox.update(emptyPoint, "EPSG:4326");

    // Empty geometry should retain the initial -Inf/Inf state
    Assert.assertEquals(Double.NaN, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(Double.NaN, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(Double.NaN, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(Double.NaN, boundingBox.getYMax(), 0.0);

    // Test that after adding a non-empty geometry, values are updated correctly
    Point point = geometryFactory.createPoint(new Coordinate(10, 20));
    boundingBox.update(point, "EPSG:4326");
    Assert.assertEquals(10.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(10.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMax(), 0.0);

    // Update with another empty geometry, should not change the bounds
    boundingBox.update(emptyPoint, "EPSG:4326");
    Assert.assertEquals(Double.NaN, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(Double.NaN, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(Double.NaN, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(Double.NaN, boundingBox.getYMax(), 0.0);
  }

  @Test
  public void testNaNCoordinates() {
    GeometryFactory geometryFactory = new GeometryFactory();
    BoundingBox boundingBox = new BoundingBox();

    // Create a point with NaN coordinates
    Point nanPoint = geometryFactory.createPoint(new Coordinate(Double.NaN, Double.NaN));
    boundingBox.update(nanPoint, "EPSG:4326");

    // All values should be NaN after updating with all-NaN coordinates
    Assert.assertEquals("XMin should be NaN", Double.NaN, boundingBox.getXMin(), 0.0);
    Assert.assertEquals("XMax should be NaN", Double.NaN, boundingBox.getXMax(), 0.0);
    Assert.assertEquals("YMin should be NaN", Double.NaN, boundingBox.getYMin(), 0.0);
    Assert.assertEquals("YMax should be NaN", Double.NaN, boundingBox.getYMax(), 0.0);

    // Reset the bounding box for the next test
    boundingBox = new BoundingBox();

    // Create a mixed point with a valid coordinate and a NaN coordinate
    Point mixedPoint = geometryFactory.createPoint(new Coordinate(15.0, Double.NaN));
    boundingBox.update(mixedPoint, "EPSG:4326");

    // The valid X coordinate should be used, Y should remain at initial values
    Assert.assertEquals("XMin should be 15.0", 15.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals("XMax should be 15.0", 15.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals("YMin should be NaN", Double.NaN, boundingBox.getYMin(), 0.0);
    Assert.assertEquals("YMax should be NaN", Double.NaN, boundingBox.getYMax(), 0.0);

    // Add a fully valid point
    Point validPoint = geometryFactory.createPoint(new Coordinate(10, 20));
    boundingBox.update(validPoint, "EPSG:4326");

    // Both X and Y should now be updated
    Assert.assertEquals("XMin should be 10.0", 10.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals("XMax should be 15.0", 15.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals("YMin should be 20.0", 20.0, boundingBox.getYMin(), 0.0);
    Assert.assertEquals("YMax should be 20.0", 20.0, boundingBox.getYMax(), 0.0);
  }

  @Test
  public void testNaNZAndMValues() {
    GeometryFactory geometryFactory = new GeometryFactory();
    BoundingBox boundingBox = new BoundingBox();

    // Create a point with NaN Z value only
    Coordinate coord = new Coordinate(10, 20);
    coord.setZ(Double.NaN); // Only set Z, not M
    Point nanZPoint = geometryFactory.createPoint(coord);
    boundingBox.update(nanZPoint, "EPSG:4326");

    // X and Y should be updated, but Z should remain -Inf/Inf
    Assert.assertEquals(10.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(10.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMax(), 0.0);
    Assert.assertEquals(Double.NaN, boundingBox.getZMin(), 0.0);
    Assert.assertEquals(Double.NaN, boundingBox.getZMax(), 0.0);
    Assert.assertEquals(Double.NaN, boundingBox.getMMin(), 0.0);
    Assert.assertEquals(Double.NaN, boundingBox.getMMax(), 0.0);

    // Add a point with valid Z value
    Coordinate coord2 = new Coordinate(15, 25, 30); // Using constructor with Z
    Point validZPoint = geometryFactory.createPoint(coord2);
    boundingBox.update(validZPoint, "EPSG:4326");

    // X, Y, and Z values should now be updated
    Assert.assertEquals(10.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(15.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(25.0, boundingBox.getYMax(), 0.0);
    Assert.assertEquals(30.0, boundingBox.getZMin(), 0.0);
    Assert.assertEquals(30.0, boundingBox.getZMax(), 0.0);
    Assert.assertEquals(Double.NaN, boundingBox.getMMin(), 0.0);
    Assert.assertEquals(Double.NaN, boundingBox.getMMax(), 0.0);
  }
}
