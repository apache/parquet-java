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
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

public class TestBoundingBox {

  @Test
  public void testUpdate() {
    GeometryFactory geometryFactory = new GeometryFactory();
    BoundingBox boundingBox = new BoundingBox();

    // Create a 2D point
    Point point2D = geometryFactory.createPoint(new Coordinate(10, 20));
    boundingBox.update(point2D);
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
    boundingBox.update(emptyPoint);

    // Empty geometry should retain the initial NaN state
    Assert.assertEquals(Double.NaN, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(Double.NaN, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(Double.NaN, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(Double.NaN, boundingBox.getYMax(), 0.0);

    // Test that after adding a non-empty geometry, values are updated correctly
    Point point = geometryFactory.createPoint(new Coordinate(10, 20));
    boundingBox.update(point);
    Assert.assertEquals(10.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(10.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMax(), 0.0);

    // Update with another empty geometry, should not change the bounds
    boundingBox.update(emptyPoint);
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
    boundingBox.update(nanPoint);

    // All values should be NaN after updating with all-NaN coordinates
    Assert.assertEquals("XMin should be NaN", Double.NaN, boundingBox.getXMin(), 0.0);
    Assert.assertEquals("XMax should be NaN", Double.NaN, boundingBox.getXMax(), 0.0);
    Assert.assertEquals("YMin should be NaN", Double.NaN, boundingBox.getYMin(), 0.0);
    Assert.assertEquals("YMax should be NaN", Double.NaN, boundingBox.getYMax(), 0.0);

    // Reset the bounding box for the next test
    boundingBox = new BoundingBox();

    // Create a mixed point with a valid coordinate and a NaN coordinate
    Point mixedPoint = geometryFactory.createPoint(new Coordinate(15.0, Double.NaN));
    boundingBox.update(mixedPoint);

    // The valid X coordinate should be used, Y should remain at initial values
    Assert.assertEquals("XMin should be 15.0", 15.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals("XMax should be 15.0", 15.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals("YMin should be NaN", Double.NaN, boundingBox.getYMin(), 0.0);
    Assert.assertEquals("YMax should be NaN", Double.NaN, boundingBox.getYMax(), 0.0);

    // Add a fully valid point
    Point validPoint = geometryFactory.createPoint(new Coordinate(10, 20));
    boundingBox.update(validPoint);

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
    boundingBox.update(nanZPoint);

    // X and Y should be updated, but Z should remain NaN
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
    boundingBox.update(validZPoint);

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

  @Test
  public void testAbort() {
    GeometryFactory geometryFactory = new GeometryFactory();
    BoundingBox boundingBox = new BoundingBox();

    // Create a valid point
    Point validPoint = geometryFactory.createPoint(new Coordinate(10, 20));
    boundingBox.update(validPoint);

    // Check initial values
    Assert.assertEquals(10.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(10.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMax(), 0.0);

    // Abort the update
    boundingBox.abort();

    // Check that values are reset to initial NaN state
    Assert.assertTrue(Double.isNaN(boundingBox.getXMin()));
    Assert.assertTrue(Double.isNaN(boundingBox.getXMax()));
    Assert.assertTrue(Double.isNaN(boundingBox.getYMin()));
    Assert.assertTrue(Double.isNaN(boundingBox.getYMax()));
  }

  @Test
  public void testEmptyBoundingBox() {
    BoundingBox boundingBox = new BoundingBox();
    Assert.assertTrue(Double.isNaN(boundingBox.getXMin()));
    Assert.assertTrue(Double.isNaN(boundingBox.getXMax()));
    Assert.assertTrue(Double.isNaN(boundingBox.getYMin()));
    Assert.assertTrue(Double.isNaN(boundingBox.getYMax()));
    Assert.assertTrue(Double.isNaN(boundingBox.getZMin()));
    Assert.assertTrue(Double.isNaN(boundingBox.getZMax()));
    Assert.assertTrue(Double.isNaN(boundingBox.getMMin()));
    Assert.assertTrue(Double.isNaN(boundingBox.getMMax()));
  }

  @Test
  public void testMergeBoundingBoxes() {
    BoundingBox boundingBox1 = new BoundingBox(0, 10, 0, 20, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
    BoundingBox boundingBox2 = new BoundingBox(5, 15, 10, 30, Double.NaN, Double.NaN, Double.NaN, Double.NaN);

    boundingBox1.merge(boundingBox2);

    Assert.assertEquals(0.0, boundingBox1.getXMin(), 0.0);
    Assert.assertEquals(15.0, boundingBox1.getXMax(), 0.0);
    Assert.assertEquals(0.0, boundingBox1.getYMin(), 0.0);
    Assert.assertEquals(30.0, boundingBox1.getYMax(), 0.0);
    Assert.assertTrue(Double.isNaN(boundingBox1.getZMin()));
    Assert.assertTrue(Double.isNaN(boundingBox1.getZMax()));
    Assert.assertTrue(Double.isNaN(boundingBox1.getMMin()));
    Assert.assertTrue(Double.isNaN(boundingBox1.getMMax()));
  }

  @Test
  public void testMergeWithEmptyBoundingBox() {
    BoundingBox boundingBox1 = new BoundingBox(0, 10, 0, 20, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
    BoundingBox emptyBoundingBox = new BoundingBox();

    boundingBox1.merge(emptyBoundingBox);

    Assert.assertEquals(0.0, boundingBox1.getXMin(), 0.0);
    Assert.assertEquals(10.0, boundingBox1.getXMax(), 0.0);
    Assert.assertEquals(0.0, boundingBox1.getYMin(), 0.0);
    Assert.assertEquals(20.0, boundingBox1.getYMax(), 0.0);
    Assert.assertTrue(Double.isNaN(boundingBox1.getZMin()));
    Assert.assertTrue(Double.isNaN(boundingBox1.getZMax()));
    Assert.assertTrue(Double.isNaN(boundingBox1.getMMin()));
    Assert.assertTrue(Double.isNaN(boundingBox1.getMMax()));
  }

  @Test
  public void testUpdateWithNullGeometry() {
    BoundingBox boundingBox = new BoundingBox();
    boundingBox.update(null);

    // Check that the bounding box remains unchanged
    Assert.assertTrue(Double.isNaN(boundingBox.getXMin()));
    Assert.assertTrue(Double.isNaN(boundingBox.getXMax()));
    Assert.assertTrue(Double.isNaN(boundingBox.getYMin()));
    Assert.assertTrue(Double.isNaN(boundingBox.getYMax()));
  }

  @Test
  public void testMergeWithNaNValues() {
    // Test merging with NaN values in different dimensions
    BoundingBox box1 = new BoundingBox(0, 10, 0, 10, 0, 10, 0, 10);
    BoundingBox box2 = new BoundingBox(5, 15, Double.NaN, Double.NaN, 5, 15, Double.NaN, Double.NaN);

    box1.merge(box2);

    Assert.assertEquals(0.0, box1.getXMin(), 0.0);
    Assert.assertEquals(15.0, box1.getXMax(), 0.0);
    Assert.assertEquals(0.0, box1.getYMin(), 0.0);
    Assert.assertEquals(10.0, box1.getYMax(), 0.0);
    Assert.assertEquals(0.0, box1.getZMin(), 0.0);
    Assert.assertEquals(15.0, box1.getZMax(), 0.0);
    Assert.assertEquals(0.0, box1.getMMin(), 0.0);
    Assert.assertEquals(10.0, box1.getMMax(), 0.0);
  }

  @Test
  public void testUpdateWithAllNaNCoordinatesAfterValid() {
    GeometryFactory gf = new GeometryFactory();
    BoundingBox box = new BoundingBox();

    // First add a valid point
    box.update(gf.createPoint(new Coordinate(10, 20)));
    Assert.assertEquals(10.0, box.getXMin(), 0.0);

    // Then update with all NaN coordinates - should not change valid values
    Point nanPoint = gf.createPoint(new Coordinate(Double.NaN, Double.NaN));
    box.update(nanPoint);

    // Values should remain unchanged
    Assert.assertEquals(10.0, box.getXMin(), 0.0);
    Assert.assertEquals(10.0, box.getXMax(), 0.0);
    Assert.assertEquals(20.0, box.getYMin(), 0.0);
    Assert.assertEquals(20.0, box.getYMax(), 0.0);
  }

  @Test
  public void testUpdate3DPoint() {
    GeometryFactory gf = new GeometryFactory();
    BoundingBox box = new BoundingBox();

    // Create a 3D point
    Coordinate coord = new Coordinate(10, 20, 30);
    Point point3D = gf.createPoint(coord);
    box.update(point3D);

    Assert.assertEquals(10.0, box.getXMin(), 0.0);
    Assert.assertEquals(10.0, box.getXMax(), 0.0);
    Assert.assertEquals(20.0, box.getYMin(), 0.0);
    Assert.assertEquals(20.0, box.getYMax(), 0.0);
    Assert.assertEquals(30.0, box.getZMin(), 0.0);
    Assert.assertEquals(30.0, box.getZMax(), 0.0);

    // Add another 3D point with different Z
    box.update(gf.createPoint(new Coordinate(15, 25, 10)));

    Assert.assertEquals(10.0, box.getXMin(), 0.0);
    Assert.assertEquals(15.0, box.getXMax(), 0.0);
    Assert.assertEquals(20.0, box.getYMin(), 0.0);
    Assert.assertEquals(25.0, box.getYMax(), 0.0);
    Assert.assertEquals(10.0, box.getZMin(), 0.0);
    Assert.assertEquals(30.0, box.getZMax(), 0.0);
  }

  @Test
  public void testUpdateWithMeasureValue() {
    GeometryFactory gf = new GeometryFactory();
    BoundingBox box = new BoundingBox();

    // Create a point with M value using CoordinateXYZM instead of setM
    CoordinateXYZM coord = new CoordinateXYZM(10, 20, Double.NaN, 5.0);
    Point pointWithM = gf.createPoint(coord);
    box.update(pointWithM);

    Assert.assertEquals(10.0, box.getXMin(), 0.0);
    Assert.assertEquals(10.0, box.getXMax(), 0.0);
    Assert.assertEquals(20.0, box.getYMin(), 0.0);
    Assert.assertEquals(20.0, box.getYMax(), 0.0);
    Assert.assertEquals(Double.NaN, box.getZMin(), 0.0);
    Assert.assertEquals(Double.NaN, box.getZMax(), 0.0);
    Assert.assertEquals(5.0, box.getMMin(), 0.0);
    Assert.assertEquals(5.0, box.getMMax(), 0.0);

    // Add another point with different M value
    CoordinateXYZM coord2 = new CoordinateXYZM(15, 25, Double.NaN, 10.0);
    box.update(gf.createPoint(coord2));

    Assert.assertEquals(10.0, box.getXMin(), 0.0);
    Assert.assertEquals(15.0, box.getXMax(), 0.0);
    Assert.assertEquals(20.0, box.getYMin(), 0.0);
    Assert.assertEquals(25.0, box.getYMax(), 0.0);
    Assert.assertEquals(Double.NaN, box.getZMin(), 0.0);
    Assert.assertEquals(Double.NaN, box.getZMax(), 0.0);
    Assert.assertEquals(5.0, box.getMMin(), 0.0);
    Assert.assertEquals(10.0, box.getMMax(), 0.0);
  }

  @Test
  public void testResetAfterUpdate() {
    GeometryFactory gf = new GeometryFactory();
    BoundingBox box = new BoundingBox();

    // Update with valid point
    box.update(gf.createPoint(new Coordinate(10, 20)));
    Assert.assertEquals(10.0, box.getXMin(), 0.0);

    // Reset the box
    box.reset();

    // All values should be NaN
    Assert.assertEquals(Double.NaN, box.getXMin(), 0.0);
    Assert.assertEquals(Double.NaN, box.getXMax(), 0.0);
    Assert.assertEquals(Double.NaN, box.getYMin(), 0.0);
    Assert.assertEquals(Double.NaN, box.getYMax(), 0.0);

    // Update after reset should work correctly
    box.update(gf.createPoint(new Coordinate(30, 40)));
    Assert.assertEquals(30.0, box.getXMin(), 0.0);
    Assert.assertEquals(30.0, box.getXMax(), 0.0);
    Assert.assertEquals(40.0, box.getYMin(), 0.0);
    Assert.assertEquals(40.0, box.getYMax(), 0.0);
  }

  @Test
  public void testCopy() {
    // Create and populate a bounding box
    BoundingBox original = new BoundingBox(1, 2, 3, 4, 5, 6, 7, 8);

    // Create copy
    BoundingBox copy = original.copy();

    // Verify all values are copied correctly
    Assert.assertEquals(original.getXMin(), copy.getXMin(), 0.0);
    Assert.assertEquals(original.getXMax(), copy.getXMax(), 0.0);
    Assert.assertEquals(original.getYMin(), copy.getYMin(), 0.0);
    Assert.assertEquals(original.getYMax(), copy.getYMax(), 0.0);
    Assert.assertEquals(original.getZMin(), copy.getZMin(), 0.0);
    Assert.assertEquals(original.getZMax(), copy.getZMax(), 0.0);
    Assert.assertEquals(original.getMMin(), copy.getMMin(), 0.0);
    Assert.assertEquals(original.getMMax(), copy.getMMax(), 0.0);

    // Modify the copy and verify original is unchanged
    copy.reset();
    Assert.assertEquals(1.0, original.getXMin(), 0.0);
    Assert.assertEquals(Double.NaN, copy.getXMin(), 0.0);
  }

  @Test
  public void testMergeWithAllNaNBox() {
    // Box with valid values
    BoundingBox box1 = new BoundingBox(1, 2, 3, 4, 5, 6, 7, 8);

    // Empty box with all NaN values
    BoundingBox box2 = new BoundingBox();

    // Merge should keep existing values
    box1.merge(box2);

    Assert.assertEquals(1.0, box1.getXMin(), 0.0);
    Assert.assertEquals(2.0, box1.getXMax(), 0.0);
    Assert.assertEquals(3.0, box1.getYMin(), 0.0);
    Assert.assertEquals(4.0, box1.getYMax(), 0.0);
    Assert.assertEquals(5.0, box1.getZMin(), 0.0);
    Assert.assertEquals(6.0, box1.getZMax(), 0.0);
    Assert.assertEquals(7.0, box1.getMMin(), 0.0);
    Assert.assertEquals(8.0, box1.getMMax(), 0.0);

    // Test the reverse - NaN box merging with valid box
    BoundingBox box3 = new BoundingBox();
    box3.merge(box1);

    Assert.assertEquals(1.0, box3.getXMin(), 0.0);
    Assert.assertEquals(2.0, box3.getXMax(), 0.0);
    Assert.assertEquals(3.0, box3.getYMin(), 0.0);
    Assert.assertEquals(4.0, box3.getYMax(), 0.0);
  }

  @Test
  public void testLineStringWithNaNCoordinates() {
    GeometryFactory gf = new GeometryFactory();
    BoundingBox box = new BoundingBox();

    // Create a LineString with NaN coordinates in the middle
    Coordinate[] coords =
        new Coordinate[] {new Coordinate(0, 1), new Coordinate(Double.NaN, Double.NaN), new Coordinate(2, 3)};

    box.update(gf.createLineString(coords));

    // The bounding box should include the valid coordinates and ignore NaN
    Assert.assertEquals(0.0, box.getXMin(), 0.0);
    Assert.assertEquals(2.0, box.getXMax(), 0.0);
    Assert.assertEquals(1.0, box.getYMin(), 0.0);
    Assert.assertEquals(3.0, box.getYMax(), 0.0);

    // Test with only one valid coordinate
    BoundingBox box2 = new BoundingBox();
    Coordinate[] coords2 = new Coordinate[] {
      new Coordinate(5, 6), new Coordinate(Double.NaN, Double.NaN), new Coordinate(Double.NaN, Double.NaN)
    };

    box2.update(gf.createLineString(coords2));

    Assert.assertEquals(5.0, box2.getXMin(), 0.0);
    Assert.assertEquals(5.0, box2.getXMax(), 0.0);
    Assert.assertEquals(6.0, box2.getYMin(), 0.0);
    Assert.assertEquals(6.0, box2.getYMax(), 0.0);

    // Test with all NaN coordinates
    BoundingBox box3 = new BoundingBox();
    Coordinate[] coords3 =
        new Coordinate[] {new Coordinate(Double.NaN, Double.NaN), new Coordinate(Double.NaN, Double.NaN)};

    box3.update(gf.createLineString(coords3));

    // Should result in NaN for all values
    Assert.assertEquals(Double.NaN, box3.getXMin(), 0.0);
    Assert.assertEquals(Double.NaN, box3.getXMax(), 0.0);
    Assert.assertEquals(Double.NaN, box3.getYMin(), 0.0);
    Assert.assertEquals(Double.NaN, box3.getYMax(), 0.0);
  }

  @Test
  public void testLineStringWithPartialNaNCoordinates() {
    GeometryFactory gf = new GeometryFactory();
    BoundingBox box = new BoundingBox();

    // Create a LineString with partial NaN coordinate in the middle
    // where only the Y value is NaN: "LINESTRING (0 1, 1 nan, 2 3)"
    Coordinate[] coords =
        new Coordinate[] {new Coordinate(0, 1), new Coordinate(1, Double.NaN), new Coordinate(2, 3)};

    box.update(gf.createLineString(coords));

    // The bounding box should include all valid coordinates
    // X should include all values: 0, 1, 2
    // Y should only include valid values: 1, 3
    Assert.assertEquals(0.0, box.getXMin(), 0.0);
    Assert.assertEquals(2.0, box.getXMax(), 0.0);
    Assert.assertEquals(1.0, box.getYMin(), 0.0);
    Assert.assertEquals(3.0, box.getYMax(), 0.0);

    // Test with mixed NaN values in different components
    BoundingBox box2 = new BoundingBox();
    Coordinate[] coords2 =
        new Coordinate[] {new Coordinate(Double.NaN, 5), new Coordinate(6, Double.NaN), new Coordinate(7, 8)};

    box2.update(gf.createLineString(coords2));

    // X should only include valid values: 6, 7
    // Y should only include valid values: 5, 8
    Assert.assertEquals(Double.NaN, box2.getXMin(), 0.0);
    Assert.assertEquals(Double.NaN, box2.getXMax(), 0.0);
    Assert.assertEquals(5.0, box2.getYMin(), 0.0);
    Assert.assertEquals(8.0, box2.getYMax(), 0.0);
  }
}
