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

import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;

public class TestBoundingBox {

  @Test
  public void testUpdate() {
    GeometryFactory geometryFactory = new GeometryFactory();
    BoundingBox boundingBox = new BoundingBox();

    // Create a 2D point
    Point point2D = geometryFactory.createPoint(new Coordinate(10, 20));
    boundingBox.update(point2D);

    Assert.assertTrue(boundingBox.isValid());
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

    // Empty geometry should retain the initial state
    Assert.assertTrue(boundingBox.isValid());
    Assert.assertEquals(Double.POSITIVE_INFINITY, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, boundingBox.getYMax(), 0.0);

    // Test that after adding a non-empty geometry, values are updated correctly
    Point point = geometryFactory.createPoint(new Coordinate(10, 20));
    boundingBox.update(point);
    Assert.assertTrue(boundingBox.isValid());
    Assert.assertEquals(10.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(10.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMax(), 0.0);

    // Update with another empty geometry, should not change the bounds
    boundingBox.update(emptyPoint);
    Assert.assertTrue(boundingBox.isValid());
    Assert.assertEquals(10.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(10.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMax(), 0.0);
  }

  @Test
  public void testNaNCoordinates() {
    GeometryFactory geometryFactory = new GeometryFactory();
    BoundingBox boundingBox = new BoundingBox();

    // Create a point with NaN coordinates
    Point nanPoint = geometryFactory.createPoint(new Coordinate(Double.NaN, Double.NaN));
    boundingBox.update(nanPoint);

    // All values should be NaN after updating with all-NaN coordinates
    Assert.assertTrue(boundingBox.isValid());
    Assert.assertTrue(boundingBox.isXYEmpty());

    // Reset the bounding box for the next test
    boundingBox = new BoundingBox();

    // Create a mixed point with a valid coordinate and a NaN coordinate
    Point mixedPoint = geometryFactory.createPoint(new Coordinate(15.0, Double.NaN));
    boundingBox.update(mixedPoint);

    // The valid X coordinate should be used, Y should remain at initial values
    Assert.assertTrue(boundingBox.isValid());
    Assert.assertTrue(boundingBox.isXYEmpty());
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
    Assert.assertTrue(boundingBox.isValid());
    Assert.assertEquals(10.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(10.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMax(), 0.0);

    // Add a point with valid Z value
    Coordinate coord2 = new Coordinate(15, 25, 30); // Using constructor with Z
    Point validZPoint = geometryFactory.createPoint(coord2);
    boundingBox.update(validZPoint);

    // X, Y, and Z values should now be updated
    Assert.assertTrue(boundingBox.isValid());
    Assert.assertEquals(10.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(15.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(25.0, boundingBox.getYMax(), 0.0);
    Assert.assertEquals(30.0, boundingBox.getZMin(), 0.0);
    Assert.assertEquals(30.0, boundingBox.getZMax(), 0.0);

    // Reset the bounding box for M value tests
    boundingBox.reset();

    // Create a point with NaN M value
    CoordinateXYZM coordNanM = new CoordinateXYZM(10, 20, 30, Double.NaN);
    Point nanMPoint = geometryFactory.createPoint(coordNanM);
    boundingBox.update(nanMPoint);

    // X, Y, Z should be updated, but M should remain at initial values
    Assert.assertTrue(boundingBox.isValid());
    Assert.assertEquals(10.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(10.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMax(), 0.0);
    Assert.assertEquals(30.0, boundingBox.getZMin(), 0.0);
    Assert.assertEquals(30.0, boundingBox.getZMax(), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, boundingBox.getMMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, boundingBox.getMMax(), 0.0);

    // Add a point with valid M value
    CoordinateXYZM coordValidM = new CoordinateXYZM(15, 25, 35, 40);
    Point validMPoint = geometryFactory.createPoint(coordValidM);
    boundingBox.update(validMPoint);

    // All values including M should now be updated
    Assert.assertTrue(boundingBox.isValid());
    Assert.assertEquals(10.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(15.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(25.0, boundingBox.getYMax(), 0.0);
    Assert.assertEquals(30.0, boundingBox.getZMin(), 0.0);
    Assert.assertEquals(35.0, boundingBox.getZMax(), 0.0);
    Assert.assertEquals(40.0, boundingBox.getMMin(), 0.0);
    Assert.assertEquals(40.0, boundingBox.getMMax(), 0.0);
  }

  @Test
  public void testAbort() {
    GeometryFactory geometryFactory = new GeometryFactory();
    BoundingBox boundingBox = new BoundingBox();

    // Create a valid point
    Point validPoint = geometryFactory.createPoint(new Coordinate(10, 20));
    boundingBox.update(validPoint);

    // Check initial values
    Assert.assertTrue(boundingBox.isValid());
    Assert.assertEquals(10.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(10.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(20.0, boundingBox.getYMax(), 0.0);

    // Abort the update
    boundingBox.abort();

    // Check that values are reset to initial state
    Assert.assertFalse(boundingBox.isValid());
    Assert.assertEquals(Double.POSITIVE_INFINITY, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, boundingBox.getYMax(), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, boundingBox.getZMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, boundingBox.getZMax(), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, boundingBox.getMMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, boundingBox.getMMax(), 0.0);
  }

  @Test
  public void testEmptyBoundingBox() {
    BoundingBox boundingBox = new BoundingBox();

    // Assert all initial values are Infinity
    Assert.assertTrue(boundingBox.isValid());
    Assert.assertEquals(Double.POSITIVE_INFINITY, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, boundingBox.getYMax(), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, boundingBox.getZMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, boundingBox.getZMax(), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, boundingBox.getMMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, boundingBox.getMMax(), 0.0);
  }

  @Test
  public void testMergeBoundingBoxes() {
    BoundingBox boundingBox1 = new BoundingBox(0, 10, 0, 20, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
    BoundingBox boundingBox2 = new BoundingBox(5, 15, 10, 30, Double.NaN, Double.NaN, Double.NaN, Double.NaN);

    boundingBox1.merge(boundingBox2);

    Assert.assertTrue(boundingBox1.isValid());
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

    Assert.assertTrue(boundingBox1.isValid());
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

    // Check that the bounding box remains in its initial state
    Assert.assertTrue(boundingBox.isValid());
    Assert.assertEquals(Double.POSITIVE_INFINITY, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, boundingBox.getYMax(), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, boundingBox.getZMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, boundingBox.getZMax(), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, boundingBox.getMMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, boundingBox.getMMax(), 0.0);
  }

  @Test
  public void testMergeWithNaNValues() {
    // Test merging with NaN values in different dimensions
    BoundingBox box1 = new BoundingBox(0, 10, 0, 10, 0, 10, 0, 10);
    BoundingBox box2 = new BoundingBox(5, 15, Double.NaN, Double.NaN, 5, 15, Double.NaN, Double.NaN);

    box1.merge(box2);

    // Check that box1 is invalid after the merge
    Assert.assertFalse("Box1 should be invalid after the merge", box1.isValid());
  }

  @Test
  public void testUpdateWithAllNaNCoordinatesAfterValid() {
    GeometryFactory gf = new GeometryFactory();
    BoundingBox box = new BoundingBox();

    // First add a valid point
    box.update(gf.createPoint(new Coordinate(10, 20)));
    Assert.assertTrue(box.isValid());
    Assert.assertEquals(10.0, box.getXMin(), 0.0);
    Assert.assertEquals(10.0, box.getXMax(), 0.0);
    Assert.assertEquals(20.0, box.getYMin(), 0.0);
    Assert.assertEquals(20.0, box.getYMax(), 0.0);

    // Then update with all NaN coordinates - should not change valid values
    Point nanPoint = gf.createPoint(new Coordinate(Double.NaN, Double.NaN));
    box.update(nanPoint);

    Assert.assertFalse("Box should be empty after the merge", box.isXYEmpty());
    Assert.assertTrue("Box should be valid after the merge", box.isValid());
  }

  @Test
  public void testUpdate3DPoint() {
    GeometryFactory gf = new GeometryFactory();
    BoundingBox box = new BoundingBox();

    // Create a 3D point
    Coordinate coord = new Coordinate(10, 20, 30);
    Point point3D = gf.createPoint(coord);
    box.update(point3D);

    Assert.assertTrue(box.isValid());
    Assert.assertEquals(10.0, box.getXMin(), 0.0);
    Assert.assertEquals(10.0, box.getXMax(), 0.0);
    Assert.assertEquals(20.0, box.getYMin(), 0.0);
    Assert.assertEquals(20.0, box.getYMax(), 0.0);
    Assert.assertEquals(30.0, box.getZMin(), 0.0);
    Assert.assertEquals(30.0, box.getZMax(), 0.0);

    // Add another 3D point with different Z
    box.update(gf.createPoint(new Coordinate(15, 25, 10)));

    Assert.assertTrue(box.isValid());
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

    Assert.assertTrue(box.isValid());
    Assert.assertEquals(10.0, box.getXMin(), 0.0);
    Assert.assertEquals(10.0, box.getXMax(), 0.0);
    Assert.assertEquals(20.0, box.getYMin(), 0.0);
    Assert.assertEquals(20.0, box.getYMax(), 0.0);
    Assert.assertEquals(5.0, box.getMMin(), 0.0);
    Assert.assertEquals(5.0, box.getMMax(), 0.0);

    // Add another point with different M value
    CoordinateXYZM coord2 = new CoordinateXYZM(15, 25, Double.NaN, 10.0);
    box.update(gf.createPoint(coord2));

    Assert.assertTrue(box.isValid());
    Assert.assertEquals(10.0, box.getXMin(), 0.0);
    Assert.assertEquals(15.0, box.getXMax(), 0.0);
    Assert.assertEquals(20.0, box.getYMin(), 0.0);
    Assert.assertEquals(25.0, box.getYMax(), 0.0);
    Assert.assertEquals(5.0, box.getMMin(), 0.0);
    Assert.assertEquals(10.0, box.getMMax(), 0.0);
  }

  @Test
  public void testResetAfterUpdate() {
    GeometryFactory gf = new GeometryFactory();
    BoundingBox box = new BoundingBox();

    // Update with a valid point
    box.update(gf.createPoint(new Coordinate(10, 20)));
    Assert.assertTrue(box.isValid());
    Assert.assertEquals(10.0, box.getXMin(), 0.0);

    // Reset the box
    box.reset();

    // All values should be reset to their initial state
    Assert.assertTrue(box.isValid());
    Assert.assertEquals(Double.POSITIVE_INFINITY, box.getXMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, box.getXMax(), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, box.getYMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, box.getYMax(), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, box.getZMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, box.getZMax(), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, box.getMMin(), 0.0);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, box.getMMax(), 0.0);

    // Update after reset should work correctly
    box.update(gf.createPoint(new Coordinate(30, 40)));
    Assert.assertTrue(box.isValid());
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
    Assert.assertTrue(original.isValid());
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
    Assert.assertTrue(original.isValid());
    Assert.assertEquals(1.0, original.getXMin(), 0.0);
  }

  @Test
  public void testMergeWithAllNaNBox() {
    // Box with valid values
    BoundingBox box1 = new BoundingBox(1, 2, 3, 4, 5, 6, 7, 8);

    // Empty box with all NaN values
    BoundingBox box2 = new BoundingBox();

    // Merge should keep existing values
    box1.merge(box2);

    Assert.assertTrue(box1.isValid());
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

    Assert.assertTrue(box1.isValid());
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
    Assert.assertTrue(box.isValid());
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

    Assert.assertTrue(box2.isValid());
    Assert.assertEquals(5.0, box2.getXMin(), 0.0);
    Assert.assertEquals(5.0, box2.getXMax(), 0.0);
    Assert.assertEquals(6.0, box2.getYMin(), 0.0);
    Assert.assertEquals(6.0, box2.getYMax(), 0.0);

    // Test with all NaN coordinates
    BoundingBox box3 = new BoundingBox();
    Coordinate[] coords3 =
        new Coordinate[] {new Coordinate(Double.NaN, Double.NaN), new Coordinate(Double.NaN, Double.NaN)};

    box3.update(gf.createLineString(coords3));

    // The bounding box should remain empty
    Assert.assertTrue(box3.isValid());
    Assert.assertTrue(box3.isXYEmpty());
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
    Assert.assertTrue(box.isValid());
    Assert.assertEquals(0.0, box.getXMin(), 0.0);
    Assert.assertEquals(2.0, box.getXMax(), 0.0);
    Assert.assertEquals(1.0, box.getYMin(), 0.0);
    Assert.assertEquals(3.0, box.getYMax(), 0.0);

    // Test with mixed NaN values in different components
    BoundingBox box2 = new BoundingBox();
    Coordinate[] coords2 =
        new Coordinate[] {new Coordinate(Double.NaN, 5), new Coordinate(6, Double.NaN), new Coordinate(7, 8)};

    box2.update(gf.createLineString(coords2));
    Assert.assertTrue(box2.isValid());
    Assert.assertTrue(box2.isXYEmpty());
  }

  /**
   * Tests the end-to-end case for updating and merging bounding boxes with mixed valid and NaN coordinates.
   *
   * Scenario - Parquet file with multiple row groups:
   * file-level bbox: [1, 9, 100, 900]
   *
   * Row group 1: [1, 2, 100, 100]
   * - POINT (1, 100)
   * - POINT (2, NaN)
   *
   * Row group 2: [3, 3, 300, 300]
   * - POINT (3, 300)
   * - POINT (NaN, NaN)
   *
   * Row group 3: no valid bbox
   * - POINT (5, NaN)
   * - POINT (6, NaN)
   *
   * Row group 4: [7, 8, 700, 800]
   * - POINT (7, 700)
   * - POINT (8, 800)
   *
   * Row group 5: no valid bbox
   * - POINT (NaN, NaN)
   * - POINT (NaN, NaN)
   *
   * Row group 6: [9, 9, 900, 900]
   * - POINT (9, 900)
   * - LINESTRING EMPTY
   *
   * The test verifies that:
   * 1. Individual row group bounding boxes correctly handle NaN coordinates
   * 2. The merge operation correctly combines valid bounding boxes and ignores invalid ones
   * 3. The resulting file-level bounding box correctly represents the overall spatial extent [1, 8, 100, 800]
   * 4. The merge operation is commutative - the order of merging does not affect the result
   */
  @Test
  public void testMergingRowGroupBoundingBoxes() {
    GeometryFactory gf = new GeometryFactory();

    // File-level bounding box (to be computed by merging row group boxes)
    BoundingBox fileBBox = new BoundingBox();

    // Row Group 1: [1, 2, 100, 100]
    BoundingBox rowGroup1 = new BoundingBox();
    rowGroup1.update(gf.createPoint(new Coordinate(1, 100)));
    // Point with NaN Y-coordinate
    rowGroup1.update(gf.createPoint(new Coordinate(2, Double.NaN)));

    // Verify Row Group 1
    Assert.assertTrue(rowGroup1.isValid());
    Assert.assertEquals(1.0, rowGroup1.getXMin(), 0.0);
    Assert.assertEquals(2.0, rowGroup1.getXMax(), 0.0);
    Assert.assertEquals(100.0, rowGroup1.getYMin(), 0.0);
    Assert.assertEquals(100.0, rowGroup1.getYMax(), 0.0);
    Assert.assertTrue(rowGroup1.isValid());

    // Row Group 2: [3, 3, 300, 300]
    BoundingBox rowGroup2 = new BoundingBox();
    rowGroup2.update(gf.createPoint(new Coordinate(3, 300)));
    // Point with all NaN coordinates
    Coordinate nanCoord = new Coordinate(Double.NaN, Double.NaN);
    rowGroup2.update(gf.createPoint(nanCoord));

    // Verify Row Group 2
    Assert.assertTrue(rowGroup2.isValid());
    Assert.assertEquals(3.0, rowGroup2.getXMin(), 0.0);
    Assert.assertEquals(3.0, rowGroup2.getXMax(), 0.0);
    Assert.assertEquals(300.0, rowGroup2.getYMin(), 0.0);
    Assert.assertEquals(300.0, rowGroup2.getYMax(), 0.0);
    Assert.assertTrue(rowGroup2.isValid());

    // Row Group 3: No defined bbox due to NaN Y-coordinates
    BoundingBox rowGroup3 = new BoundingBox();
    rowGroup3.update(gf.createPoint(new Coordinate(5, Double.NaN)));
    rowGroup3.update(gf.createPoint(new Coordinate(6, Double.NaN)));

    // Verify Row Group 3
    Assert.assertTrue(rowGroup3.isXYEmpty());

    // Row Group 4: [7, 8, 700, 800]
    BoundingBox rowGroup4 = new BoundingBox();
    rowGroup4.update(gf.createPoint(new Coordinate(7, 700)));
    rowGroup4.update(gf.createPoint(new Coordinate(8, 800)));

    // Verify Row Group 4
    Assert.assertTrue(rowGroup4.isValid());
    Assert.assertEquals(7.0, rowGroup4.getXMin(), 0.0);
    Assert.assertEquals(8.0, rowGroup4.getXMax(), 0.0);
    Assert.assertEquals(700.0, rowGroup4.getYMin(), 0.0);
    Assert.assertEquals(800.0, rowGroup4.getYMax(), 0.0);
    Assert.assertTrue(rowGroup4.isValid());

    // Row Group 5: No defined bbox due to all NaN coordinates
    BoundingBox rowGroup5 = new BoundingBox();
    rowGroup5.update(gf.createPoint(nanCoord));
    rowGroup5.update(gf.createPoint(nanCoord));

    // Verify Row Group 5
    Assert.assertTrue(rowGroup5.isXYEmpty());

    // Row Group 6: Test mixing an empty geometry with a valid point [9, 9, 900, 900]
    BoundingBox rowGroup6 = new BoundingBox();
    // Create an empty LineString
    LineString emptyLineString = gf.createLineString(new Coordinate[0]);
    // Create a valid point
    Coordinate pointCoord = new Coordinate(9, 900);
    Point validPoint = gf.createPoint(pointCoord);

    // Update the bounding box with both geometries
    rowGroup6.update(emptyLineString); // This should be a no-op
    rowGroup6.update(validPoint); // This should set the bounds

    // Verify Row Group 6
    Assert.assertTrue(rowGroup6.isValid());
    Assert.assertEquals(9.0, rowGroup6.getXMin(), 0.0);
    Assert.assertEquals(9.0, rowGroup6.getXMax(), 0.0);
    Assert.assertEquals(900.0, rowGroup6.getYMin(), 0.0);
    Assert.assertEquals(900.0, rowGroup6.getYMax(), 0.0);

    // Merge row group boxes into file-level box
    fileBBox.merge(rowGroup1);
    fileBBox.merge(rowGroup2);
    fileBBox.merge(rowGroup3);
    fileBBox.merge(rowGroup4);
    fileBBox.merge(rowGroup5);
    fileBBox.merge(rowGroup6);

    // Verify file-level bounding box
    // Note: Now includes point (9, 900) from rowGroup6
    Assert.assertTrue(fileBBox.isValid());
    Assert.assertEquals(1.0, fileBBox.getXMin(), 0.0);
    Assert.assertEquals(9.0, fileBBox.getXMax(), 0.0);
    Assert.assertEquals(100.0, fileBBox.getYMin(), 0.0);
    Assert.assertEquals(900.0, fileBBox.getYMax(), 0.0);
    Assert.assertTrue(fileBBox.isValid());

    // Test merging in reverse order to ensure commutativity
    BoundingBox reverseMergeBox = new BoundingBox();
    reverseMergeBox.merge(rowGroup6);
    reverseMergeBox.merge(rowGroup5);
    reverseMergeBox.merge(rowGroup4);
    reverseMergeBox.merge(rowGroup3);
    reverseMergeBox.merge(rowGroup2);
    reverseMergeBox.merge(rowGroup1);

    Assert.assertTrue(reverseMergeBox.isValid());
    Assert.assertEquals(1.0, reverseMergeBox.getXMin(), 0.0);
    Assert.assertEquals(9.0, reverseMergeBox.getXMax(), 0.0);
    Assert.assertEquals(100.0, reverseMergeBox.getYMin(), 0.0);
    Assert.assertEquals(900.0, reverseMergeBox.getYMax(), 0.0);
    Assert.assertTrue(reverseMergeBox.isValid());
  }
}
