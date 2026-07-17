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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

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

    assertThat(boundingBox.isValid()).isTrue();
    assertThat(boundingBox.getXMin()).isCloseTo(10.0, offset(0.0));
    assertThat(boundingBox.getXMax()).isCloseTo(10.0, offset(0.0));
    assertThat(boundingBox.getYMin()).isCloseTo(20.0, offset(0.0));
    assertThat(boundingBox.getYMax()).isCloseTo(20.0, offset(0.0));
  }

  @Test
  public void testEmptyGeometry() {
    GeometryFactory geometryFactory = new GeometryFactory();
    BoundingBox boundingBox = new BoundingBox();

    // Create an empty point
    Point emptyPoint = geometryFactory.createPoint();
    boundingBox.update(emptyPoint);

    // Empty geometry should retain the initial state
    assertThat(boundingBox.isValid()).isTrue();
    assertThat(boundingBox.getXMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getXMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getYMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getYMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));

    // Test that after adding a non-empty geometry, values are updated correctly
    Point point = geometryFactory.createPoint(new Coordinate(10, 20));
    boundingBox.update(point);
    assertThat(boundingBox.isValid()).isTrue();
    assertThat(boundingBox.getXMin()).isCloseTo(10.0, offset(0.0));
    assertThat(boundingBox.getXMax()).isCloseTo(10.0, offset(0.0));
    assertThat(boundingBox.getYMin()).isCloseTo(20.0, offset(0.0));
    assertThat(boundingBox.getYMax()).isCloseTo(20.0, offset(0.0));

    // Update with another empty geometry, should not change the bounds
    boundingBox.update(emptyPoint);
    assertThat(boundingBox.isValid()).isTrue();
    assertThat(boundingBox.getXMin()).isCloseTo(10.0, offset(0.0));
    assertThat(boundingBox.getXMax()).isCloseTo(10.0, offset(0.0));
    assertThat(boundingBox.getYMin()).isCloseTo(20.0, offset(0.0));
    assertThat(boundingBox.getYMax()).isCloseTo(20.0, offset(0.0));
  }

  @Test
  public void testNaNCoordinates() {
    GeometryFactory geometryFactory = new GeometryFactory();
    BoundingBox boundingBox = new BoundingBox();

    // Create a point with NaN coordinates
    Point nanPoint = geometryFactory.createPoint(new Coordinate(Double.NaN, Double.NaN));
    boundingBox.update(nanPoint);

    // All values should be NaN after updating with all-NaN coordinates
    assertThat(boundingBox.isValid()).isTrue();
    assertThat(boundingBox.isXYEmpty()).isTrue();

    // Reset the bounding box for the next test
    boundingBox = new BoundingBox();

    // Create a mixed point with a valid coordinate and a NaN coordinate
    Point mixedPoint = geometryFactory.createPoint(new Coordinate(15.0, Double.NaN));
    boundingBox.update(mixedPoint);

    // The valid X coordinate should be used, Y should remain at initial values
    assertThat(boundingBox.isValid()).isTrue();
    assertThat(boundingBox.isXYEmpty()).isTrue();
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
    assertThat(boundingBox.isValid()).isTrue();
    assertThat(boundingBox.getXMin()).isCloseTo(10.0, offset(0.0));
    assertThat(boundingBox.getXMax()).isCloseTo(10.0, offset(0.0));
    assertThat(boundingBox.getYMin()).isCloseTo(20.0, offset(0.0));
    assertThat(boundingBox.getYMax()).isCloseTo(20.0, offset(0.0));

    // Add a point with valid Z value
    Coordinate coord2 = new Coordinate(15, 25, 30); // Using constructor with Z
    Point validZPoint = geometryFactory.createPoint(coord2);
    boundingBox.update(validZPoint);

    // X, Y, and Z values should now be updated
    assertThat(boundingBox.isValid()).isTrue();
    assertThat(boundingBox.getXMin()).isCloseTo(10.0, offset(0.0));
    assertThat(boundingBox.getXMax()).isCloseTo(15.0, offset(0.0));
    assertThat(boundingBox.getYMin()).isCloseTo(20.0, offset(0.0));
    assertThat(boundingBox.getYMax()).isCloseTo(25.0, offset(0.0));
    assertThat(boundingBox.getZMin()).isCloseTo(30.0, offset(0.0));
    assertThat(boundingBox.getZMax()).isCloseTo(30.0, offset(0.0));

    // Reset the bounding box for M value tests
    boundingBox.reset();

    // Create a point with NaN M value
    CoordinateXYZM coordNanM = new CoordinateXYZM(10, 20, 30, Double.NaN);
    Point nanMPoint = geometryFactory.createPoint(coordNanM);
    boundingBox.update(nanMPoint);

    // X, Y, Z should be updated, but M should remain at initial values
    assertThat(boundingBox.isValid()).isTrue();
    assertThat(boundingBox.getXMin()).isCloseTo(10.0, offset(0.0));
    assertThat(boundingBox.getXMax()).isCloseTo(10.0, offset(0.0));
    assertThat(boundingBox.getYMin()).isCloseTo(20.0, offset(0.0));
    assertThat(boundingBox.getYMax()).isCloseTo(20.0, offset(0.0));
    assertThat(boundingBox.getZMin()).isCloseTo(30.0, offset(0.0));
    assertThat(boundingBox.getZMax()).isCloseTo(30.0, offset(0.0));
    assertThat(boundingBox.getMMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getMMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));

    // Add a point with valid M value
    CoordinateXYZM coordValidM = new CoordinateXYZM(15, 25, 35, 40);
    Point validMPoint = geometryFactory.createPoint(coordValidM);
    boundingBox.update(validMPoint);

    // All values including M should now be updated
    assertThat(boundingBox.isValid()).isTrue();
    assertThat(boundingBox.getXMin()).isCloseTo(10.0, offset(0.0));
    assertThat(boundingBox.getXMax()).isCloseTo(15.0, offset(0.0));
    assertThat(boundingBox.getYMin()).isCloseTo(20.0, offset(0.0));
    assertThat(boundingBox.getYMax()).isCloseTo(25.0, offset(0.0));
    assertThat(boundingBox.getZMin()).isCloseTo(30.0, offset(0.0));
    assertThat(boundingBox.getZMax()).isCloseTo(35.0, offset(0.0));
    assertThat(boundingBox.getMMin()).isCloseTo(40.0, offset(0.0));
    assertThat(boundingBox.getMMax()).isCloseTo(40.0, offset(0.0));
  }

  @Test
  public void testAbort() {
    GeometryFactory geometryFactory = new GeometryFactory();
    BoundingBox boundingBox = new BoundingBox();

    // Create a valid point
    Point validPoint = geometryFactory.createPoint(new Coordinate(10, 20));
    boundingBox.update(validPoint);

    // Check initial values
    assertThat(boundingBox.isValid()).isTrue();
    assertThat(boundingBox.getXMin()).isCloseTo(10.0, offset(0.0));
    assertThat(boundingBox.getXMax()).isCloseTo(10.0, offset(0.0));
    assertThat(boundingBox.getYMin()).isCloseTo(20.0, offset(0.0));
    assertThat(boundingBox.getYMax()).isCloseTo(20.0, offset(0.0));

    // Abort the update
    boundingBox.abort();

    // Check that values are reset to initial state
    assertThat(boundingBox.isValid()).isFalse();
    assertThat(boundingBox.getXMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getXMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getYMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getYMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getZMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getZMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getMMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getMMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));
  }

  @Test
  public void testEmptyBoundingBox() {
    BoundingBox boundingBox = new BoundingBox();

    // Assert all initial values are Infinity
    assertThat(boundingBox.isValid()).isTrue();
    assertThat(boundingBox.getXMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getXMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getYMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getYMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getZMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getZMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getMMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getMMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));
  }

  @Test
  public void testMergeBoundingBoxes() {
    BoundingBox boundingBox1 = new BoundingBox(0, 10, 0, 20, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
    BoundingBox boundingBox2 = new BoundingBox(5, 15, 10, 30, Double.NaN, Double.NaN, Double.NaN, Double.NaN);

    boundingBox1.merge(boundingBox2);

    assertThat(boundingBox1.isValid()).isTrue();
    assertThat(boundingBox1.getXMin()).isCloseTo(0.0, offset(0.0));
    assertThat(boundingBox1.getXMax()).isCloseTo(15.0, offset(0.0));
    assertThat(boundingBox1.getYMin()).isCloseTo(0.0, offset(0.0));
    assertThat(boundingBox1.getYMax()).isCloseTo(30.0, offset(0.0));
    assertThat(boundingBox1.getZMin()).isNaN();
    assertThat(boundingBox1.getZMax()).isNaN();
    assertThat(boundingBox1.getMMin()).isNaN();
    assertThat(boundingBox1.getMMax()).isNaN();
  }

  @Test
  public void testMergeWithEmptyBoundingBox() {
    BoundingBox boundingBox1 = new BoundingBox(0, 10, 0, 20, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
    BoundingBox emptyBoundingBox = new BoundingBox();

    boundingBox1.merge(emptyBoundingBox);

    assertThat(boundingBox1.isValid()).isTrue();
    assertThat(boundingBox1.getXMin()).isCloseTo(0.0, offset(0.0));
    assertThat(boundingBox1.getXMax()).isCloseTo(10.0, offset(0.0));
    assertThat(boundingBox1.getYMin()).isCloseTo(0.0, offset(0.0));
    assertThat(boundingBox1.getYMax()).isCloseTo(20.0, offset(0.0));
    assertThat(boundingBox1.getZMin()).isNaN();
    assertThat(boundingBox1.getZMax()).isNaN();
    assertThat(boundingBox1.getMMin()).isNaN();
    assertThat(boundingBox1.getMMax()).isNaN();
  }

  @Test
  public void testUpdateWithNullGeometry() {
    BoundingBox boundingBox = new BoundingBox();
    boundingBox.update(null);

    // Check that the bounding box remains in its initial state
    assertThat(boundingBox.isValid()).isTrue();
    assertThat(boundingBox.getXMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getXMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getYMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getYMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getZMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getZMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getMMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(boundingBox.getMMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));
  }

  @Test
  public void testMergeWithNaNValues() {
    // Test merging with NaN values in different dimensions
    BoundingBox box1 = new BoundingBox(0, 10, 0, 10, 0, 10, 0, 10);
    BoundingBox box2 = new BoundingBox(5, 15, Double.NaN, Double.NaN, 5, 15, Double.NaN, Double.NaN);

    box1.merge(box2);

    // Check that box1 is invalid after the merge
    assertThat(box1.isValid()).as("Box1 should be invalid after the merge").isFalse();
  }

  @Test
  public void testUpdateWithAllNaNCoordinatesAfterValid() {
    GeometryFactory gf = new GeometryFactory();
    BoundingBox box = new BoundingBox();

    // First add a valid point
    box.update(gf.createPoint(new Coordinate(10, 20)));
    assertThat(box.isValid()).isTrue();
    assertThat(box.getXMin()).isCloseTo(10.0, offset(0.0));
    assertThat(box.getXMax()).isCloseTo(10.0, offset(0.0));
    assertThat(box.getYMin()).isCloseTo(20.0, offset(0.0));
    assertThat(box.getYMax()).isCloseTo(20.0, offset(0.0));

    // Then update with all NaN coordinates - should not change valid values
    Point nanPoint = gf.createPoint(new Coordinate(Double.NaN, Double.NaN));
    box.update(nanPoint);

    assertThat(box.isXYEmpty()).as("Box should be empty after the merge").isFalse();
    assertThat(box.isValid()).as("Box should be valid after the merge").isTrue();
  }

  @Test
  public void testUpdate3DPoint() {
    GeometryFactory gf = new GeometryFactory();
    BoundingBox box = new BoundingBox();

    // Create a 3D point
    Coordinate coord = new Coordinate(10, 20, 30);
    Point point3D = gf.createPoint(coord);
    box.update(point3D);

    assertThat(box.isValid()).isTrue();
    assertThat(box.getXMin()).isCloseTo(10.0, offset(0.0));
    assertThat(box.getXMax()).isCloseTo(10.0, offset(0.0));
    assertThat(box.getYMin()).isCloseTo(20.0, offset(0.0));
    assertThat(box.getYMax()).isCloseTo(20.0, offset(0.0));
    assertThat(box.getZMin()).isCloseTo(30.0, offset(0.0));
    assertThat(box.getZMax()).isCloseTo(30.0, offset(0.0));

    // Add another 3D point with different Z
    box.update(gf.createPoint(new Coordinate(15, 25, 10)));

    assertThat(box.isValid()).isTrue();
    assertThat(box.getXMin()).isCloseTo(10.0, offset(0.0));
    assertThat(box.getXMax()).isCloseTo(15.0, offset(0.0));
    assertThat(box.getYMin()).isCloseTo(20.0, offset(0.0));
    assertThat(box.getYMax()).isCloseTo(25.0, offset(0.0));
    assertThat(box.getZMin()).isCloseTo(10.0, offset(0.0));
    assertThat(box.getZMax()).isCloseTo(30.0, offset(0.0));
  }

  @Test
  public void testUpdateWithMeasureValue() {
    GeometryFactory gf = new GeometryFactory();
    BoundingBox box = new BoundingBox();

    // Create a point with M value using CoordinateXYZM instead of setM
    CoordinateXYZM coord = new CoordinateXYZM(10, 20, Double.NaN, 5.0);
    Point pointWithM = gf.createPoint(coord);
    box.update(pointWithM);

    assertThat(box.isValid()).isTrue();
    assertThat(box.getXMin()).isCloseTo(10.0, offset(0.0));
    assertThat(box.getXMax()).isCloseTo(10.0, offset(0.0));
    assertThat(box.getYMin()).isCloseTo(20.0, offset(0.0));
    assertThat(box.getYMax()).isCloseTo(20.0, offset(0.0));
    assertThat(box.getMMin()).isCloseTo(5.0, offset(0.0));
    assertThat(box.getMMax()).isCloseTo(5.0, offset(0.0));

    // Add another point with different M value
    CoordinateXYZM coord2 = new CoordinateXYZM(15, 25, Double.NaN, 10.0);
    box.update(gf.createPoint(coord2));

    assertThat(box.isValid()).isTrue();
    assertThat(box.getXMin()).isCloseTo(10.0, offset(0.0));
    assertThat(box.getXMax()).isCloseTo(15.0, offset(0.0));
    assertThat(box.getYMin()).isCloseTo(20.0, offset(0.0));
    assertThat(box.getYMax()).isCloseTo(25.0, offset(0.0));
    assertThat(box.getMMin()).isCloseTo(5.0, offset(0.0));
    assertThat(box.getMMax()).isCloseTo(10.0, offset(0.0));
  }

  @Test
  public void testResetAfterUpdate() {
    GeometryFactory gf = new GeometryFactory();
    BoundingBox box = new BoundingBox();

    // Update with a valid point
    box.update(gf.createPoint(new Coordinate(10, 20)));
    assertThat(box.isValid()).isTrue();
    assertThat(box.getXMin()).isCloseTo(10.0, offset(0.0));

    // Reset the box
    box.reset();

    // All values should be reset to their initial state
    assertThat(box.isValid()).isTrue();
    assertThat(box.getXMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(box.getXMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));
    assertThat(box.getYMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(box.getYMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));
    assertThat(box.getZMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(box.getZMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));
    assertThat(box.getMMin()).isCloseTo(Double.POSITIVE_INFINITY, offset(0.0));
    assertThat(box.getMMax()).isCloseTo(Double.NEGATIVE_INFINITY, offset(0.0));

    // Update after reset should work correctly
    box.update(gf.createPoint(new Coordinate(30, 40)));
    assertThat(box.isValid()).isTrue();
    assertThat(box.getXMin()).isCloseTo(30.0, offset(0.0));
    assertThat(box.getXMax()).isCloseTo(30.0, offset(0.0));
    assertThat(box.getYMin()).isCloseTo(40.0, offset(0.0));
    assertThat(box.getYMax()).isCloseTo(40.0, offset(0.0));
  }

  @Test
  public void testCopy() {
    // Create and populate a bounding box
    BoundingBox original = new BoundingBox(1, 2, 3, 4, 5, 6, 7, 8);

    // Create copy
    BoundingBox copy = original.copy();

    // Verify all values are copied correctly
    assertThat(original.isValid()).isTrue();
    assertThat(copy.getXMin()).isCloseTo(original.getXMin(), offset(0.0));
    assertThat(copy.getXMax()).isCloseTo(original.getXMax(), offset(0.0));
    assertThat(copy.getYMin()).isCloseTo(original.getYMin(), offset(0.0));
    assertThat(copy.getYMax()).isCloseTo(original.getYMax(), offset(0.0));
    assertThat(copy.getZMin()).isCloseTo(original.getZMin(), offset(0.0));
    assertThat(copy.getZMax()).isCloseTo(original.getZMax(), offset(0.0));
    assertThat(copy.getMMin()).isCloseTo(original.getMMin(), offset(0.0));
    assertThat(copy.getMMax()).isCloseTo(original.getMMax(), offset(0.0));

    // Modify the copy and verify original is unchanged
    copy.reset();
    assertThat(original.isValid()).isTrue();
    assertThat(original.getXMin()).isCloseTo(1.0, offset(0.0));
  }

  @Test
  public void testMergeWithAllNaNBox() {
    // Box with valid values
    BoundingBox box1 = new BoundingBox(1, 2, 3, 4, 5, 6, 7, 8);

    // Empty box with all NaN values
    BoundingBox box2 = new BoundingBox();

    // Merge should keep existing values
    box1.merge(box2);

    assertThat(box1.isValid()).isTrue();
    assertThat(box1.getXMin()).isCloseTo(1.0, offset(0.0));
    assertThat(box1.getXMax()).isCloseTo(2.0, offset(0.0));
    assertThat(box1.getYMin()).isCloseTo(3.0, offset(0.0));
    assertThat(box1.getYMax()).isCloseTo(4.0, offset(0.0));
    assertThat(box1.getZMin()).isCloseTo(5.0, offset(0.0));
    assertThat(box1.getZMax()).isCloseTo(6.0, offset(0.0));
    assertThat(box1.getMMin()).isCloseTo(7.0, offset(0.0));
    assertThat(box1.getMMax()).isCloseTo(8.0, offset(0.0));

    // Test the reverse - NaN box merging with valid box
    BoundingBox box3 = new BoundingBox();
    box3.merge(box1);

    assertThat(box1.isValid()).isTrue();
    assertThat(box3.getXMin()).isCloseTo(1.0, offset(0.0));
    assertThat(box3.getXMax()).isCloseTo(2.0, offset(0.0));
    assertThat(box3.getYMin()).isCloseTo(3.0, offset(0.0));
    assertThat(box3.getYMax()).isCloseTo(4.0, offset(0.0));
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
    assertThat(box.isValid()).isTrue();
    assertThat(box.getXMin()).isCloseTo(0.0, offset(0.0));
    assertThat(box.getXMax()).isCloseTo(2.0, offset(0.0));
    assertThat(box.getYMin()).isCloseTo(1.0, offset(0.0));
    assertThat(box.getYMax()).isCloseTo(3.0, offset(0.0));

    // Test with only one valid coordinate
    BoundingBox box2 = new BoundingBox();
    Coordinate[] coords2 = new Coordinate[] {
      new Coordinate(5, 6), new Coordinate(Double.NaN, Double.NaN), new Coordinate(Double.NaN, Double.NaN)
    };

    box2.update(gf.createLineString(coords2));

    assertThat(box2.isValid()).isTrue();
    assertThat(box2.getXMin()).isCloseTo(5.0, offset(0.0));
    assertThat(box2.getXMax()).isCloseTo(5.0, offset(0.0));
    assertThat(box2.getYMin()).isCloseTo(6.0, offset(0.0));
    assertThat(box2.getYMax()).isCloseTo(6.0, offset(0.0));

    // Test with all NaN coordinates
    BoundingBox box3 = new BoundingBox();
    Coordinate[] coords3 =
        new Coordinate[] {new Coordinate(Double.NaN, Double.NaN), new Coordinate(Double.NaN, Double.NaN)};

    box3.update(gf.createLineString(coords3));

    // The bounding box should remain empty
    assertThat(box3.isValid()).isTrue();
    assertThat(box3.isXYEmpty()).isTrue();
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
    assertThat(box.isValid()).isTrue();
    assertThat(box.getXMin()).isCloseTo(0.0, offset(0.0));
    assertThat(box.getXMax()).isCloseTo(2.0, offset(0.0));
    assertThat(box.getYMin()).isCloseTo(1.0, offset(0.0));
    assertThat(box.getYMax()).isCloseTo(3.0, offset(0.0));

    // Test with mixed NaN values in different components
    BoundingBox box2 = new BoundingBox();
    Coordinate[] coords2 =
        new Coordinate[] {new Coordinate(Double.NaN, 5), new Coordinate(6, Double.NaN), new Coordinate(7, 8)};

    box2.update(gf.createLineString(coords2));
    assertThat(box2.isValid()).isTrue();
    assertThat(box2.isXYEmpty()).isTrue();
  }

  /**
   * Tests the end-to-end case for updating and merging bounding boxes with mixed valid and NaN coordinates.
   * <p>
   * Scenario - Parquet file with multiple row groups:
   * file-level bbox: [1, 9, 100, 900]
   * <p>
   * Row group 1: [1, 2, 100, 100]
   * - POINT (1, 100)
   * - POINT (2, NaN)
   * <p>
   * Row group 2: [3, 3, 300, 300]
   * - POINT (3, 300)
   * - POINT (NaN, NaN)
   * <p>
   * Row group 3: no valid bbox
   * - POINT (5, NaN)
   * - POINT (6, NaN)
   * <p>
   * Row group 4: [7, 8, 700, 800]
   * - POINT (7, 700)
   * - POINT (8, 800)
   * <p>
   * Row group 5: no valid bbox
   * - POINT (NaN, NaN)
   * - POINT (NaN, NaN)
   * <p>
   * Row group 6: [9, 9, 900, 900]
   * - POINT (9, 900)
   * - LINESTRING EMPTY
   * <p>
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
    assertThat(rowGroup1.isValid()).isTrue();
    assertThat(rowGroup1.getXMin()).isCloseTo(1.0, offset(0.0));
    assertThat(rowGroup1.getXMax()).isCloseTo(2.0, offset(0.0));
    assertThat(rowGroup1.getYMin()).isCloseTo(100.0, offset(0.0));
    assertThat(rowGroup1.getYMax()).isCloseTo(100.0, offset(0.0));
    assertThat(rowGroup1.isValid()).isTrue();

    // Row Group 2: [3, 3, 300, 300]
    BoundingBox rowGroup2 = new BoundingBox();
    rowGroup2.update(gf.createPoint(new Coordinate(3, 300)));
    // Point with all NaN coordinates
    Coordinate nanCoord = new Coordinate(Double.NaN, Double.NaN);
    rowGroup2.update(gf.createPoint(nanCoord));

    // Verify Row Group 2
    assertThat(rowGroup2.isValid()).isTrue();
    assertThat(rowGroup2.getXMin()).isCloseTo(3.0, offset(0.0));
    assertThat(rowGroup2.getXMax()).isCloseTo(3.0, offset(0.0));
    assertThat(rowGroup2.getYMin()).isCloseTo(300.0, offset(0.0));
    assertThat(rowGroup2.getYMax()).isCloseTo(300.0, offset(0.0));
    assertThat(rowGroup2.isValid()).isTrue();

    // Row Group 3: No defined bbox due to NaN Y-coordinates
    BoundingBox rowGroup3 = new BoundingBox();
    rowGroup3.update(gf.createPoint(new Coordinate(5, Double.NaN)));
    rowGroup3.update(gf.createPoint(new Coordinate(6, Double.NaN)));

    // Verify Row Group 3
    assertThat(rowGroup3.isXYEmpty()).isTrue();

    // Row Group 4: [7, 8, 700, 800]
    BoundingBox rowGroup4 = new BoundingBox();
    rowGroup4.update(gf.createPoint(new Coordinate(7, 700)));
    rowGroup4.update(gf.createPoint(new Coordinate(8, 800)));

    // Verify Row Group 4
    assertThat(rowGroup4.isValid()).isTrue();
    assertThat(rowGroup4.getXMin()).isCloseTo(7.0, offset(0.0));
    assertThat(rowGroup4.getXMax()).isCloseTo(8.0, offset(0.0));
    assertThat(rowGroup4.getYMin()).isCloseTo(700.0, offset(0.0));
    assertThat(rowGroup4.getYMax()).isCloseTo(800.0, offset(0.0));
    assertThat(rowGroup4.isValid()).isTrue();

    // Row Group 5: No defined bbox due to all NaN coordinates
    BoundingBox rowGroup5 = new BoundingBox();
    rowGroup5.update(gf.createPoint(nanCoord));
    rowGroup5.update(gf.createPoint(nanCoord));

    // Verify Row Group 5
    assertThat(rowGroup5.isXYEmpty()).isTrue();

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
    assertThat(rowGroup6.isValid()).isTrue();
    assertThat(rowGroup6.getXMin()).isCloseTo(9.0, offset(0.0));
    assertThat(rowGroup6.getXMax()).isCloseTo(9.0, offset(0.0));
    assertThat(rowGroup6.getYMin()).isCloseTo(900.0, offset(0.0));
    assertThat(rowGroup6.getYMax()).isCloseTo(900.0, offset(0.0));

    // Merge row group boxes into file-level box
    fileBBox.merge(rowGroup1);
    fileBBox.merge(rowGroup2);
    fileBBox.merge(rowGroup3);
    fileBBox.merge(rowGroup4);
    fileBBox.merge(rowGroup5);
    fileBBox.merge(rowGroup6);

    // Verify file-level bounding box
    // Note: Now includes point (9, 900) from rowGroup6
    assertThat(fileBBox.isValid()).isTrue();
    assertThat(fileBBox.getXMin()).isCloseTo(1.0, offset(0.0));
    assertThat(fileBBox.getXMax()).isCloseTo(9.0, offset(0.0));
    assertThat(fileBBox.getYMin()).isCloseTo(100.0, offset(0.0));
    assertThat(fileBBox.getYMax()).isCloseTo(900.0, offset(0.0));
    assertThat(fileBBox.isValid()).isTrue();

    // Test merging in reverse order to ensure commutativity
    BoundingBox reverseMergeBox = new BoundingBox();
    reverseMergeBox.merge(rowGroup6);
    reverseMergeBox.merge(rowGroup5);
    reverseMergeBox.merge(rowGroup4);
    reverseMergeBox.merge(rowGroup3);
    reverseMergeBox.merge(rowGroup2);
    reverseMergeBox.merge(rowGroup1);

    assertThat(reverseMergeBox.isValid()).isTrue();
    assertThat(reverseMergeBox.getXMin()).isCloseTo(1.0, offset(0.0));
    assertThat(reverseMergeBox.getXMax()).isCloseTo(9.0, offset(0.0));
    assertThat(reverseMergeBox.getYMin()).isCloseTo(100.0, offset(0.0));
    assertThat(reverseMergeBox.getYMax()).isCloseTo(900.0, offset(0.0));
    assertThat(reverseMergeBox.isValid()).isTrue();
  }

  @Test
  public void testIsXValidAndIsYValid() {
    // Test with valid X and Y
    BoundingBox validBox = new BoundingBox(1, 2, 3, 4, 5, 6, 7, 8);
    assertThat(validBox.isXValid()).isTrue();
    assertThat(validBox.isYValid()).isTrue();
    assertThat(validBox.isXYValid()).isTrue();
    assertThat(validBox.isZValid()).isTrue();
    assertThat(validBox.isMValid()).isTrue();

    // Test with invalid X (NaN)
    BoundingBox invalidXBox = new BoundingBox(Double.NaN, 2, 3, 4, 5, 6, 7, 8);
    assertThat(invalidXBox.isXValid()).isFalse();
    assertThat(invalidXBox.isYValid()).isTrue();
    assertThat(invalidXBox.isXYValid()).isFalse();
    assertThat(invalidXBox.isZValid()).isTrue();
    assertThat(invalidXBox.isMValid()).isTrue();

    // Test with invalid Y (NaN)
    BoundingBox invalidYBox = new BoundingBox(1, 2, Double.NaN, 4, 5, 6, 7, 8);
    assertThat(invalidYBox.isXValid()).isTrue();
    assertThat(invalidYBox.isYValid()).isFalse();
    assertThat(invalidXBox.isXYValid()).isFalse();
    assertThat(invalidXBox.isZValid()).isTrue();
    assertThat(invalidXBox.isMValid()).isTrue();

    // Test with both X and Y invalid
    BoundingBox invalidXYBox = new BoundingBox(Double.NaN, Double.NaN, Double.NaN, Double.NaN, 5, 6, 7, 8);
    assertThat(invalidXYBox.isXValid()).isFalse();
    assertThat(invalidXYBox.isYValid()).isFalse();
    assertThat(invalidXYBox.isXYValid()).isFalse();
    assertThat(invalidXBox.isZValid()).isTrue();
    assertThat(invalidXBox.isMValid()).isTrue();
  }

  @Test
  public void testIsXEmptyAndIsYEmpty() {
    // Empty bounding box (initial state)
    BoundingBox emptyBox = new BoundingBox();
    assertThat(emptyBox.isXEmpty()).isTrue();
    assertThat(emptyBox.isYEmpty()).isTrue();
    assertThat(emptyBox.isXYEmpty()).isTrue();

    // Non-empty box
    BoundingBox nonEmptyBox = new BoundingBox(1, 2, 3, 4, 5, 6, 7, 8);
    assertThat(nonEmptyBox.isXEmpty()).isFalse();
    assertThat(nonEmptyBox.isYEmpty()).isFalse();
    assertThat(nonEmptyBox.isXYEmpty()).isFalse();

    // Box with empty X dimension only
    GeometryFactory gf = new GeometryFactory();
    BoundingBox emptyXBox = new BoundingBox();
    // Only update Y dimension
    emptyXBox.update(gf.createPoint(new Coordinate(Double.NaN, 5)));
    assertThat(emptyXBox.isXEmpty()).isTrue();
    assertThat(emptyXBox.isYEmpty()).isFalse();
    assertThat(emptyXBox.isXYEmpty()).isTrue();

    // Box with empty Y dimension only
    BoundingBox emptyYBox = new BoundingBox();
    // Only update X dimension
    emptyYBox.update(gf.createPoint(new Coordinate(10, Double.NaN)));
    assertThat(emptyYBox.isXEmpty()).isFalse();
    assertThat(emptyYBox.isYEmpty()).isTrue();
    assertThat(emptyYBox.isXYEmpty()).isTrue();
  }

  @Test
  public void testIsXWraparound() {
    // Normal bounding box (no wraparound)
    BoundingBox normalBox = new BoundingBox(1, 2, 3, 4, 5, 6, 7, 8);
    assertThat(normalBox.isXWraparound()).isFalse();

    // Wraparound box (xMin > xMax)
    BoundingBox wraparoundBox = new BoundingBox(170, 20, 10, 20, 0, 0, 0, 0);
    assertThat(wraparoundBox.isXWraparound()).isTrue();

    // Edge case: equal bounds
    BoundingBox equalBoundsBox = new BoundingBox(10, 10, 20, 20, 0, 0, 0, 0);
    assertThat(equalBoundsBox.isXWraparound()).isFalse();

    // Test static method directly
    assertThat(BoundingBox.isWraparound(180, -180)).isTrue();
    assertThat(BoundingBox.isWraparound(-180, 180)).isFalse();

    // Test with infinity values
    assertThat(BoundingBox.isWraparound(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY))
        .isFalse();
    assertThat(BoundingBox.isWraparound(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY))
        .isFalse();
    assertThat(BoundingBox.isWraparound(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY))
        .isFalse();
    assertThat(BoundingBox.isWraparound(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY))
        .isFalse();

    // Check edge cases
    assertThat(BoundingBox.isWraparound(0.0, Double.POSITIVE_INFINITY)).isFalse();
    assertThat(BoundingBox.isWraparound(Double.NEGATIVE_INFINITY, 0.0)).isFalse();
  }

  @Test
  public void testWraparoundHandlingInMerge() {
    // Test with two normal boxes
    BoundingBox box1 = new BoundingBox(10, 20, 10, 20, 0, 0, 0, 0);
    BoundingBox box2 = new BoundingBox(15, 25, 15, 25, 0, 0, 0, 0);
    box1.merge(box2);

    assertThat(box1.isValid()).isTrue();
    assertThat(box1.getXMin()).isCloseTo(10.0, offset(0.0));
    assertThat(box1.getXMax()).isCloseTo(25.0, offset(0.0));

    // Test with one wraparound box
    BoundingBox normalBox = new BoundingBox(0, 10, 0, 10, 0, 0, 0, 0);
    BoundingBox wraparoundBox = new BoundingBox(170, -170, 5, 15, 0, 0, 0, 0);

    normalBox.merge(wraparoundBox);

    assertThat(normalBox.isValid()).isFalse();
    assertThat(normalBox.getXMin()).isNaN();
    assertThat(normalBox.getXMax()).isNaN();
    assertThat(normalBox.getYMin()).isCloseTo(0.0, offset(0.0));
    assertThat(normalBox.getYMax()).isCloseTo(15.0, offset(0.0));
  }

  @Test
  public void testWraparoundBoxMergingNormalBox() {
    // Create a normal bounding box
    BoundingBox normalBox = new BoundingBox(0, 10, 0, 10, 0, 0, 0, 0);

    // Create a wraparound bounding box (xMin > xMax)
    BoundingBox wraparoundBox = new BoundingBox(170, -170, 5, 15, 0, 0, 0, 0);

    // Merge the normal box into the wraparound box
    wraparoundBox.merge(normalBox);

    // After merging, X dimension should be marked as invalid (NaN)
    // because we don't support merging wraparound bounds
    assertThat(wraparoundBox.isValid()).isFalse();
    assertThat(wraparoundBox.getXMin()).isNaN();
    assertThat(wraparoundBox.getXMax()).isNaN();

    // Y dimension should be properly merged
    assertThat(wraparoundBox.getYMin()).isCloseTo(0.0, offset(0.0));
    assertThat(wraparoundBox.getYMax()).isCloseTo(15.0, offset(0.0));
  }
}
