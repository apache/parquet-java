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
import org.locationtech.jts.geom.Polygon;

public class BoundingBoxTest {

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
  public void testWraparound() {
    GeometryFactory geometryFactory = new GeometryFactory();
    BoundingBox boundingBox = new BoundingBox();

    // Create a polygon near the antimeridian line
    Coordinate[] coords1 = new Coordinate[] {
      new Coordinate(170, 10), new Coordinate(175, 15), new Coordinate(170, 15), new Coordinate(170, 10)
    };
    Polygon polygon1 = geometryFactory.createPolygon(coords1);
    boundingBox.update(polygon1, "EPSG:4326");
    // Check if the wraparound is handled correctly
    Assert.assertEquals(170.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(175.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(10.0, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(15.0, boundingBox.getYMax(), 0.0);

    // Create an additional polygon crossing the antimeridian line
    Coordinate[] coords2 = new Coordinate[] {
      new Coordinate(175, -10), new Coordinate(-175, -5), new Coordinate(175, -5), new Coordinate(175, -10)
    };
    Polygon polygon2 = geometryFactory.createPolygon(coords2);

    boundingBox.update(polygon2, "EPSG:4326");
    // Check if the wraparound is handled correctly
    Assert.assertEquals(175.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(-175.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(-10.0, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(15.0, boundingBox.getYMax(), 0.0);

    // Create another polygon on the other side of the antimeridian line
    Coordinate[] coords3 = new Coordinate[] {
      new Coordinate(-170, 20), new Coordinate(-165, 25), new Coordinate(-170, 25), new Coordinate(-170, 20)
    };
    // longitude range: [-170, -165]
    Polygon polygon3 = geometryFactory.createPolygon(coords3);
    boundingBox.update(polygon3, "EPSG:4326");

    // Check if the wraparound is handled correctly
    Assert.assertEquals(175.0, boundingBox.getXMin(), 0.0);
    Assert.assertEquals(-175.0, boundingBox.getXMax(), 0.0);
    Assert.assertEquals(-10.0, boundingBox.getYMin(), 0.0);
    Assert.assertEquals(25.0, boundingBox.getYMax(), 0.0);
  }
}
