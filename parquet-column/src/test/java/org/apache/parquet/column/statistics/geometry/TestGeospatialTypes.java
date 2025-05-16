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

import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

public class TestGeospatialTypes {

  @Test
  public void testUpdateWithDifferentGeometryTypes() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes geospatialTypes = new GeospatialTypes();

    // Test with Point (type code 1)
    Point point = gf.createPoint(new Coordinate(1, 1));
    geospatialTypes.update(point);
    Assert.assertTrue(geospatialTypes.getTypes().contains(1));
    Assert.assertEquals(1, geospatialTypes.getTypes().size());

    // Test with LineString (type code 2)
    Coordinate[] lineCoords = new Coordinate[] {new Coordinate(1, 1), new Coordinate(2, 2)};
    LineString line = gf.createLineString(lineCoords);
    geospatialTypes.update(line);
    Assert.assertTrue(geospatialTypes.getTypes().contains(1));
    Assert.assertTrue(geospatialTypes.getTypes().contains(2));
    Assert.assertEquals(2, geospatialTypes.getTypes().size());

    // Test with Polygon (type code 3)
    Coordinate[] polygonCoords = new Coordinate[] {
      new Coordinate(0, 0), new Coordinate(1, 0),
      new Coordinate(1, 1), new Coordinate(0, 1),
      new Coordinate(0, 0)
    };
    LinearRing shell = gf.createLinearRing(polygonCoords);
    Polygon polygon = gf.createPolygon(shell);
    geospatialTypes.update(polygon);
    Assert.assertTrue(geospatialTypes.getTypes().contains(1));
    Assert.assertTrue(geospatialTypes.getTypes().contains(2));
    Assert.assertTrue(geospatialTypes.getTypes().contains(3));
    Assert.assertEquals(3, geospatialTypes.getTypes().size());
  }

  @Test
  public void testUpdateWithComplexGeometries() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes geospatialTypes = new GeospatialTypes();

    // MultiPoint (type code 4)
    Point[] points = new Point[] {gf.createPoint(new Coordinate(1, 1)), gf.createPoint(new Coordinate(2, 2))};
    MultiPoint multiPoint = gf.createMultiPoint(points);
    geospatialTypes.update(multiPoint);
    Assert.assertTrue(geospatialTypes.getTypes().contains(4));
    Assert.assertEquals(1, geospatialTypes.getTypes().size());

    // MultiLineString (type code 5)
    LineString[] lines = new LineString[] {
      gf.createLineString(new Coordinate[] {new Coordinate(1, 1), new Coordinate(2, 2)}),
      gf.createLineString(new Coordinate[] {new Coordinate(3, 3), new Coordinate(4, 4)})
    };
    MultiLineString multiLine = gf.createMultiLineString(lines);
    geospatialTypes.update(multiLine);
    Assert.assertTrue(geospatialTypes.getTypes().contains(4));
    Assert.assertTrue(geospatialTypes.getTypes().contains(5));
    Assert.assertEquals(2, geospatialTypes.getTypes().size());

    // MultiPolygon (type code 6)
    Polygon[] polygons = new Polygon[] {
      gf.createPolygon(gf.createLinearRing(new Coordinate[] {
        new Coordinate(0, 0), new Coordinate(1, 0),
        new Coordinate(1, 1), new Coordinate(0, 1),
        new Coordinate(0, 0)
      }))
    };
    MultiPolygon multiPolygon = gf.createMultiPolygon(polygons);
    geospatialTypes.update(multiPolygon);
    Assert.assertTrue(geospatialTypes.getTypes().contains(4));
    Assert.assertTrue(geospatialTypes.getTypes().contains(5));
    Assert.assertTrue(geospatialTypes.getTypes().contains(6));
    Assert.assertEquals(3, geospatialTypes.getTypes().size());

    // GeometryCollection (type code 7)
    GeometryCollection collection = gf.createGeometryCollection(
        new org.locationtech.jts.geom.Geometry[] {multiPoint, multiLine, multiPolygon});
    geospatialTypes.update(collection);
    Assert.assertTrue(geospatialTypes.getTypes().contains(4));
    Assert.assertTrue(geospatialTypes.getTypes().contains(5));
    Assert.assertTrue(geospatialTypes.getTypes().contains(6));
    Assert.assertTrue(geospatialTypes.getTypes().contains(7));
    Assert.assertEquals(4, geospatialTypes.getTypes().size());
  }

  @Test
  public void testUpdateWithZCoordinates() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes geospatialTypes = new GeospatialTypes();

    // Create a 3D point (XYZ) - should be type code 1001
    Point pointXYZ = gf.createPoint(new Coordinate(1, 1, 1));
    geospatialTypes.update(pointXYZ);
    Assert.assertTrue(geospatialTypes.getTypes().contains(1001));
    Assert.assertEquals(1, geospatialTypes.getTypes().size());
  }

  @Test
  public void testUpdateWithMCoordinates() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes geospatialTypes = new GeospatialTypes();

    // Create a point with measure (XYM) - should be type code 2001
    CoordinateXYZM coord = new CoordinateXYZM(1, 1, Double.NaN, 10);
    Point pointXYM = gf.createPoint(coord);
    geospatialTypes.update(pointXYM);
    Assert.assertTrue(geospatialTypes.getTypes().contains(2001));
    Assert.assertEquals(1, geospatialTypes.getTypes().size());
  }

  @Test
  public void testUpdateWithZMCoordinates() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes geospatialTypes = new GeospatialTypes();

    // Create a 4D point (XYZM) - should be type code 3001
    CoordinateXYZM coord = new CoordinateXYZM(1, 1, 1, 10);
    Point pointXYZM = gf.createPoint(coord);
    geospatialTypes.update(pointXYZM);
    Assert.assertTrue(geospatialTypes.getTypes().contains(3001));
    Assert.assertEquals(1, geospatialTypes.getTypes().size());
  }

  @Test
  public void testMergeGeospatialTypes() {
    GeometryFactory gf = new GeometryFactory();

    // Create first set of types
    GeospatialTypes types1 = new GeospatialTypes();
    types1.update(gf.createPoint(new Coordinate(1, 1))); // Point (1)

    // Create second set of types
    GeospatialTypes types2 = new GeospatialTypes();
    Coordinate[] lineCoords = new Coordinate[] {new Coordinate(1, 1), new Coordinate(2, 2)};
    types2.update(gf.createLineString(lineCoords)); // LineString (2)

    // Merge types2 into types1
    types1.merge(types2);

    // Check merged result
    Assert.assertTrue(types1.getTypes().contains(1)); // Point
    Assert.assertTrue(types1.getTypes().contains(2)); // LineString
    Assert.assertEquals(2, types1.getTypes().size());

    // Create third set of types with Z dimension
    GeospatialTypes types3 = new GeospatialTypes();
    types3.update(gf.createPoint(new Coordinate(1, 1, 1))); // Point XYZ (1001)

    // Merge types3 into types1
    types1.merge(types3);

    // Check merged result
    Assert.assertTrue(types1.getTypes().contains(1)); // Point XY
    Assert.assertTrue(types1.getTypes().contains(2)); // LineString XY
    Assert.assertTrue(types1.getTypes().contains(1001)); // Point XYZ
    Assert.assertEquals(3, types1.getTypes().size());
  }

  @Test
  public void testMergeWithEmptyGeospatialTypes() {
    GeometryFactory gf = new GeometryFactory();

    // Create set with types
    GeospatialTypes types1 = new GeospatialTypes();
    types1.update(gf.createPoint(new Coordinate(1, 1))); // Type 1
    Assert.assertEquals(1, types1.getTypes().size());

    // Create empty set
    GeospatialTypes emptyTypes = new GeospatialTypes();
    Assert.assertEquals(0, emptyTypes.getTypes().size());

    // Merge empty into non-empty
    types1.merge(emptyTypes);
    Assert.assertEquals(1, types1.getTypes().size());
    Assert.assertTrue(types1.getTypes().contains(1));

    // Merge non-empty into empty
    emptyTypes.merge(types1);
    Assert.assertEquals(1, emptyTypes.getTypes().size());
    Assert.assertTrue(emptyTypes.getTypes().contains(1));
  }

  @Test
  public void testUpdateWithNullOrEmptyGeometry() {
    GeospatialTypes geospatialTypes = new GeospatialTypes();

    // Update with null geometry
    geospatialTypes.update(null);
    Assert.assertEquals(0, geospatialTypes.getTypes().size());

    // Update with empty point
    GeometryFactory gf = new GeometryFactory();
    Point emptyPoint = gf.createPoint((Coordinate) null);
    geospatialTypes.update(emptyPoint);
    Assert.assertEquals(0, geospatialTypes.getTypes().size());

    // Update with empty linestring
    LineString emptyLine = gf.createLineString((Coordinate[]) null);
    geospatialTypes.update(emptyLine);
    Assert.assertEquals(0, geospatialTypes.getTypes().size());
  }

  @Test
  public void testReset() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes geospatialTypes = new GeospatialTypes();

    // Add some types
    geospatialTypes.update(gf.createPoint(new Coordinate(1, 1)));
    geospatialTypes.update(gf.createLineString(new Coordinate[] {new Coordinate(1, 1), new Coordinate(2, 2)}));
    Assert.assertEquals(2, geospatialTypes.getTypes().size());

    // Reset the types
    geospatialTypes.reset();
    Assert.assertEquals(0, geospatialTypes.getTypes().size());

    // Add new types after reset
    geospatialTypes.update(gf.createPoint(new Coordinate(3, 3, 3))); // XYZ point (1001)
    Assert.assertEquals(1, geospatialTypes.getTypes().size());
    Assert.assertTrue(geospatialTypes.getTypes().contains(1001));
  }

  @Test
  public void testAbort() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes geospatialTypes = new GeospatialTypes();

    // Add some types
    geospatialTypes.update(gf.createPoint(new Coordinate(1, 1)));
    Assert.assertTrue(geospatialTypes.isValid());
    Assert.assertEquals(1, geospatialTypes.getTypes().size());

    // Abort the set
    geospatialTypes.abort();
    Assert.assertFalse(geospatialTypes.isValid());
    Assert.assertEquals(0, geospatialTypes.getTypes().size());

    // Update after abort shouldn't add anything
    geospatialTypes.update(gf.createPoint(new Coordinate(2, 2)));
    Assert.assertEquals(0, geospatialTypes.getTypes().size());
    Assert.assertFalse(geospatialTypes.isValid());
  }

  @Test
  public void testCopy() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes original = new GeospatialTypes();

    // Add some types
    original.update(gf.createPoint(new Coordinate(1, 1)));
    original.update(gf.createLineString(new Coordinate[] {new Coordinate(1, 1), new Coordinate(2, 2)}));

    // Create a copy
    GeospatialTypes copy = original.copy();

    // Verify the copy has the same types
    Assert.assertEquals(original.getTypes().size(), copy.getTypes().size());
    for (Integer typeId : original.getTypes()) {
      Assert.assertTrue(copy.getTypes().contains(typeId));
    }

    // Modify copy and verify it doesn't affect the original
    copy.update(gf.createPoint(new Coordinate(3, 3, 3))); // Add XYZ point (1001)
    Assert.assertEquals(2, original.getTypes().size());
    Assert.assertEquals(3, copy.getTypes().size());
    Assert.assertTrue(copy.getTypes().contains(1001));
    Assert.assertFalse(original.getTypes().contains(1001));
  }

  @Test
  public void testMergeWithNullGeospatialTypes() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes types = new GeospatialTypes();

    // Add a type
    types.update(gf.createPoint(new Coordinate(1, 1)));
    Assert.assertEquals(1, types.getTypes().size());
    Assert.assertTrue(types.isValid());

    // Merge with null
    types.merge(null);
    Assert.assertEquals(0, types.getTypes().size());
    Assert.assertFalse(types.isValid());
  }

  @Test
  public void testMergeWithInvalidGeospatialTypes() {
    GeometryFactory gf = new GeometryFactory();

    // Create valid types
    GeospatialTypes validTypes = new GeospatialTypes();
    validTypes.update(gf.createPoint(new Coordinate(1, 1)));
    Assert.assertTrue(validTypes.isValid());
    Assert.assertEquals(1, validTypes.getTypes().size());

    // Create invalid types
    GeospatialTypes invalidTypes = new GeospatialTypes();
    invalidTypes.abort(); // Mark as invalid
    Assert.assertFalse(invalidTypes.isValid());

    // Merge invalid into valid
    validTypes.merge(invalidTypes);
    Assert.assertFalse(validTypes.isValid());
    Assert.assertEquals(0, validTypes.getTypes().size());

    // Create new valid types
    GeospatialTypes newValidTypes = new GeospatialTypes();
    newValidTypes.update(gf.createPoint(new Coordinate(2, 2)));

    // Merge valid into invalid
    invalidTypes.merge(newValidTypes);
    Assert.assertFalse(invalidTypes.isValid());
    Assert.assertEquals(0, invalidTypes.getTypes().size());
  }

  @Test
  public void testConstructorWithTypes() {
    // Create a set of types
    Set<Integer> typeSet = new HashSet<>();
    typeSet.add(1); // Point XY
    typeSet.add(1001); // Point XYZ
    typeSet.add(2); // LineString XY

    // Create GeospatialTypes with the set
    GeospatialTypes types = new GeospatialTypes(typeSet);

    // Verify types were properly set
    Assert.assertEquals(3, types.getTypes().size());
    Assert.assertTrue(types.getTypes().contains(1));
    Assert.assertTrue(types.getTypes().contains(1001));
    Assert.assertTrue(types.getTypes().contains(2));
    Assert.assertTrue(types.isValid());
  }

  @Test
  public void testUpdateWithMixedDimensionGeometries() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes types = new GeospatialTypes();

    // Add Point XY
    types.update(gf.createPoint(new Coordinate(1, 1)));
    Assert.assertTrue(types.getTypes().contains(1));

    // Add Point XYZ
    types.update(gf.createPoint(new Coordinate(2, 2, 2)));
    Assert.assertTrue(types.getTypes().contains(1));
    Assert.assertTrue(types.getTypes().contains(1001));
    Assert.assertEquals(2, types.getTypes().size());

    // Add Point XYM
    CoordinateXYZM coordXYM = new CoordinateXYZM(3, 3, Double.NaN, 10);
    types.update(gf.createPoint(coordXYM));
    Assert.assertTrue(types.getTypes().contains(1));
    Assert.assertTrue(types.getTypes().contains(1001));
    Assert.assertTrue(types.getTypes().contains(2001));
    Assert.assertEquals(3, types.getTypes().size());

    // Add Point XYZM
    CoordinateXYZM coordXYZM = new CoordinateXYZM(4, 4, 4, 10);
    types.update(gf.createPoint(coordXYZM));
    Assert.assertTrue(types.getTypes().contains(1));
    Assert.assertTrue(types.getTypes().contains(1001));
    Assert.assertTrue(types.getTypes().contains(2001));
    Assert.assertTrue(types.getTypes().contains(3001));
    Assert.assertEquals(4, types.getTypes().size());
  }

  @Test
  public void testRowGroupTypeMerging() {
    GeometryFactory gf = new GeometryFactory();

    // File level geospatial types (to be computed by merging row groups)
    GeospatialTypes fileTypes = new GeospatialTypes();

    // Row Group 1: Points XY and XYZ
    GeospatialTypes rowGroup1 = new GeospatialTypes();
    rowGroup1.update(gf.createPoint(new Coordinate(1, 1))); // Point XY (1)
    rowGroup1.update(gf.createPoint(new Coordinate(2, 2, 2))); // Point XYZ (1001)
    Assert.assertEquals(2, rowGroup1.getTypes().size());
    Assert.assertTrue(rowGroup1.getTypes().contains(1));
    Assert.assertTrue(rowGroup1.getTypes().contains(1001));

    // Row Group 2: LineStrings XY
    GeospatialTypes rowGroup2 = new GeospatialTypes();
    LineString lineXY = gf.createLineString(new Coordinate[] {new Coordinate(1, 1), new Coordinate(2, 2)});
    rowGroup2.update(lineXY); // LineString XY (2)
    Assert.assertEquals(1, rowGroup2.getTypes().size());
    Assert.assertTrue(rowGroup2.getTypes().contains(2));

    // Row Group 3: Invalid types (aborted)
    GeospatialTypes rowGroup3 = new GeospatialTypes();
    rowGroup3.abort();
    Assert.assertFalse(rowGroup3.isValid());

    // Merge row groups into file-level types
    fileTypes.merge(rowGroup1);
    fileTypes.merge(rowGroup2);
    fileTypes.merge(rowGroup3); // This should invalidate fileTypes

    // Verify file level types after merge
    Assert.assertFalse(fileTypes.isValid());
    Assert.assertEquals(0, fileTypes.getTypes().size());

    // Test with different merge order - abort last
    fileTypes = new GeospatialTypes();
    fileTypes.merge(rowGroup1);
    fileTypes.merge(rowGroup3); // This should invalidate fileTypes immediately
    fileTypes.merge(rowGroup2); // This shouldn't change anything since fileTypes is already invalid

    // Verify file level types after second merge sequence
    Assert.assertFalse(fileTypes.isValid());
    Assert.assertEquals(0, fileTypes.getTypes().size());

    // Test without the invalid row group
    fileTypes = new GeospatialTypes();
    fileTypes.merge(rowGroup1);
    fileTypes.merge(rowGroup2);

    // Verify file level types - should have 3 types: Point XY, Point XYZ, LineString XY
    Assert.assertTrue(fileTypes.isValid());
    Assert.assertEquals(3, fileTypes.getTypes().size());
    Assert.assertTrue(fileTypes.getTypes().contains(1));
    Assert.assertTrue(fileTypes.getTypes().contains(1001));
    Assert.assertTrue(fileTypes.getTypes().contains(2));
  }

  @Test
  public void testGeometryTypeCodeAssignment() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes geospatialTypes = new GeospatialTypes();

    // Test Point (type code 1)
    Point point = gf.createPoint(new Coordinate(1, 1));
    geospatialTypes.update(point);
    Assert.assertEquals(1, geospatialTypes.getTypes().size());
    Assert.assertTrue(geospatialTypes.getTypes().contains(1));

    geospatialTypes.reset();

    // Test LineString (type code 2)
    LineString line = gf.createLineString(new Coordinate[] {new Coordinate(1, 1), new Coordinate(2, 2)});
    geospatialTypes.update(line);
    Assert.assertEquals(1, geospatialTypes.getTypes().size());
    Assert.assertTrue(geospatialTypes.getTypes().contains(2));

    geospatialTypes.reset();

    // Test Polygon (type code 3)
    LinearRing shell = gf.createLinearRing(new Coordinate[] {
      new Coordinate(0, 0), new Coordinate(1, 0),
      new Coordinate(1, 1), new Coordinate(0, 1),
      new Coordinate(0, 0)
    });
    Polygon polygon = gf.createPolygon(shell);
    geospatialTypes.update(polygon);
    Assert.assertEquals(1, geospatialTypes.getTypes().size());
    Assert.assertTrue(geospatialTypes.getTypes().contains(3));

    geospatialTypes.reset();

    // Test MultiPoint (type code 4)
    MultiPoint multiPoint = gf.createMultiPoint(
        new Point[] {gf.createPoint(new Coordinate(1, 1)), gf.createPoint(new Coordinate(2, 2))});
    geospatialTypes.update(multiPoint);
    Assert.assertEquals(1, geospatialTypes.getTypes().size());
    Assert.assertTrue(geospatialTypes.getTypes().contains(4));

    geospatialTypes.reset();

    // Test MultiLineString (type code 5)
    MultiLineString multiLine = gf.createMultiLineString(new LineString[] {
      gf.createLineString(new Coordinate[] {new Coordinate(1, 1), new Coordinate(2, 2)}),
      gf.createLineString(new Coordinate[] {new Coordinate(3, 3), new Coordinate(4, 4)})
    });
    geospatialTypes.update(multiLine);
    Assert.assertEquals(1, geospatialTypes.getTypes().size());
    Assert.assertTrue(geospatialTypes.getTypes().contains(5));

    geospatialTypes.reset();

    // Test MultiPolygon (type code 6)
    MultiPolygon multiPolygon = gf.createMultiPolygon(new Polygon[] {gf.createPolygon(shell)});
    geospatialTypes.update(multiPolygon);
    Assert.assertEquals(1, geospatialTypes.getTypes().size());
    Assert.assertTrue(geospatialTypes.getTypes().contains(6));

    geospatialTypes.reset();

    // Test GeometryCollection (type code 7)
    GeometryCollection collection =
        gf.createGeometryCollection(new org.locationtech.jts.geom.Geometry[] {point, line});
    geospatialTypes.update(collection);
    Assert.assertEquals(1, geospatialTypes.getTypes().size());
    Assert.assertTrue(geospatialTypes.getTypes().contains(7));
  }

  @Test
  public void testGeometryTypeDimensionCodes() {
    GeometryFactory gf = new GeometryFactory();

    // Test XY (standard 2D, no prefix = 0)
    GeospatialTypes types2D = new GeospatialTypes();
    types2D.update(gf.createPoint(new Coordinate(1, 1)));
    Assert.assertTrue(types2D.getTypes().contains(1)); // Point XY

    // Test XYZ (Z dimension, prefix = 1000)
    GeospatialTypes types3D = new GeospatialTypes();
    types3D.update(gf.createPoint(new Coordinate(1, 1, 1)));
    Assert.assertTrue(types3D.getTypes().contains(1001)); // Point XYZ

    // Test XYM (M dimension, prefix = 2000)
    GeospatialTypes typesXYM = new GeospatialTypes();
    CoordinateXYZM coordXYM = new CoordinateXYZM(1, 1, Double.NaN, 10);
    typesXYM.update(gf.createPoint(coordXYM));
    Assert.assertTrue(typesXYM.getTypes().contains(2001)); // Point XYM

    // Test XYZM (Z and M dimensions, prefix = 3000)
    GeospatialTypes typesXYZM = new GeospatialTypes();
    CoordinateXYZM coordXYZM = new CoordinateXYZM(1, 1, 1, 10);
    typesXYZM.update(gf.createPoint(coordXYZM));
    Assert.assertTrue(typesXYZM.getTypes().contains(3001)); // Point XYZM
  }
}
