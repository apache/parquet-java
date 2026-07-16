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

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
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
    assertThat(geospatialTypes.getTypes()).containsExactly(1);

    // Test with LineString (type code 2)
    Coordinate[] lineCoords = new Coordinate[] {new Coordinate(1, 1), new Coordinate(2, 2)};
    LineString line = gf.createLineString(lineCoords);
    geospatialTypes.update(line);
    assertThat(geospatialTypes.getTypes()).containsExactlyInAnyOrder(1, 2);

    // Test with Polygon (type code 3)
    Coordinate[] polygonCoords = new Coordinate[] {
      new Coordinate(0, 0), new Coordinate(1, 0),
      new Coordinate(1, 1), new Coordinate(0, 1),
      new Coordinate(0, 0)
    };
    LinearRing shell = gf.createLinearRing(polygonCoords);
    Polygon polygon = gf.createPolygon(shell);
    geospatialTypes.update(polygon);
    assertThat(geospatialTypes.getTypes()).containsExactlyInAnyOrder(1, 2, 3);
  }

  @Test
  public void testUpdateWithComplexGeometries() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes geospatialTypes = new GeospatialTypes();

    // MultiPoint (type code 4)
    Point[] points = new Point[] {gf.createPoint(new Coordinate(1, 1)), gf.createPoint(new Coordinate(2, 2))};
    MultiPoint multiPoint = gf.createMultiPoint(points);
    geospatialTypes.update(multiPoint);
    assertThat(geospatialTypes.getTypes()).containsExactly(4);

    // MultiLineString (type code 5)
    LineString[] lines = new LineString[] {
      gf.createLineString(new Coordinate[] {new Coordinate(1, 1), new Coordinate(2, 2)}),
      gf.createLineString(new Coordinate[] {new Coordinate(3, 3), new Coordinate(4, 4)})
    };
    MultiLineString multiLine = gf.createMultiLineString(lines);
    geospatialTypes.update(multiLine);
    assertThat(geospatialTypes.getTypes()).containsExactlyInAnyOrder(4, 5);

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
    assertThat(geospatialTypes.getTypes()).containsExactlyInAnyOrder(4, 5, 6);

    // GeometryCollection (type code 7)
    GeometryCollection collection = gf.createGeometryCollection(
        new org.locationtech.jts.geom.Geometry[] {multiPoint, multiLine, multiPolygon});
    geospatialTypes.update(collection);
    assertThat(geospatialTypes.getTypes()).containsExactlyInAnyOrder(4, 5, 6, 7);
  }

  @Test
  public void testUpdateWithZCoordinates() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes geospatialTypes = new GeospatialTypes();

    // Create a 3D point (XYZ) - should be type code 1001
    Point pointXYZ = gf.createPoint(new Coordinate(1, 1, 1));
    geospatialTypes.update(pointXYZ);
    assertThat(geospatialTypes.getTypes()).containsExactly(1001);
  }

  @Test
  public void testUpdateWithMCoordinates() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes geospatialTypes = new GeospatialTypes();

    // Create a point with measure (XYM) - should be type code 2001
    CoordinateXYZM coord = new CoordinateXYZM(1, 1, Double.NaN, 10);
    Point pointXYM = gf.createPoint(coord);
    geospatialTypes.update(pointXYM);
    assertThat(geospatialTypes.getTypes()).containsExactly(2001);
  }

  @Test
  public void testUpdateWithZMCoordinates() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes geospatialTypes = new GeospatialTypes();

    // Create a 4D point (XYZM) - should be type code 3001
    CoordinateXYZM coord = new CoordinateXYZM(1, 1, 1, 10);
    Point pointXYZM = gf.createPoint(coord);
    geospatialTypes.update(pointXYZM);
    assertThat(geospatialTypes.getTypes()).containsExactly(3001);
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
    assertThat(types1.getTypes()).containsExactlyInAnyOrder(1, 2);

    // Create third set of types with Z dimension
    GeospatialTypes types3 = new GeospatialTypes();
    types3.update(gf.createPoint(new Coordinate(1, 1, 1))); // Point XYZ (1001)

    // Merge types3 into types1
    types1.merge(types3);

    // Check merged result
    assertThat(types1.getTypes()).containsExactlyInAnyOrder(1, 2, 1001);
  }

  @Test
  public void testMergeWithEmptyGeospatialTypes() {
    GeometryFactory gf = new GeometryFactory();

    // Create set with types
    GeospatialTypes types1 = new GeospatialTypes();
    types1.update(gf.createPoint(new Coordinate(1, 1))); // Type 1
    assertThat(types1.getTypes()).containsExactly(1);

    // Create empty set
    GeospatialTypes emptyTypes = new GeospatialTypes();
    assertThat(emptyTypes.getTypes()).isEmpty();

    // Merge empty into non-empty
    types1.merge(emptyTypes);
    assertThat(types1.getTypes()).containsExactly(1);

    // Merge non-empty into empty
    emptyTypes.merge(types1);
    assertThat(emptyTypes.getTypes()).containsExactly(1);
  }

  @Test
  public void testUpdateWithNullOrEmptyGeometry() {
    GeospatialTypes geospatialTypes = new GeospatialTypes();

    // Update with null geometry
    geospatialTypes.update(null);
    assertThat(geospatialTypes.getTypes()).isEmpty();

    // Update with empty point
    GeometryFactory gf = new GeometryFactory();
    Point emptyPoint = gf.createPoint((Coordinate) null);
    geospatialTypes.update(emptyPoint);
    assertThat(geospatialTypes.getTypes()).isEmpty();

    // Update with empty linestring
    LineString emptyLine = gf.createLineString((Coordinate[]) null);
    geospatialTypes.update(emptyLine);
    assertThat(geospatialTypes.getTypes()).isEmpty();
  }

  @Test
  public void testReset() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes geospatialTypes = new GeospatialTypes();

    // Add some types
    geospatialTypes.update(gf.createPoint(new Coordinate(1, 1)));
    geospatialTypes.update(gf.createLineString(new Coordinate[] {new Coordinate(1, 1), new Coordinate(2, 2)}));
    assertThat(geospatialTypes.getTypes()).containsExactlyInAnyOrder(1, 2);

    // Reset the types
    geospatialTypes.reset();
    assertThat(geospatialTypes.getTypes()).isEmpty();

    // Add new types after reset
    geospatialTypes.update(gf.createPoint(new Coordinate(3, 3, 3))); // XYZ point (1001)
    assertThat(geospatialTypes.getTypes()).containsExactly(1001);
  }

  @Test
  public void testAbort() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes geospatialTypes = new GeospatialTypes();

    // Add some types
    geospatialTypes.update(gf.createPoint(new Coordinate(1, 1)));
    assertThat(geospatialTypes.isValid()).isTrue();
    assertThat(geospatialTypes.getTypes()).containsExactly(1);

    // Abort the set
    geospatialTypes.abort();
    assertThat(geospatialTypes.isValid()).isFalse();
    assertThat(geospatialTypes.getTypes()).isEmpty();

    // Update after abort shouldn't add anything
    geospatialTypes.update(gf.createPoint(new Coordinate(2, 2)));
    assertThat(geospatialTypes.getTypes()).isEmpty();
    assertThat(geospatialTypes.isValid()).isFalse();
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
    assertThat(copy.getTypes()).hasSameSizeAs(original.getTypes()).containsExactlyInAnyOrder(1, 2);

    // Modify copy and verify it doesn't affect the original
    copy.update(gf.createPoint(new Coordinate(3, 3, 3))); // Add XYZ point (1001)
    assertThat(original.getTypes()).containsExactlyInAnyOrder(1, 2);
    assertThat(copy.getTypes()).containsExactlyInAnyOrder(1, 2, 1001);
    assertThat(original.getTypes()).doesNotContain(1001);
  }

  @Test
  public void testMergeWithNullGeospatialTypes() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes types = new GeospatialTypes();

    // Add a type
    types.update(gf.createPoint(new Coordinate(1, 1)));
    assertThat(types.getTypes()).containsExactly(1);
    assertThat(types.isValid()).isTrue();

    // Merge with null
    types.merge(null);
    assertThat(types.getTypes()).isEmpty();
    assertThat(types.isValid()).isFalse();
  }

  @Test
  public void testMergeWithInvalidGeospatialTypes() {
    GeometryFactory gf = new GeometryFactory();

    // Create valid types
    GeospatialTypes validTypes = new GeospatialTypes();
    validTypes.update(gf.createPoint(new Coordinate(1, 1)));
    assertThat(validTypes.isValid()).isTrue();
    assertThat(validTypes.getTypes()).containsExactly(1);

    // Create invalid types
    GeospatialTypes invalidTypes = new GeospatialTypes();
    invalidTypes.abort(); // Mark as invalid
    assertThat(invalidTypes.isValid()).isFalse();

    // Merge invalid into valid
    validTypes.merge(invalidTypes);
    assertThat(validTypes.isValid()).isFalse();
    assertThat(validTypes.getTypes()).isEmpty();

    // Create new valid types
    GeospatialTypes newValidTypes = new GeospatialTypes();
    newValidTypes.update(gf.createPoint(new Coordinate(2, 2)));

    // Merge valid into invalid
    invalidTypes.merge(newValidTypes);
    assertThat(invalidTypes.isValid()).isFalse();
    assertThat(invalidTypes.getTypes()).isEmpty();
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
    assertThat(types.getTypes()).containsExactlyInAnyOrder(1, 1001, 2);
    assertThat(types.isValid()).isTrue();
  }

  @Test
  public void testUpdateWithMixedDimensionGeometries() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes types = new GeospatialTypes();

    // Add Point XY
    types.update(gf.createPoint(new Coordinate(1, 1)));
    assertThat(types.getTypes()).containsExactly(1);

    // Add Point XYZ
    types.update(gf.createPoint(new Coordinate(2, 2, 2)));
    assertThat(types.getTypes()).containsExactlyInAnyOrder(1, 1001);

    // Add Point XYM
    CoordinateXYZM coordXYM = new CoordinateXYZM(3, 3, Double.NaN, 10);
    types.update(gf.createPoint(coordXYM));
    assertThat(types.getTypes()).containsExactlyInAnyOrder(1, 1001, 2001);

    // Add Point XYZM
    CoordinateXYZM coordXYZM = new CoordinateXYZM(4, 4, 4, 10);
    types.update(gf.createPoint(coordXYZM));
    assertThat(types.getTypes()).containsExactlyInAnyOrder(1, 1001, 2001, 3001);
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
    assertThat(rowGroup1.getTypes()).containsExactlyInAnyOrder(1, 1001);

    // Row Group 2: LineStrings XY
    GeospatialTypes rowGroup2 = new GeospatialTypes();
    LineString lineXY = gf.createLineString(new Coordinate[] {new Coordinate(1, 1), new Coordinate(2, 2)});
    rowGroup2.update(lineXY); // LineString XY (2)
    assertThat(rowGroup2.getTypes()).containsExactly(2);

    // Row Group 3: Invalid types (aborted)
    GeospatialTypes rowGroup3 = new GeospatialTypes();
    rowGroup3.abort();
    assertThat(rowGroup3.isValid()).isFalse();

    // Merge row groups into file-level types
    fileTypes.merge(rowGroup1);
    fileTypes.merge(rowGroup2);
    fileTypes.merge(rowGroup3); // This should invalidate fileTypes

    // Verify file level types after merge
    assertThat(fileTypes.isValid()).isFalse();
    assertThat(fileTypes.getTypes()).isEmpty();

    // Test with different merge order - abort last
    fileTypes = new GeospatialTypes();
    fileTypes.merge(rowGroup1);
    fileTypes.merge(rowGroup3); // This should invalidate fileTypes immediately
    fileTypes.merge(rowGroup2); // This shouldn't change anything since fileTypes is already invalid

    // Verify file level types after second merge sequence
    assertThat(fileTypes.isValid()).isFalse();
    assertThat(fileTypes.getTypes()).isEmpty();

    // Test without the invalid row group
    fileTypes = new GeospatialTypes();
    fileTypes.merge(rowGroup1);
    fileTypes.merge(rowGroup2);

    // Verify file level types - should have 3 types: Point XY, Point XYZ, LineString XY
    assertThat(fileTypes.isValid()).isTrue();
    assertThat(fileTypes.getTypes()).containsExactlyInAnyOrder(1, 1001, 2);
  }

  @Test
  public void testGeometryTypeCodeAssignment() {
    GeometryFactory gf = new GeometryFactory();
    GeospatialTypes geospatialTypes = new GeospatialTypes();

    // Test Point (type code 1)
    Point point = gf.createPoint(new Coordinate(1, 1));
    geospatialTypes.update(point);
    assertThat(geospatialTypes.getTypes()).containsExactly(1);

    geospatialTypes.reset();

    // Test LineString (type code 2)
    LineString line = gf.createLineString(new Coordinate[] {new Coordinate(1, 1), new Coordinate(2, 2)});
    geospatialTypes.update(line);
    assertThat(geospatialTypes.getTypes()).containsExactly(2);

    geospatialTypes.reset();

    // Test Polygon (type code 3)
    LinearRing shell = gf.createLinearRing(new Coordinate[] {
      new Coordinate(0, 0), new Coordinate(1, 0),
      new Coordinate(1, 1), new Coordinate(0, 1),
      new Coordinate(0, 0)
    });
    Polygon polygon = gf.createPolygon(shell);
    geospatialTypes.update(polygon);
    assertThat(geospatialTypes.getTypes()).containsExactly(3);

    geospatialTypes.reset();

    // Test MultiPoint (type code 4)
    MultiPoint multiPoint = gf.createMultiPoint(
        new Point[] {gf.createPoint(new Coordinate(1, 1)), gf.createPoint(new Coordinate(2, 2))});
    geospatialTypes.update(multiPoint);
    assertThat(geospatialTypes.getTypes()).containsExactly(4);

    geospatialTypes.reset();

    // Test MultiLineString (type code 5)
    MultiLineString multiLine = gf.createMultiLineString(new LineString[] {
      gf.createLineString(new Coordinate[] {new Coordinate(1, 1), new Coordinate(2, 2)}),
      gf.createLineString(new Coordinate[] {new Coordinate(3, 3), new Coordinate(4, 4)})
    });
    geospatialTypes.update(multiLine);
    assertThat(geospatialTypes.getTypes()).containsExactly(5);

    geospatialTypes.reset();

    // Test MultiPolygon (type code 6)
    MultiPolygon multiPolygon = gf.createMultiPolygon(new Polygon[] {gf.createPolygon(shell)});
    geospatialTypes.update(multiPolygon);
    assertThat(geospatialTypes.getTypes()).containsExactly(6);

    geospatialTypes.reset();

    // Test GeometryCollection (type code 7)
    GeometryCollection collection =
        gf.createGeometryCollection(new org.locationtech.jts.geom.Geometry[] {point, line});
    geospatialTypes.update(collection);
    assertThat(geospatialTypes.getTypes()).containsExactly(7);
  }

  @Test
  public void testGeometryTypeDimensionCodes() {
    GeometryFactory gf = new GeometryFactory();

    // Test XY (standard 2D, no prefix = 0)
    GeospatialTypes types2D = new GeospatialTypes();
    types2D.update(gf.createPoint(new Coordinate(1, 1)));
    assertThat(types2D.getTypes()).containsExactly(1); // Point XY

    // Test XYZ (Z dimension, prefix = 1000)
    GeospatialTypes types3D = new GeospatialTypes();
    types3D.update(gf.createPoint(new Coordinate(1, 1, 1)));
    assertThat(types3D.getTypes()).containsExactly(1001); // Point XYZ

    // Test XYM (M dimension, prefix = 2000)
    GeospatialTypes typesXYM = new GeospatialTypes();
    CoordinateXYZM coordXYM = new CoordinateXYZM(1, 1, Double.NaN, 10);
    typesXYM.update(gf.createPoint(coordXYM));
    assertThat(typesXYM.getTypes()).containsExactly(2001); // Point XYM

    // Test XYZM (Z and M dimensions, prefix = 3000)
    GeospatialTypes typesXYZM = new GeospatialTypes();
    CoordinateXYZM coordXYZM = new CoordinateXYZM(1, 1, 1, 10);
    typesXYZM.update(gf.createPoint(coordXYZM));
    assertThat(typesXYZM.getTypes()).containsExactly(3001); // Point XYZM
  }
}
