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
import java.util.stream.Collectors;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

public class GeospatialTypes {

  private static final int UNKNOWN_TYPE_ID = -1;
  private Set<Integer> types = new HashSet<>();
  private boolean valid = true;

  public GeospatialTypes(Set<Integer> types) {
    this.types = types;
    this.valid = validateTypeValues();
  }

  public GeospatialTypes(Set<Integer> types, boolean valid) {
    this.types = types;
    this.valid = valid;
  }

  public GeospatialTypes() {}

  public Set<Integer> getTypes() {
    return types;
  }

  public void update(Geometry geometry) {
    if (!valid) {
      return;
    }

    if (geometry == null || geometry.isEmpty()) {
      return;
    }

    int code = getGeometryTypeCode(geometry);
    if (code != UNKNOWN_TYPE_ID) {
      types.add(code);
    } else {
      valid = false;
      types.clear();
    }
  }

  public void merge(GeospatialTypes other) {
    if (!valid) {
      return;
    }

    if (other == null || !other.valid) {
      valid = false;
      types.clear();
      return;
    }
    types.addAll(other.types);
  }

  public void reset() {
    types.clear();
    valid = true;
  }

  public void abort() {
    valid = false;
    types.clear();
  }

  public boolean isValid() {
    return valid;
  }

  public GeospatialTypes copy() {
    return new GeospatialTypes(new HashSet<>(types), valid);
  }

  /**
   * Validates the geometry type codes in this GeospatialTypes instance.
   * This method ensures that all type codes represent valid geometry types and dimensions.
   *
   * @return true if all type codes are valid, false otherwise.
   */
  public boolean validateTypeValues() {
    if (!isValid()) {
      return false;
    }

    // No types is considered valid
    if (types.isEmpty()) {
      return true;
    }

    for (Integer typeId : types) {
      // Null type code is invalid
      if (typeId == null) {
        return false;
      }

      // Negative type codes (except UNKNOWN_TYPE_ID) are invalid
      if (typeId < 0 && typeId != UNKNOWN_TYPE_ID) {
        return false;
      }

      // Check for valid geometry type base codes (1-7)
      // After removing dimension prefix: 1=Point, 2=LineString, 3=Polygon, etc.
      int baseTypeCode = getBaseTypeCode(typeId);
      if (baseTypeCode < 1 || baseTypeCode > 7) {
        return false;
      }

      // Check for valid dimension prefix (0=XY, 1000=XYZ, 2000=XYM, 3000=XYZM)
      int dimension = getDimensionPrefix(typeId);
      if (dimension != 0 && dimension != 1000 && dimension != 2000 && dimension != 3000) {
        return false;
      }
    }

    return true;
  }

  /**
   * Extracts the base geometry type code from a full type code.
   * For example: 1001 (XYZ Point) -> 1 (Point)
   *
   * @param typeId the full geometry type code
   * @return the base type code (1-7)
   */
  private int getBaseTypeCode(int typeId) {
    return typeId % 1000;
  }

  /**
   * Extracts the dimension prefix from a full type code.
   * For example: 1001 (XYZ Point) -> 1000 (XYZ)
   *
   * @param typeId the full geometry type code
   * @return the dimension prefix (0, 1000, 2000, or 3000)
   */
  private int getDimensionPrefix(int typeId) {
    return (typeId / 1000) * 1000;
  }

  @Override
  public String toString() {
    return "GeospatialTypes{" + "types="
        + types.stream().map(this::typeIdToString).collect(Collectors.toSet()) + '}';
  }

  private int getGeometryTypeId(Geometry geometry) {
    switch (geometry.getGeometryType()) {
      case Geometry.TYPENAME_POINT:
        return 1;
      case Geometry.TYPENAME_LINESTRING:
        return 2;
      case Geometry.TYPENAME_POLYGON:
        return 3;
      case Geometry.TYPENAME_MULTIPOINT:
        return 4;
      case Geometry.TYPENAME_MULTILINESTRING:
        return 5;
      case Geometry.TYPENAME_MULTIPOLYGON:
        return 6;
      case Geometry.TYPENAME_GEOMETRYCOLLECTION:
        return 7;
      default:
        return UNKNOWN_TYPE_ID;
    }
  }

  /**
   * Geospatial type codes:
   *
   * | Type               | XY   | XYZ  | XYM  | XYZM |
   * | :----------------- | :--- | :--- | :--- | :--: |
   * | Point              | 0001 | 1001 | 2001 | 3001 |
   * | LineString         | 0002 | 1002 | 2002 | 3002 |
   * | Polygon            | 0003 | 1003 | 2003 | 3003 |
   * | MultiPoint         | 0004 | 1004 | 2004 | 3004 |
   * | MultiLineString    | 0005 | 1005 | 2005 | 3005 |
   * | MultiPolygon       | 0006 | 1006 | 2006 | 3006 |
   * | GeometryCollection | 0007 | 1007 | 2007 | 3007 |
   *
   * See https://github.com/apache/parquet-format/blob/master/Geospatial.md#geospatial-types
   */
  private int getGeometryTypeCode(Geometry geometry) {
    int typeId = getGeometryTypeId(geometry);
    if (typeId == UNKNOWN_TYPE_ID) {
      return UNKNOWN_TYPE_ID;
    }
    Coordinate[] coordinates = geometry.getCoordinates();
    boolean hasZ = false;
    boolean hasM = false;
    if (coordinates.length > 0) {
      Coordinate firstCoord = coordinates[0];
      hasZ = !Double.isNaN(firstCoord.getZ());
      hasM = !Double.isNaN(firstCoord.getM());
    }
    if (hasZ) {
      typeId += 1000;
    }
    if (hasM) {
      typeId += 2000;
    }
    return typeId;
  }

  private String typeIdToString(int typeId) {
    String typeString;
    switch (typeId % 1000) {
      case 1:
        typeString = Geometry.TYPENAME_POINT;
        break;
      case 2:
        typeString = Geometry.TYPENAME_LINESTRING;
        break;
      case 3:
        typeString = Geometry.TYPENAME_POLYGON;
        break;
      case 4:
        typeString = Geometry.TYPENAME_MULTIPOINT;
        break;
      case 5:
        typeString = Geometry.TYPENAME_MULTILINESTRING;
        break;
      case 6:
        typeString = Geometry.TYPENAME_MULTIPOLYGON;
        break;
      case 7:
        typeString = Geometry.TYPENAME_GEOMETRYCOLLECTION;
        break;
      default:
        return "Unknown";
    }
    if (typeId >= 3000) {
      typeString += " (XYZM)";
    } else if (typeId >= 2000) {
      typeString += " (XYM)";
    } else if (typeId >= 1000) {
      typeString += " (XYZ)";
    } else {
      typeString += " (XY)";
    }
    return typeString;
  }
}
