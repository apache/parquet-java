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

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Geometry;

class GeometryUtils {

  public static void normalizeLongitude(Geometry geometry) {
    if (geometry == null || geometry.isEmpty()) {
      return;
    }

    geometry.apply(new CoordinateSequenceFilter() {
      @Override
      public void filter(CoordinateSequence seq, int i) {
        double x = seq.getX(i);
        // Normalize the longitude to be within -180 to 180 range
        while (x > 180) x -= 360;
        while (x < -180) x += 360;
        seq.setOrdinate(i, CoordinateSequence.X, x);
      }

      @Override
      public boolean isDone() {
        return false; // Continue processing until all coordinates are processed
      }

      @Override
      public boolean isGeometryChanged() {
        return true; // The geometry is changed as we are modifying the coordinates
      }
    });

    geometry.geometryChanged(); // Notify the geometry that its coordinates have been changed
  }
}
