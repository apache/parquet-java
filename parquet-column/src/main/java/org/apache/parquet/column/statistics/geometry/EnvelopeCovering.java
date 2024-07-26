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

import java.nio.ByteBuffer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

public class EnvelopeCovering extends Covering {

  private static final ByteBuffer EMPTY = ByteBuffer.wrap(new byte[0]);
  private final WKBReader reader = new WKBReader();
  private final WKBWriter writer = new WKBWriter();
  private final GeometryFactory factory = new GeometryFactory();

  public EnvelopeCovering() {
    super(EMPTY, LogicalTypeAnnotation.Edges.SPHERICAL);
  }

  @Override
  void update(Geometry geom) {
    if (geom == null) {
      return;
    }
    try {
      if (geometry != EMPTY) {
        Geometry existingGeometry = reader.read(geometry.array());
        Envelope existingEnvelope = existingGeometry.getEnvelopeInternal();
        Envelope newEnvelope = geom.getEnvelopeInternal();

        // The following expandToInclude method works correctly for planar coordinate systems.
        // It simply extends the existing envelope to include the new envelope by adjusting
        // the min and max values of x and y coordinates.
        existingEnvelope.expandToInclude(newEnvelope);

        // However, this approach is not accurate for spherical coordinate systems.
        // In spherical coordinates, the Earth's curvature needs to be taken into account,
        // and simple min/max bounds may not correctly represent the spatial extent.
        // More sophisticated geodetic calculations are required for spherical envelopes.

        Geometry envelopePolygon = createPolygonFromEnvelope(existingEnvelope);

        geometry = ByteBuffer.wrap(writer.write(envelopePolygon));
      } else {
        Geometry envelopePolygon = createPolygonFromEnvelope(geom.getEnvelopeInternal());
        geometry = ByteBuffer.wrap(writer.write(envelopePolygon));
      }
    } catch (ParseException e) {
      geometry = null;
    }
  }

  // Create a polygon from an envelope
  // Assume we are using the Standard WKB format, that no Z and M dimension is supported
  // https://libgeos.org/specifications/wkb/#standard-wkb
  // Enhancement is to do post POC phase to support Z and M dimension.
  private Geometry createPolygonFromEnvelope(Envelope envelope) {
    return factory.createPolygon(new Coordinate[] {
      new Coordinate(envelope.getMinX(), envelope.getMinY()),
      new Coordinate(envelope.getMinX(), envelope.getMaxY()),
      new Coordinate(envelope.getMaxX(), envelope.getMaxY()),
      new Coordinate(envelope.getMaxX(), envelope.getMinY()),
      new Coordinate(envelope.getMinX(), envelope.getMinY())
    });
  }

  @Override
  public void merge(Covering other) {
    if (other instanceof EnvelopeCovering) {
      try {
        update(reader.read(other.geometry.array()));
      } catch (ParseException e) {
        geometry = null;
      }
    } else {
      throw new UnsupportedOperationException("Cannot merge " + this.getClass() + " with "
          + other.getClass().getSimpleName());
    }
  }

  @Override
  public void reset() {
    geometry = EMPTY;
  }

  @Override
  public void abort() {
    geometry = null;
  }

  @Override
  public EnvelopeCovering copy() {
    EnvelopeCovering copy = new EnvelopeCovering();
    copy.geometry = geometry == null ? null : ByteBuffer.wrap(geometry.array());
    return copy;
  }
}
