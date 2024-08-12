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
import java.util.Arrays;
import java.util.List;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

public class EnvelopeCovering extends Covering {

  // The POC only supports EPSG:3857 and EPSG:4326 at the moment
  private static final List<String> SUPPORTED_CRS = Arrays.asList("EPSG:3857", "EPSG:4326");

  private static final ByteBuffer EMPTY = ByteBuffer.wrap(new byte[0]);
  private final WKBReader reader = new WKBReader();
  private final WKBWriter writer = new WKBWriter();
  private final GeometryFactory factory = new GeometryFactory();
  private final LogicalTypeAnnotation.Edges edges;
  private final String crs;

  public EnvelopeCovering(LogicalTypeAnnotation.Edges edges, String crs) {
    super(EMPTY, DEFAULT_COVERING_KIND);
    this.edges = edges;
    this.crs = crs;
    validateSupportedCrs(crs);
  }

  private void validateSupportedCrs(String crs) {
    if (!SUPPORTED_CRS.contains(crs)) {
      throw new IllegalArgumentException(
          "Unsupported CRS: " + crs + ". Supported CRS are EPSG:3857 and EPSG:4326.");
    }
  }

  @Override
  void update(Geometry geom) {
    if (geom == null) {
      return;
    }
    try {
      if (value != EMPTY) {
        Geometry existingGeometry = reader.read(value.array());
        Envelope existingEnvelope = createEnvelopeFromPolygon(existingGeometry);
        Envelope newEnvelope = geom.getEnvelopeInternal();

        Envelope combinedEnvelope = edges == LogicalTypeAnnotation.Edges.SPHERICAL
            ? extendEnvelopeSpherical(existingEnvelope, newEnvelope)
            : extendEnvelopePlanar(existingEnvelope, newEnvelope);

        Geometry envelopePolygon = createPolygonFromEnvelope(combinedEnvelope);

        value = ByteBuffer.wrap(writer.write(envelopePolygon));
      } else {
        Geometry envelopePolygon = createPolygonFromEnvelope(geom.getEnvelopeInternal());
        value = ByteBuffer.wrap(writer.write(envelopePolygon));
      }
    } catch (ParseException e) {
      value = null;
    }
  }

  private Envelope createEnvelopeFromPolygon(Geometry polygon) {
    Coordinate[] coordinates = polygon.getCoordinates();
    double minX = coordinates[0].x;
    double minY = coordinates[0].y;
    double maxX = coordinates[2].x;
    double maxY = coordinates[2].y;
    return new Envelope(minX, maxX, minY, maxY);
  }

  private Envelope extendEnvelopePlanar(Envelope existingEnvelope, Envelope newEnvelope) {
    existingEnvelope.expandToInclude(newEnvelope);
    return existingEnvelope;
  }

  private Envelope extendEnvelopeSpherical(Envelope existingEnvelope, Envelope newEnvelope) {
    // Currently, we don't have an easy way to correctly compute the polygonal covering of spherical edge.
    // In this POC implementation, we will throw a not-implemented exception for the covering statistics,
    // when the spherical edge is specified.
    throw new UnsupportedOperationException("Spherical edges are not supported yet.");
  }

  private Geometry createPolygonFromEnvelope(Envelope envelope) {
    Coordinate[] coordinates;
    if (envelope.getMinX() == envelope.getMaxX() && envelope.getMinY() == envelope.getMaxY()) {
      // Handle the case where the envelope is a point
      coordinates = new Coordinate[] {
        new Coordinate(envelope.getMinX(), envelope.getMinY()),
        new Coordinate(envelope.getMinX(), envelope.getMinY() + 1),
        new Coordinate(envelope.getMinX() + 1, envelope.getMinY() + 1),
        new Coordinate(envelope.getMinX() + 1, envelope.getMinY()),
        new Coordinate(envelope.getMinX(), envelope.getMinY()) // Closing the ring
      };
    } else if (envelope.getMinX() == envelope.getMaxX() || envelope.getMinY() == envelope.getMaxY()) {
      // Handle the case where the envelope is a line
      coordinates = new Coordinate[] {
        new Coordinate(envelope.getMinX(), envelope.getMinY()),
        new Coordinate(envelope.getMinX(), envelope.getMaxY()),
        new Coordinate(envelope.getMaxX(), envelope.getMaxY()),
        new Coordinate(envelope.getMaxX(), envelope.getMinY()),
        new Coordinate(envelope.getMinX(), envelope.getMinY()) // Closing the ring
      };
    } else {
      // Handle the normal case
      coordinates = new Coordinate[] {
        new Coordinate(envelope.getMinX(), envelope.getMinY()),
        new Coordinate(envelope.getMinX(), envelope.getMaxY()),
        new Coordinate(envelope.getMaxX(), envelope.getMaxY()),
        new Coordinate(envelope.getMaxX(), envelope.getMinY()),
        new Coordinate(envelope.getMinX(), envelope.getMinY()) // Closing the ring
      };
    }
    return factory.createPolygon(factory.createLinearRing(coordinates), null);
  }

  @Override
  public void merge(Covering other) {
    if (other instanceof EnvelopeCovering) {
      try {
        update(reader.read(other.value.array()));
      } catch (ParseException e) {
        value = null;
      }
    } else {
      throw new UnsupportedOperationException("Cannot merge " + this.getClass() + " with "
          + other.getClass().getSimpleName());
    }
  }

  @Override
  public void reset() {
    value = EMPTY;
  }

  @Override
  public void abort() {
    value = null;
  }

  @Override
  public EnvelopeCovering copy() {
    EnvelopeCovering copy = new EnvelopeCovering(edges, crs);
    copy.value = value == null ? null : ByteBuffer.wrap(value.array());
    return copy;
  }
}
