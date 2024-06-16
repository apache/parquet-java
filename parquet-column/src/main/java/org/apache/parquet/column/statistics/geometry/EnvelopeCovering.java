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
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

public class EnvelopeCovering extends Covering {

  private static final ByteBuffer EMPTY = ByteBuffer.wrap(new byte[0]);
  private final WKBReader reader = new WKBReader();
  private final WKBWriter writer = new WKBWriter();

  public EnvelopeCovering() {
    super(EMPTY, LogicalTypeAnnotation.Edges.PLANAR);
  }

  @Override
  public void update(Geometry geom) {
    if (geometry == null) {
      return;
    }
    try {
      if (geometry != EMPTY) {
        Geometry envelope = reader.read(geometry.array());
        geometry = ByteBuffer.wrap(writer.write(envelope.union(geom).getEnvelope()));
      } else {
        geometry = ByteBuffer.wrap(writer.write(geom.getEnvelope()));
      }
    } catch (ParseException e) {
      geometry = null;
    }
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
