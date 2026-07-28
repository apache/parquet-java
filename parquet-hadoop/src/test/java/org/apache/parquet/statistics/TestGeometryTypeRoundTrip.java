/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.parquet.statistics;

import static org.apache.parquet.schema.LogicalTypeAnnotation.geographyType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.geometryType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.statistics.geospatial.GeospatialStatistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBWriter;

public class TestGeometryTypeRoundTrip {
  @TempDir
  private Path tempDir;

  private Path newTempPath() {
    return tempDir.resolve(java.util.UUID.randomUUID() + ".parquet");
  }

  @Test
  public void testBasicReadWriteGeometryValue() throws Exception {
    GeometryFactory geomFactory = new GeometryFactory();

    // A class to convert JTS Geometry objects to and from Well-Known Binary (WKB) format.
    WKBWriter wkbWriter = new WKBWriter();

    // OGC:CRS84 (WGS 84): Uses the order longitude, latitude
    Binary[] points = {
      Binary.fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(1.0, 1.0)))),
      Binary.fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(2.0, 2.0))))
    };

    // A message type that represents a message with a geometry column.
    MessageType schema = Types.buildMessage()
        .required(BINARY)
        .as(geometryType(null))
        .named("geometry")
        .named("msg");

    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);
    GroupFactory factory = new SimpleGroupFactory(schema);
    Path path = newTempPath();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(path))
        .withConf(conf)
        .withDictionaryEncoding(false)
        .build()) {
      for (Binary value : points) {
        writer.write(factory.newGroup().append("geometry", value));
      }
    }

    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(path))) {
      assertThat(reader.getRecordCount()).isEqualTo(2);

      ParquetMetadata footer = reader.getFooter();
      assertThat(footer).isNotNull();

      ColumnChunkMetaData columnChunkMetaData =
          reader.getRowGroups().get(0).getColumns().get(0);
      assertThat(columnChunkMetaData).isNotNull();

      GeospatialStatistics geospatialStatistics = columnChunkMetaData.getGeospatialStatistics();
      assertThat(geospatialStatistics).isNotNull();

      assertThat(geospatialStatistics.getBoundingBox().getXMin()).isEqualTo(1.0);
      assertThat(geospatialStatistics.getBoundingBox().getXMax()).isEqualTo(2.0);
      assertThat(geospatialStatistics.getBoundingBox().getYMin()).isEqualTo(1.0);
      assertThat(geospatialStatistics.getBoundingBox().getYMax()).isEqualTo(2.0);
    }
  }

  @Test
  public void testBasicReadWriteGeographyValue() throws Exception {
    GeometryFactory geomFactory = new GeometryFactory();

    // A class to convert JTS Geometry objects to and from Well-Known Binary (WKB) format.
    WKBWriter wkbWriter = new WKBWriter();

    // OGC:CRS84 (WGS 84): Uses the order longitude, latitude
    Binary[] points = {
      Binary.fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(1.0, 1.0)))),
      Binary.fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(2.0, 2.0))))
    };

    // A message type that represents a message with a geography column.
    MessageType schema = Types.buildMessage()
        .required(BINARY)
        .as(geographyType()) // Assuming geographyType() is similar to geometryType()
        .named("geography")
        .named("msg");

    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);
    GroupFactory factory = new SimpleGroupFactory(schema);
    Path path = newTempPath();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(path))
        .withConf(conf)
        .withDictionaryEncoding(false)
        .build()) {
      for (Binary value : points) {
        writer.write(factory.newGroup().append("geography", value));
      }
    }

    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(path))) {
      assertThat(reader.getRecordCount()).isEqualTo(2);

      ParquetMetadata footer = reader.getFooter();
      assertThat(footer).isNotNull();

      ColumnChunkMetaData columnChunkMetaData =
          reader.getRowGroups().get(0).getColumns().get(0);
      assertThat(columnChunkMetaData).isNotNull();

      GeospatialStatistics geospatialStatistics = columnChunkMetaData.getGeospatialStatistics();
      assertThat(geospatialStatistics).isNull();
    }
  }

  @Test
  public void testInvalidGeometryPresented() throws Exception {
    GeometryFactory geomFactory = new GeometryFactory();
    WKBWriter wkbWriter = new WKBWriter();

    // Create an array of binary values with a mix of valid and invalid geometry data
    Binary[] geometries = {
      // Valid point
      Binary.fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(1.0, 1.0)))),
      // Invalid "geometry" - corrupt WKB data
      Binary.fromConstantByteArray(new byte[] {0x01, 0x02, 0x03, 0x04}),
      // Another valid point
      Binary.fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(2.0, 2.0))))
    };

    // Create schema with geometry type
    MessageType schema = Types.buildMessage()
        .required(BINARY)
        .as(geometryType(null))
        .named("geometry")
        .named("msg");

    // Write file with mixed valid/invalid geometries
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);
    GroupFactory factory = new SimpleGroupFactory(schema);
    Path path = newTempPath();

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(path))
        .withConf(conf)
        .withDictionaryEncoding(false)
        .build()) {
      for (Binary value : geometries) {
        writer.write(factory.newGroup().append("geometry", value));
      }
    }

    // Read and verify the file
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(path))) {
      assertThat(reader.getRecordCount()).isEqualTo(3);

      ParquetMetadata footer = reader.getFooter();
      assertThat(footer).isNotNull();

      ColumnChunkMetaData columnChunkMetaData =
          reader.getRowGroups().get(0).getColumns().get(0);
      assertThat(columnChunkMetaData).isNotNull();

      // The key verification - when invalid geometry data is present,
      // geospatial statistics should omit the invalid data
      GeospatialStatistics geospatialStatistics = columnChunkMetaData.getGeospatialStatistics();
      assertThat(geospatialStatistics)
          .as("Geospatial statistics should omit the corrupt geometry")
          .isNotNull();

      // further check fields in the GeospatialStatistics
      assertThat(geospatialStatistics.isValid())
          .as("Geospatial statistics should be valid")
          .isTrue();
      assertThat(geospatialStatistics.getBoundingBox())
          .as("Bounding box should not be null")
          .isNotNull();
      assertThat(geospatialStatistics.getGeospatialTypes())
          .as("Geospatial types should not be null")
          .isNotNull();
      assertThat(geospatialStatistics.getGeospatialTypes().isValid())
          .as("Geospatial types should be valid")
          .isTrue();
    }
  }
}
