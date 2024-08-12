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

import static org.apache.parquet.schema.LogicalTypeAnnotation.geometryType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

import java.io.*;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.geometry.GeometryStatistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation.Edges;
import org.apache.parquet.schema.LogicalTypeAnnotation.GeometryEncoding;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBWriter;

public class TestGeometryTypeRoundTrip {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private Path newTempPath() throws IOException {
    File file = temp.newFile();
    Preconditions.checkArgument(file.delete(), "Could not remove temp file");
    return file.toPath();
  }

  @Test
  public void testEPSG4326BasicReadWriteGeometryValue() throws Exception {
    GeometryFactory geomFactory = new GeometryFactory();

    // A class to convert JTS Geometry objects to and from Well-Known Binary (WKB) format.
    WKBWriter wkbWriter = new WKBWriter();

    // EPSG:4326: Also known as WGS 84, it uses latitude and longitude coordinates.
    Binary[] points = {
      Binary.fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(1.0, 1.0)))),
      Binary.fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(2.0, 2.0))))
    };

    // A message type that represents a message with a geometry column.
    MessageType schema = Types.buildMessage()
        .required(BINARY)
        .as(geometryType(GeometryEncoding.WKB, Edges.PLANAR, "EPSG:4326", "PROJJSON", null))
        .named("col_geom")
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
        writer.write(factory.newGroup().append("col_geom", value));
      }
    }

    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(path))) {
      Assert.assertEquals(2, reader.getRecordCount());

      ParquetMetadata footer = reader.getFooter();
      Assert.assertNotNull(footer);

      ColumnChunkMetaData columnChunkMetaData =
          reader.getRowGroups().get(0).getColumns().get(0);
      Assert.assertNotNull(columnChunkMetaData);

      BinaryStatistics binaryStatistics = (BinaryStatistics) columnChunkMetaData.getStatistics();
      GeometryStatistics geometryStatistics = binaryStatistics.getGeometryStatistics();
      Assert.assertNotNull(geometryStatistics);
      Assert.assertEquals(1.0, geometryStatistics.getBoundingBox().getXMin(), 0.0);
      Assert.assertEquals(2.0, geometryStatistics.getBoundingBox().getXMax(), 0.0);
      Assert.assertEquals(1.0, geometryStatistics.getBoundingBox().getYMin(), 0.0);
      Assert.assertEquals(2.0, geometryStatistics.getBoundingBox().getYMax(), 0.0);
      Assert.assertNotNull(geometryStatistics.getCoverings());

      Assert.assertEquals(
          "[Covering{geometry=POLYGON ((1 1, 1 2, 2 2, 2 1, 1 1)), kind=WKB}]",
          geometryStatistics.getCoverings().toString());

      ColumnIndex columnIndex = reader.readColumnIndex(columnChunkMetaData);
      Assert.assertNotNull(columnIndex);

      List<GeometryStatistics> pageGeometryStatistics = columnIndex.getGeometryStatistics();
      Assert.assertNotNull(pageGeometryStatistics);
      Assert.assertEquals(
          1.0, pageGeometryStatistics.get(0).getBoundingBox().getXMin(), 0.0);
      Assert.assertEquals(
          2.0, pageGeometryStatistics.get(0).getBoundingBox().getXMax(), 0.0);
      Assert.assertEquals(
          1.0, pageGeometryStatistics.get(0).getBoundingBox().getYMin(), 0.0);
      Assert.assertEquals(
          2.0, pageGeometryStatistics.get(0).getBoundingBox().getYMax(), 0.0);
    }
  }

  @Test
  public void testEPSG4326BasicReadWriteGeometryValueWithCovering() throws Exception {
    GeometryFactory geomFactory = new GeometryFactory();

    // A class to convert JTS Geometry objects to and from Well-Known Binary (WKB) format.
    WKBWriter wkbWriter = new WKBWriter();

    // EPSG:4326: Also known as WGS 84, it uses latitude and longitude coordinates.
    Binary[] points = {
      Binary.fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(1.0, 1.0)))),
      Binary.fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(2.0, 2.0))))
    };

    // A message type that represents a message with a geometry column.
    MessageType schema = Types.buildMessage()
        .required(BINARY)
        .as(geometryType(GeometryEncoding.WKB, Edges.PLANAR, "EPSG:4326", "PROJJSON", null))
        .named("col_geom")
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
        writer.write(factory.newGroup().append("col_geom", value));
      }
    }

    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(path))) {
      Assert.assertEquals(2, reader.getRecordCount());

      ParquetMetadata footer = reader.getFooter();
      Assert.assertNotNull(footer);

      ColumnChunkMetaData columnChunkMetaData =
          reader.getRowGroups().get(0).getColumns().get(0);
      Assert.assertNotNull(columnChunkMetaData);

      BinaryStatistics binaryStatistics = (BinaryStatistics) columnChunkMetaData.getStatistics();
      GeometryStatistics geometryStatistics = binaryStatistics.getGeometryStatistics();
      Assert.assertNotNull(geometryStatistics);

      Assert.assertNotNull(geometryStatistics.getCoverings());
      Assert.assertEquals(
          "[Covering{geometry=POLYGON ((1 1, 1 2, 2 2, 2 1, 1 1)), kind=WKB}]",
          geometryStatistics.getCoverings().toString());

      ColumnIndex columnIndex = reader.readColumnIndex(columnChunkMetaData);
      Assert.assertNotNull(columnIndex);
    }
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testEPSG3857BasicReadWriteGeometryValue() throws Exception {
    GeometryFactory geomFactory = new GeometryFactory();

    // A class to convert JTS Geometry objects to and from Well-Known Binary (WKB) format.
    WKBWriter wkbWriter = new WKBWriter();

    // EPSG:3857: Web Mercator projection, commonly used by web mapping applications.
    Binary[] points = {
      Binary.fromConstantByteArray(
          wkbWriter.write(geomFactory.createPoint(new Coordinate(-8237491.37, 4974209.75)))),
      Binary.fromConstantByteArray(
          wkbWriter.write(geomFactory.createPoint(new Coordinate(-8237491.37, 4974249.75)))),
      Binary.fromConstantByteArray(
          wkbWriter.write(geomFactory.createPoint(new Coordinate(-8237531.37, 4974209.75)))),
      Binary.fromConstantByteArray(
          wkbWriter.write(geomFactory.createPoint(new Coordinate(-8237531.37, 4974249.75))))
    };

    // A message type that represents a message with a geometry column.
    MessageType schema = Types.buildMessage()
        .required(BINARY)
        .as(geometryType(GeometryEncoding.WKB, Edges.SPHERICAL, "EPSG:3857", "PROJJSON", null))
        .named("col_geom")
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
        writer.write(factory.newGroup().append("col_geom", value));
      }
    }

    //     try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(path))) {
    //       Assert.assertEquals(4, reader.getRecordCount());
    //
    //       ParquetMetadata footer = reader.getFooter();
    //       Assert.assertNotNull(footer);
    //
    //       ColumnChunkMetaData columnChunkMetaData =
    //           reader.getRowGroups().get(0).getColumns().get(0);
    //       Assert.assertNotNull(columnChunkMetaData);
    //
    //       BinaryStatistics binaryStatistics = (BinaryStatistics) columnChunkMetaData.getStatistics();
    //       GeometryStatistics geometryStatistics = binaryStatistics.getGeometryStatistics();
    //       Assert.assertNotNull(geometryStatistics);
    //
    //       Assert.assertNotNull(geometryStatistics.getCoverings());
    //       Assert.assertEquals(
    //           "[Covering{geometry=POLYGON ((-8237531.370000001 4974209.750000001, -8237531.370000001
    // 4974249.750000003, -8237490.369999999 4974249.750000003, -8237490.369999999 4974209.750000001,
    // -8237531.370000001 4974209.750000001)), kind=WKB}]",
    //           geometryStatistics.getCoverings().toString());
    //
    //       ColumnIndex columnIndex = reader.readColumnIndex(columnChunkMetaData);
    //       Assert.assertNotNull(columnIndex);
    //     }
  }
}
