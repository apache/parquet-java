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
package org.apache.parquet.hadoop;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.junit.jupiter.api.Test;

/**
 * Verifies ALP files written by the Arrow C++ and parquet-java implementations against
 * independent CSV data.
 */
public class TestInteropAlpEncoding {

  private static Path resourcePath(String name) {
    try {
      return new Path(TestInteropAlpEncoding.class.getResource("/" + name).toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testReadAlpAradeParquet() throws IOException {
    Path parquetPath = resourcePath("alp_arade.parquet");
    String[] columnNames = {"value1", "value2", "value3", "value4"};
    int expectedRows = 15000;

    double[][] expected = readExpectedCsv("/alp_arade_expect.csv.gz", columnNames.length, expectedRows);

    List<Group> rows = readParquetGroups(parquetPath);
    assertThat(rows).as("row count").hasSize(expectedRows);

    verifyAlpEncoding(parquetPath);

    for (int r = 0; r < expectedRows; r++) {
      Group group = rows.get(r);
      for (int c = 0; c < columnNames.length; c++) {
        double actual = group.getDouble(columnNames[c], 0);
        assertThat(Double.doubleToLongBits(actual))
            .as(String.format("Mismatch at row %d, column %s", r, columnNames[c]))
            .isEqualTo(Double.doubleToLongBits(expected[c][r]));
      }
    }
  }

  @Test
  public void testReadAlpSpotify1Parquet() throws IOException {
    Path parquetPath = resourcePath("alp_spotify1.parquet");
    String[] columnNames = {
      "danceability",
      "energy",
      "loudness",
      "speechiness",
      "acousticness",
      "instrumentalness",
      "liveness",
      "valence",
      "tempo"
    };
    int expectedRows = 15000;

    double[][] expected = readExpectedCsv("/alp_spotify1_expect.csv.gz", columnNames.length, expectedRows);

    List<Group> rows = readParquetGroups(parquetPath);
    assertThat(rows).as("row count").hasSize(expectedRows);

    verifyAlpEncoding(parquetPath);

    for (int r = 0; r < expectedRows; r++) {
      Group group = rows.get(r);
      for (int c = 0; c < columnNames.length; c++) {
        double actual = group.getDouble(columnNames[c], 0);
        assertThat(Double.doubleToLongBits(actual))
            .as(String.format("Mismatch at row %d, column %s", r, columnNames[c]))
            .isEqualTo(Double.doubleToLongBits(expected[c][r]));
      }
    }
  }

  @Test
  public void testReadAlpJavaAradeParquet() throws IOException {
    Path parquetPath = resourcePath("alp_java_arade.parquet");
    String[] columnNames = {"value1", "value2", "value3", "value4"};
    int expectedRows = 15000;

    double[][] expected = readExpectedCsv("/alp_arade_expect.csv.gz", columnNames.length, expectedRows);

    List<Group> rows = readParquetGroups(parquetPath);
    assertThat(rows).as("row count").hasSize(expectedRows);

    verifyAlpEncoding(parquetPath);

    for (int r = 0; r < expectedRows; r++) {
      Group group = rows.get(r);
      for (int c = 0; c < columnNames.length; c++) {
        double actual = group.getDouble(columnNames[c], 0);
        assertThat(Double.doubleToLongBits(actual))
            .as(String.format("Mismatch at row %d, column %s", r, columnNames[c]))
            .isEqualTo(Double.doubleToLongBits(expected[c][r]));
      }
    }
  }

  @Test
  public void testReadAlpJavaSpotify1Parquet() throws IOException {
    Path parquetPath = resourcePath("alp_java_spotify1.parquet");
    String[] columnNames = {
      "danceability",
      "energy",
      "loudness",
      "speechiness",
      "acousticness",
      "instrumentalness",
      "liveness",
      "valence",
      "tempo"
    };
    int expectedRows = 15000;

    double[][] expected = readExpectedCsv("/alp_spotify1_expect.csv.gz", columnNames.length, expectedRows);

    List<Group> rows = readParquetGroups(parquetPath);
    assertThat(rows).as("row count").hasSize(expectedRows);

    verifyAlpEncoding(parquetPath);

    for (int r = 0; r < expectedRows; r++) {
      Group group = rows.get(r);
      for (int c = 0; c < columnNames.length; c++) {
        double actual = group.getDouble(columnNames[c], 0);
        assertThat(Double.doubleToLongBits(actual))
            .as(String.format("Mismatch at row %d, column %s", r, columnNames[c]))
            .isEqualTo(Double.doubleToLongBits(expected[c][r]));
      }
    }
  }

  @Test
  public void testReadAlpFloatAradeParquet() throws IOException {
    Path parquetPath = resourcePath("alp_float_arade.parquet");
    String[] columnNames = {"value1", "value2", "value3", "value4"};
    int expectedRows = 15000;

    float[][] expected = readExpectedCsvFloat("/alp_float_arade_expect.csv.gz", columnNames.length, expectedRows);

    List<Group> rows = readParquetGroups(parquetPath);
    assertThat(rows).as("row count").hasSize(expectedRows);

    verifyAlpEncoding(parquetPath);

    for (int r = 0; r < expectedRows; r++) {
      Group group = rows.get(r);
      for (int c = 0; c < columnNames.length; c++) {
        float actual = group.getFloat(columnNames[c], 0);
        assertThat(Float.floatToIntBits(actual))
            .as(String.format("Mismatch at row %d, column %s", r, columnNames[c]))
            .isEqualTo(Float.floatToIntBits(expected[c][r]));
      }
    }
  }

  @Test
  public void testReadAlpFloatSpotify1Parquet() throws IOException {
    Path parquetPath = resourcePath("alp_float_spotify1.parquet");
    String[] columnNames = {
      "danceability",
      "energy",
      "loudness",
      "speechiness",
      "acousticness",
      "instrumentalness",
      "liveness",
      "valence",
      "tempo"
    };
    int expectedRows = 15000;

    float[][] expected =
        readExpectedCsvFloat("/alp_float_spotify1_expect.csv.gz", columnNames.length, expectedRows);

    List<Group> rows = readParquetGroups(parquetPath);
    assertThat(rows).as("row count").hasSize(expectedRows);

    verifyAlpEncoding(parquetPath);

    for (int r = 0; r < expectedRows; r++) {
      Group group = rows.get(r);
      for (int c = 0; c < columnNames.length; c++) {
        float actual = group.getFloat(columnNames[c], 0);
        assertThat(Float.floatToIntBits(actual))
            .as(String.format("Mismatch at row %d, column %s", r, columnNames[c]))
            .isEqualTo(Float.floatToIntBits(expected[c][r]));
      }
    }
  }

  @Test
  public void testReadAlpJavaFloatAradeParquet() throws IOException {
    Path parquetPath = resourcePath("alp_java_float_arade.parquet");
    String[] columnNames = {"value1", "value2", "value3", "value4"};
    int expectedRows = 15000;

    float[][] expected = readExpectedCsvFloat("/alp_float_arade_expect.csv.gz", columnNames.length, expectedRows);

    List<Group> rows = readParquetGroups(parquetPath);
    assertThat(rows).as("row count").hasSize(expectedRows);

    verifyAlpEncoding(parquetPath);

    for (int r = 0; r < expectedRows; r++) {
      Group group = rows.get(r);
      for (int c = 0; c < columnNames.length; c++) {
        float actual = group.getFloat(columnNames[c], 0);
        assertThat(Float.floatToIntBits(actual))
            .as(String.format("Mismatch at row %d, column %s", r, columnNames[c]))
            .isEqualTo(Float.floatToIntBits(expected[c][r]));
      }
    }
  }

  @Test
  public void testReadAlpJavaFloatSpotify1Parquet() throws IOException {
    Path parquetPath = resourcePath("alp_java_float_spotify1.parquet");
    String[] columnNames = {
      "danceability",
      "energy",
      "loudness",
      "speechiness",
      "acousticness",
      "instrumentalness",
      "liveness",
      "valence",
      "tempo"
    };
    int expectedRows = 15000;

    float[][] expected =
        readExpectedCsvFloat("/alp_float_spotify1_expect.csv.gz", columnNames.length, expectedRows);

    List<Group> rows = readParquetGroups(parquetPath);
    assertThat(rows).as("row count").hasSize(expectedRows);

    verifyAlpEncoding(parquetPath);

    for (int r = 0; r < expectedRows; r++) {
      Group group = rows.get(r);
      for (int c = 0; c < columnNames.length; c++) {
        float actual = group.getFloat(columnNames[c], 0);
        assertThat(Float.floatToIntBits(actual))
            .as(String.format("Mismatch at row %d, column %s", r, columnNames[c]))
            .isEqualTo(Float.floatToIntBits(expected[c][r]));
      }
    }
  }

  private List<Group> readParquetGroups(Path path) throws IOException {
    List<Group> rows = new ArrayList<>();
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), path).build()) {
      Group group;
      while ((group = reader.read()) != null) {
        rows.add(group);
      }
    }
    return rows;
  }

  private void verifyAlpEncoding(Path path) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
        path, new org.apache.hadoop.conf.Configuration()))) {
      List<BlockMetaData> blocks = reader.getFooter().getBlocks();
      for (BlockMetaData block : blocks) {
        for (ColumnChunkMetaData column : block.getColumns()) {
          assertThat(column.getEncodingStats())
              .as("Column " + column.getPath() + " should have encoding stats")
              .isNotNull();
          assertThat(column.getEncodings())
              .as("encodings for column " + column.getPath())
              .contains(Encoding.ALP);
        }
      }
    }
  }

  private double[][] readExpectedCsv(String resourcePath, int numColumns, int expectedRows) throws IOException {
    double[][] columns = new double[numColumns][expectedRows];
    try (InputStream raw = getClass().getResourceAsStream(resourcePath);
        InputStream is = new GZIPInputStream(raw);
        BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
      assertThat(raw).as("CSV resource not found: " + resourcePath).isNotNull();

      String header = br.readLine();
      assertThat(header).as("CSV should have a header").isNotNull();

      int row = 0;
      String line;
      while ((line = br.readLine()) != null) {
        String[] parts = line.split(",");
        assertThat(parts).as("CSV row " + row).hasSize(numColumns);
        for (int c = 0; c < numColumns; c++) {
          columns[c][row] = Double.parseDouble(parts[c]);
        }
        row++;
      }
      assertThat(row).as("CSV should have " + expectedRows + " data rows").isEqualTo(expectedRows);
    }
    return columns;
  }

  private float[][] readExpectedCsvFloat(String resourcePath, int numColumns, int expectedRows) throws IOException {
    float[][] columns = new float[numColumns][expectedRows];
    try (InputStream raw = getClass().getResourceAsStream(resourcePath);
        InputStream is = new GZIPInputStream(raw);
        BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
      assertThat(raw).as("CSV resource not found: " + resourcePath).isNotNull();

      String header = br.readLine();
      assertThat(header).as("CSV should have a header").isNotNull();

      int row = 0;
      String line;
      while ((line = br.readLine()) != null) {
        String[] parts = line.split(",");
        assertThat(parts).as("CSV row " + row).hasSize(numColumns);
        for (int c = 0; c < numColumns; c++) {
          columns[c][row] = Float.parseFloat(parts[c]);
        }
        row++;
      }
      assertThat(row).as("CSV should have " + expectedRows + " data rows").isEqualTo(expectedRows);
    }
    return columns;
  }
}
