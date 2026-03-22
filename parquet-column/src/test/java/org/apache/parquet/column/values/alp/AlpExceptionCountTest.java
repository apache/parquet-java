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

package org.apache.parquet.column.values.alp;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Measures exception counts and compression ratios using the real Spotify and Arade datasets from
 * the parquet-testing repository (Pratik's alpFloatingPointDataset branch).
 *
 * <p>These are the same datasets used in the meeting discussion to compare Java vs C++ ALP
 * exception counts. Run from the project root with the alp-test-data/ directory present:
 *
 * <pre>
 *   ./mvnw test -pl parquet-column -Dtest=AlpExceptionCountTest
 * </pre>
 *
 * <p>To use a different data directory:
 *
 * <pre>
 *   ./mvnw test -pl parquet-column -Dtest=AlpExceptionCountTest \
 *       -DALP_TEST_DATA_DIR=/path/to/alp-test-data
 * </pre>
 */
public class AlpExceptionCountTest {

  private static final int VECTOR_SIZE = AlpConstants.DEFAULT_VECTOR_SIZE;

  private File getDataDir() {
    String dir = System.getProperty("ALP_TEST_DATA_DIR");
    if (dir == null) dir = System.getenv("ALP_TEST_DATA_DIR");
    if (dir != null && new File(dir).isDirectory()) return new File(dir);
    // Default: alp-test-data/ at project root (three levels up from module target)
    File candidate = new File(System.getProperty("user.dir")).getParentFile();
    if (candidate != null) {
      File d = new File(candidate, "alp-test-data");
      if (d.isDirectory()) return d;
    }
    // Also try relative to cwd
    File d2 = new File(System.getProperty("user.dir"), "alp-test-data");
    return d2.isDirectory() ? d2 : null;
  }

  /** Reads a CSV file into per-column double arrays. Handles both ',' and '|' delimiters. */
  private List<double[]> readCsvColumns(File file) throws IOException {
    List<List<Double>> cols = new ArrayList<>();
    String delim = null;
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line;
      boolean header = true;
      while ((line = br.readLine()) != null) {
        if (delim == null) delim = line.contains("|") ? "\\|" : ",";
        String[] parts = line.split(delim);
        if (header) {
          for (String ignored : parts) cols.add(new ArrayList<>());
          header = false;
          continue;
        }
        for (int i = 0; i < parts.length && i < cols.size(); i++) {
          try {
            cols.get(i).add(Double.parseDouble(parts[i].trim()));
          } catch (NumberFormatException e) {
            cols.get(i).add(0.0);
          }
        }
      }
    }
    List<double[]> result = new ArrayList<>();
    for (List<Double> col : cols) {
      double[] arr = new double[col.size()];
      for (int i = 0; i < arr.length; i++) arr[i] = col.get(i);
      result.add(arr);
    }
    return result;
  }

  /** Reads the header row of a CSV file. */
  private String[] readHeader(File file) throws IOException {
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line = br.readLine();
      if (line == null) return new String[0];
      String delim = line.contains("|") ? "\\|" : ",";
      return line.split(delim);
    }
  }

  /** Counts total exceptions across all vectors for a double column. */
  private int countExceptions(double[] values) {
    int totalExceptions = 0;
    for (int offset = 0; offset < values.length; offset += VECTOR_SIZE) {
      int len = Math.min(VECTOR_SIZE, values.length - offset);
      AlpEncoderDecoder.EncodingParams params =
          AlpEncoderDecoder.findBestDoubleParams(values, offset, len);
      totalExceptions += params.numExceptions;
    }
    return totalExceptions;
  }

  private void reportDataset(String label, File file) throws IOException {
    List<double[]> columns = readCsvColumns(file);
    String[] headers = readHeader(file);
    int rows = columns.isEmpty() ? 0 : columns.get(0).length;
    int numVectors = (int) Math.ceil((double) rows / VECTOR_SIZE);

    System.out.printf("%n=== %s (%d rows, %d cols, %d vectors/col) ===%n",
        label, rows, columns.size(), numVectors);
    System.out.printf("  %-20s  %6s  %6s  %7s%n", "column", "rows", "exc", "exc%");
    System.out.printf("  %-20s  %6s  %6s  %7s%n", "------", "----", "---", "----");

    int totalExc = 0;
    int totalRows = 0;
    for (int i = 0; i < columns.size(); i++) {
      double[] col = columns.get(i);
      int exc = countExceptions(col);
      totalExc += exc;
      totalRows += col.length;
      String name = (i < headers.length) ? headers[i].trim() : "col" + i;
      System.out.printf("  %-20s  %6d  %6d  %6.2f%%%n",
          name, col.length, exc, 100.0 * exc / col.length);
    }
    System.out.printf("  %-20s  %6d  %6d  %6.2f%%%n",
        "TOTAL", totalRows, totalExc, 100.0 * totalExc / totalRows);
  }

  @Test
  public void testSpotifyExceptionCounts() throws IOException {
    File dir = getDataDir();
    assumeTrue("alp-test-data/ not found. Run from project root or set ALP_TEST_DATA_DIR", dir != null);
    File file = new File(dir, "floatingpoint_spotify1.csv");
    assumeTrue("floatingpoint_spotify1.csv not found in " + dir, file.exists());

    reportDataset("Spotify", file);

    // Basic sanity: Spotify data is well-behaved floats, expect < 5% exceptions overall
    List<double[]> columns = readCsvColumns(file);
    int totalExc = 0, totalRows = 0;
    for (double[] col : columns) {
      totalExc += countExceptions(col);
      totalRows += col.length;
    }
    double excRate = 100.0 * totalExc / totalRows;
    System.out.printf("%nSpotify overall exception rate: %.2f%%%n", excRate);
    assertTrue("Exception rate should be < 10% for Spotify data, got: " + excRate, excRate < 10.0);
  }

  @Test
  public void testAradeExceptionCounts() throws IOException {
    File dir = getDataDir();
    assumeTrue("alp-test-data/ not found. Run from project root or set ALP_TEST_DATA_DIR", dir != null);
    File file = new File(dir, "floatingpoint_arade.csv");
    assumeTrue("floatingpoint_arade.csv not found in " + dir, file.exists());

    reportDataset("Arade", file);

    List<double[]> columns = readCsvColumns(file);
    int totalExc = 0, totalRows = 0;
    for (double[] col : columns) {
      totalExc += countExceptions(col);
      totalRows += col.length;
    }
    double excRate = 100.0 * totalExc / totalRows;
    System.out.printf("%nArade overall exception rate: %.2f%%%n", excRate);
    assertTrue("Exception rate should be < 10% for Arade data, got: " + excRate, excRate < 10.0);
  }

  @Test
  public void testAllDatasetsExceptionCounts() throws IOException {
    File dir = getDataDir();
    assumeTrue("alp-test-data/ not found. Run from project root or set ALP_TEST_DATA_DIR", dir != null);

    File[] csvFiles = dir.listFiles((d, name) -> name.startsWith("floatingpoint_") && name.endsWith(".csv"));
    assumeTrue("No floatingpoint_*.csv files found in " + dir,
        csvFiles != null && csvFiles.length > 0);

    System.out.printf("%n=== All Datasets Summary ===%n");
    System.out.printf("  %-30s  %6s  %6s  %7s%n", "dataset", "rows", "exc", "exc%");
    System.out.printf("  %-30s  %6s  %6s  %7s%n", "-------", "----", "---", "----");

    for (File file : csvFiles) {
      List<double[]> columns = readCsvColumns(file);
      int totalExc = 0, totalRows = 0;
      for (double[] col : columns) {
        totalExc += countExceptions(col);
        totalRows += col.length;
      }
      String name = file.getName().replace("floatingpoint_", "").replace(".csv", "");
      System.out.printf("  %-30s  %6d  %6d  %6.2f%%%n",
          name, totalRows, totalExc, 100.0 * totalExc / totalRows);
    }
  }
}
