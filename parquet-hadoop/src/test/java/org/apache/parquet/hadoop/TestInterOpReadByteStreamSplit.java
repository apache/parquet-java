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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestInterOpReadByteStreamSplit {
  // The link includes a reference to a specific commit. To take a newer version - update this link.
  private static final String PARQUET_TESTING_REPO = "https://github.com/apache/parquet-testing/raw/4cb3cff/data/";
  private static String PARQUET_TESTING_PATH = "target/parquet-testing/data";
  private static String FLOATS_FILE = "byte_stream_split.zstd.parquet";

  private static final Logger LOG = LoggerFactory.getLogger(TestInterOpReadByteStreamSplit.class);
  private OkHttpClient httpClient = new OkHttpClient();

  @Test
  public void testReadFloats() throws IOException {
    Path rootPath = new Path(PARQUET_TESTING_PATH);

    // Test simple parquet file with lz4 raw compressed
    Path floatsFile = downloadInterOpFiles(rootPath, FLOATS_FILE, httpClient);
    final int expectRows = 300;

    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), floatsFile).build()) {
      for (int i = 0; i < expectRows; ++i) {
        Group group = reader.read();
        assertTrue(group != null);
        float fval = group.getFloat("f32", 0);
        double dval = group.getDouble("f64", 0);
        // Values are from the normal distribution
        assertTrue(Math.abs(fval) < 4.0);
        assertTrue(Math.abs(dval) < 4.0);
        switch (i) {
          case 0:
            assertEquals(1.7640524f, fval, 0.0);
            assertEquals(-1.3065268517353166, dval, 0.0);
            break;
          case 1:
            assertEquals(0.4001572f, fval, 0.0);
            assertEquals(1.658130679618188, dval, 0.0);
            break;
          case expectRows - 2:
            assertEquals(-0.39944902f, fval, 0.0);
            assertEquals(-0.9301565025243212, dval, 0.0);
            break;
          case expectRows - 1:
            assertEquals(0.37005588f, fval, 0.0);
            assertEquals(-0.17858909208732915, dval, 0.0);
            break;
        }
      }
      assertTrue(reader.read() == null);
    }
  }

  private Path downloadInterOpFiles(Path rootPath, String fileName, OkHttpClient httpClient) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = rootPath.getFileSystem(conf);
    if (!fs.exists(rootPath)) {
      LOG.info("Create folder for interOp files: " + rootPath);
      if (!fs.mkdirs(rootPath)) {
        throw new IOException("Cannot create path " + rootPath);
      }
    }

    Path file = new Path(rootPath, fileName);
    if (!fs.exists(file)) {
      String downloadUrl = PARQUET_TESTING_REPO + fileName;
      LOG.info("Download interOp file: " + downloadUrl);
      Request request = new Request.Builder().url(downloadUrl).build();
      Response response = httpClient.newCall(request).execute();
      if (!response.isSuccessful()) {
        throw new IOException("Failed to download file: " + response);
      }
      try (FSDataOutputStream fdos = fs.create(file)) {
        fdos.write(response.body().bytes());
      }
    }
    return file;
  }
}
