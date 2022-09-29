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

package org.apache.parquet.hadoop.codec;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestInteropReadLz4RawCodec {

  // The link includes a reference to a specific commit. To take a newer version - update this link.
  private static final String PARQUET_TESTING_REPO = "https://github.com/apache/parquet-testing/raw/19fcd4d/data/";
  private static String PARQUET_TESTING_PATH = "target/parquet-testing/data";
  private static String SIMPLE_FILE = "lz4_raw_compressed.parquet";
  private static String LARGER_FILE = "lz4_raw_compressed_larger.parquet";

  private static final Logger LOG = LoggerFactory.getLogger(TestInteropReadLz4RawCodec.class);
  private OkHttpClient httpClient = new OkHttpClient();

  @Test
  public void testInteropReadLz4RawParquetFiles() throws IOException {
    Path rootPath = new Path(PARQUET_TESTING_PATH);
    LOG.info("======== testInteropReadLz4RawParquetFiles {} ========", rootPath.toString());

    // Test simple parquet file with lz4 raw compressed
    Path simpleFile = downloadInteropFiles(rootPath, SIMPLE_FILE, httpClient);
    readParquetFile(simpleFile, 4);

    // Test larger parquet file with lz4 raw compressed
    Path largerFile = downloadInteropFiles(rootPath, LARGER_FILE, httpClient);
    readParquetFile(largerFile, 10000);
  }

  private Path downloadInteropFiles(Path rootPath, String fileName, OkHttpClient httpClient) throws IOException {
    LOG.info("Download interop files if needed");
    Configuration conf = new Configuration();
    FileSystem fs = rootPath.getFileSystem(conf);
    LOG.info(rootPath + " exists?: " + fs.exists(rootPath));
    if (!fs.exists(rootPath)) {
      LOG.info("Create folder for interop files: " + rootPath);
      if (!fs.mkdirs(rootPath)) {
        throw new IOException("Cannot create path " + rootPath);
      }
    }

    Path file = new Path(rootPath, fileName);
    if (!fs.exists(file)) {
      String downloadUrl = PARQUET_TESTING_REPO + fileName;
      LOG.info("Download interop file: " + downloadUrl);
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

  private void readParquetFile(Path filePath, int expectedNumRows) throws IOException {
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), filePath).build()) {
      int numRows = 0;
      while (reader.read() != null) {
        numRows++;
      }
      reader.close();
      assertEquals(expectedNumRows, numRows);
    }
  }

}
