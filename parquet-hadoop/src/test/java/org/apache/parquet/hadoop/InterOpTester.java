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

import java.io.IOException;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InterOpTester {
  private static final String PARQUET_TESTING_REPO = "https://github.com/apache/parquet-testing/raw/";
  private static final String PARQUET_TESTING_PATH = "target/parquet-testing/";
  private static final Logger LOG = LoggerFactory.getLogger(InterOpTester.class);
  private OkHttpClient httpClient = new OkHttpClient();

  public Path GetInterOpFile(String fileName, String changeset) throws IOException {
    return GetInterOpFile(fileName, changeset, "data");
  }

  /**
   * Get interOp file from parquet-testing repo, possibly downloading it.
   *
   * @param fileName The name of the file to get.
   * @param changeset The changeset ID in the parquet-testing repo.
   * @param subdir The subdirectory the file lives in inside the repo (for example "data").
   * @return Path The local path to the interOp file.
   */
  public Path GetInterOpFile(String fileName, String changeset, String subdir) throws IOException {
    Path rootPath = new Path(PARQUET_TESTING_PATH, subdir);
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
      String downloadUrl = String.format("%s/%s/%s/%s", PARQUET_TESTING_REPO, changeset, subdir, fileName);
      LOG.info("Download interOp file: " + downloadUrl);
      Request request = new Request.Builder().url(downloadUrl).build();
      try (Response response = httpClient.newCall(request).execute()) {
        if (!response.isSuccessful()) {
          throw new IOException("Failed to download file: " + response);
        }
        try (FSDataOutputStream fdos = fs.create(file)) {
          fdos.write(response.body().bytes());
        }
      }
    }
    return file;
  }
}
