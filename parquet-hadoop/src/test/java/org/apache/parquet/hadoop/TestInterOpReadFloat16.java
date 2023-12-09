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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestInterOpReadFloat16 {

  // The link includes a reference to a specific commit. To take a newer version - update this link.
  private static final String PARQUET_TESTING_REPO = "https://github.com/apache/parquet-testing/raw/da467da/data/";
  private static String PARQUET_TESTING_PATH = "target/parquet-testing/data";
  private static String FLOAT16_NONZEROS_NANS_FILE = "float16_nonzeros_and_nans.parquet";
  private static String FLOAT16_ZEROS_NANS_FILE = "float16_zeros_and_nans.parquet";

  private static final Logger LOG = LoggerFactory.getLogger(TestInterOpReadFloat16.class);
  private OkHttpClient httpClient = new OkHttpClient();

  @Test
  public void testInterOpReadFloat16NonZerosAndNansParquetFiles() throws IOException {
    Path rootPath = new Path(PARQUET_TESTING_PATH);
    LOG.info("======== testInterOpReadFloat16NonZerosAndNansParquetFiles {} ========", rootPath);

    Path filePath = downloadInterOpFiles(rootPath, FLOAT16_NONZEROS_NANS_FILE, httpClient);
    final int expectRows = 8;
    Binary[] c0ExpectValues = {
      null,
      Binary.fromConstantByteArray(new byte[] {0x00, 0x3c}),
      Binary.fromConstantByteArray(new byte[] {0x00, (byte) 0xc0}),
      Binary.fromConstantByteArray(new byte[] {0x00, 0x7e}),
      Binary.fromConstantByteArray(new byte[] {0x00, 0x00}),
      Binary.fromConstantByteArray(new byte[] {0x00, (byte) 0xbc}),
      Binary.fromConstantByteArray(new byte[] {0x00, (byte) 0x80}),
      Binary.fromConstantByteArray(new byte[] {0x00, (byte) 0x40}),
    };

    try (ParquetFileReader reader =
        ParquetFileReader.open(HadoopInputFile.fromPath(filePath, new Configuration()))) {
      ColumnChunkMetaData column =
          reader.getFooter().getBlocks().get(0).getColumns().get(0);

      assertArrayEquals(
          new byte[] {0x00, (byte) 0xc0}, column.getStatistics().getMinBytes());
      // 0x40 equals @ in ASCII
      assertArrayEquals(new byte[] {0x00, 0x40}, column.getStatistics().getMaxBytes());
    }

    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), filePath).build()) {
      for (int i = 0; i < expectRows; ++i) {
        Group group = reader.read();
        if (group == null) {
          fail("Should not reach end of file before " + expectRows + " rows");
        }
        if (group.getFieldRepetitionCount(0) != 0) {
          // Check if the field is not null
          assertEquals(c0ExpectValues[i], group.getBinary(0, 0));
        } else {
          // Check if the field is null
          assertEquals(0, i);
        }
      }
    }
  }

  @Test
  public void testInterOpReadFloat16ZerosAndNansParquetFiles() throws IOException {
    Path rootPath = new Path(PARQUET_TESTING_PATH);
    LOG.info("======== testInterOpReadFloat16ZerosAndNansParquetFiles {} ========", rootPath);

    Path filePath = downloadInterOpFiles(rootPath, FLOAT16_ZEROS_NANS_FILE, httpClient);
    final int expectRows = 3;
    Binary[] c0ExpectValues = {
      null,
      Binary.fromConstantByteArray(new byte[] {0x00, 0x00}),
      Binary.fromConstantByteArray(new byte[] {0x00, (byte) 0x7e})
    };

    try (ParquetFileReader reader =
        ParquetFileReader.open(HadoopInputFile.fromPath(filePath, new Configuration()))) {
      ColumnChunkMetaData column =
          reader.getFooter().getBlocks().get(0).getColumns().get(0);

      assertArrayEquals(
          new byte[] {0x00, (byte) 0x80}, column.getStatistics().getMinBytes());
      assertArrayEquals(new byte[] {0x00, 0x00}, column.getStatistics().getMaxBytes());
    }

    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), filePath).build()) {
      for (int i = 0; i < expectRows; ++i) {
        Group group = reader.read();
        if (group == null) {
          fail("Should not reach end of file before " + expectRows + " rows");
        }
        if (group.getFieldRepetitionCount(0) != 0) {
          // Check if the field is not null
          assertEquals(c0ExpectValues[i], group.getBinary(0, 0));
        } else {
          // Check if the field is null
          assertEquals(0, i);
        }
      }
    }
  }

  private Path downloadInterOpFiles(Path rootPath, String fileName, OkHttpClient httpClient) throws IOException {
    LOG.info("Download interOp files if needed");
    Configuration conf = new Configuration();
    FileSystem fs = rootPath.getFileSystem(conf);
    LOG.info(rootPath + " exists?: " + fs.exists(rootPath));
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
