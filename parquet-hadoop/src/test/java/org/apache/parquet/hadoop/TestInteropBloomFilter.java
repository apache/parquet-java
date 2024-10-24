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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestInteropBloomFilter {

  // The link includes a reference to a specific commit. To take a newer version - update this link.
  private static final String PARQUET_TESTING_REPO = "https://github.com/apache/parquet-testing/raw/d69d979/data/";
  private static String PARQUET_TESTING_PATH = "target/parquet-testing/data";
  // parquet-testing: https://github.com/apache/parquet-testing/pull/22
  private static String DATA_INDEX_BLOOM_FILE = "data_index_bloom_encoding_stats.parquet";
  // parquet-testing: https://github.com/apache/parquet-testing/pull/43
  private static String DATA_INDEX_BLOOM_WITH_LENGTH_FILE = "data_index_bloom_encoding_with_length.parquet";

  private static final Logger LOG = LoggerFactory.getLogger(TestInteropBloomFilter.class);
  private OkHttpClient httpClient = new OkHttpClient();

  @Test
  public void testReadDataIndexBloomParquetFiles() throws IOException {
    Path rootPath = new Path(PARQUET_TESTING_PATH);
    LOG.info("======== testReadDataIndexBloomParquetFiles {} ========", rootPath);

    Path filePath = downloadInterOpFiles(rootPath, DATA_INDEX_BLOOM_FILE, httpClient);

    int expectedRowCount = 14;
    String[] expectedValues = new String[] {
      "Hello",
      "This is",
      "a",
      "test",
      "How",
      "are you",
      "doing ",
      "today",
      "the quick",
      "brown fox",
      "jumps",
      "over",
      "the lazy",
      "dog"
    };

    String[] unexpectedValues = new String[] {"b", "c", "d"};

    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), filePath).build()) {
      for (int i = 0; i < expectedRowCount; ++i) {
        Group group = reader.read();
        if (group == null) {
          fail("Should not reach end of file");
        }
        assertEquals(expectedValues[i], group.getString(0, 0));
      }
    }

    ParquetFileReader reader = new ParquetFileReader(
        HadoopInputFile.fromPath(filePath, new Configuration()),
        ParquetReadOptions.builder().build());
    List<BlockMetaData> blocks = reader.getRowGroups();
    blocks.forEach(block -> {
      try {
        assertEquals(14, block.getRowCount());
        ColumnChunkMetaData idMeta = block.getColumns().get(0);
        BloomFilter bloomFilter = reader.readBloomFilter(idMeta);
        Assert.assertNotNull(bloomFilter);
        assertEquals(192, idMeta.getBloomFilterOffset());
        assertEquals(-1, idMeta.getBloomFilterLength());
        for (int i = 0; i < expectedRowCount; ++i) {
          assertTrue(bloomFilter.findHash(bloomFilter.hash(Binary.fromString(expectedValues[i]))));
        }
        for (int i = 0; i < unexpectedValues.length; ++i) {
          assertFalse(bloomFilter.findHash(bloomFilter.hash(Binary.fromString(unexpectedValues[i]))));
        }
        assertEquals(152, idMeta.getTotalSizeWithDecrypt());
        assertEquals(163, idMeta.getTotalUncompressedSizeWithDecrypt());
        assertEquals(181, idMeta.getOffsetIndexReference().getOffset());
        assertEquals(11, idMeta.getOffsetIndexReference().getLength());
        assertEquals(156, idMeta.getColumnIndexReference().getOffset());
        assertEquals(25, idMeta.getColumnIndexReference().getLength());
      } catch (IOException e) {
        fail("Should not throw exception: " + e.getMessage());
      }
    });
  }

  @Test
  public void testReadDataIndexBloomWithLengthParquetFiles() throws IOException {
    Path rootPath = new Path(PARQUET_TESTING_PATH);
    LOG.info("======== testReadDataIndexBloomWithLengthParquetFiles {} ========", rootPath);

    Path filePath = downloadInterOpFiles(rootPath, DATA_INDEX_BLOOM_WITH_LENGTH_FILE, httpClient);

    int expectedRowCount = 14;
    String[] expectedValues = new String[] {
      "Hello",
      "This is",
      "a",
      "test",
      "How",
      "are you",
      "doing ",
      "today",
      "the quick",
      "brown fox",
      "jumps",
      "over",
      "the lazy",
      "dog"
    };

    String[] unexpectedValues = new String[] {"b", "c", "d"};

    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), filePath).build()) {
      for (int i = 0; i < expectedRowCount; ++i) {
        Group group = reader.read();
        if (group == null) {
          fail("Should not reach end of file");
        }
        assertEquals(expectedValues[i], group.getString(0, 0));
      }
    }

    ParquetFileReader reader = new ParquetFileReader(
        HadoopInputFile.fromPath(filePath, new Configuration()),
        ParquetReadOptions.builder().build());
    List<BlockMetaData> blocks = reader.getRowGroups();
    blocks.forEach(block -> {
      try {
        assertEquals(14, block.getRowCount());
        ColumnChunkMetaData idMeta = block.getColumns().get(0);
        BloomFilter bloomFilter = reader.readBloomFilter(idMeta);
        Assert.assertNotNull(bloomFilter);
        assertEquals(253, idMeta.getBloomFilterOffset());
        assertEquals(2064, idMeta.getBloomFilterLength());
        for (int i = 0; i < expectedRowCount; ++i) {
          assertTrue(bloomFilter.findHash(bloomFilter.hash(Binary.fromString(expectedValues[i]))));
        }
        for (int i = 0; i < unexpectedValues.length; ++i) {
          assertFalse(bloomFilter.findHash(bloomFilter.hash(Binary.fromString(unexpectedValues[i]))));
        }
        assertEquals(199, idMeta.getTotalSizeWithDecrypt());
        assertEquals(199, idMeta.getTotalUncompressedSizeWithDecrypt());
        assertEquals(2342, idMeta.getOffsetIndexReference().getOffset());
        assertEquals(11, idMeta.getOffsetIndexReference().getLength());
        assertEquals(2317, idMeta.getColumnIndexReference().getOffset());
        assertEquals(25, idMeta.getColumnIndexReference().getLength());
      } catch (Exception e) {
        fail("Should not throw exception: " + e.getMessage());
      }
    });
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
