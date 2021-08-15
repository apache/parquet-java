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
package org.apache.parquet.hadoop.util;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ColumnEncryptorTest {

  private Configuration conf = new Configuration();
  private Map<String, String> extraMeta = ImmutableMap.of("key1", "value1", "key2", "value2");
  private ColumnEncryptor columnEncryptor = null;
  private final int numRecord = 100000;
  private String inputFile = null;
  private String outputFile = null;
  private TestFileHelper.TestDocs testDocs = null;

  @Before
  public void testSetup() throws Exception {
    columnEncryptor = new ColumnEncryptor(conf);
    testDocs = new TestFileHelper.TestDocs(numRecord);
    inputFile = TestFileHelper.createParquetFile(conf, extraMeta, numRecord, "input", "GZIP",
      ParquetProperties.WriterVersion.PARQUET_1_0, ParquetProperties.DEFAULT_PAGE_SIZE, testDocs);
    outputFile = TestFileHelper.createTempFile("test");
  }

  @Test
  public void testFlatColumn() throws IOException {
    String[] encryptColumns = {"DocId"};
    columnEncryptor.encryptColumns(inputFile, outputFile, Arrays.asList(encryptColumns),
      EncDecProperties.getFileEncryptionProperties(encryptColumns, ColumnEncryptor.EncryptMode.AES_GCM_CTR));

    verifyResultDecryptionWithValidKey();
  }

  @Test
  public void testNestedColumn() throws IOException {
    String[] encryptColumns = {"Links.Forward"};
    columnEncryptor.encryptColumns(inputFile, outputFile, Arrays.asList(encryptColumns),
      EncDecProperties.getFileEncryptionProperties(encryptColumns, ColumnEncryptor.EncryptMode.AES_GCM_CTR));
    verifyResultDecryptionWithValidKey();
  }

  @Test
  public void testNoEncryption() throws IOException {
    String[] encryptColumns = {};
    columnEncryptor.encryptColumns(inputFile, outputFile, Arrays.asList(encryptColumns),
      EncDecProperties.getFileEncryptionProperties(encryptColumns, ColumnEncryptor.EncryptMode.AES_GCM_CTR));
    verifyResultDecryptionWithValidKey();
  }

  @Test
  public void testEncryptAllColumns() throws IOException {
    String[] encryptColumns = {"DocId", "Name", "Gender", "Links.Forward", "Links.Backward"};
    columnEncryptor.encryptColumns(inputFile, outputFile, Arrays.asList(encryptColumns),
      EncDecProperties.getFileEncryptionProperties(encryptColumns, ColumnEncryptor.EncryptMode.AES_GCM_CTR));
    verifyResultDecryptionWithValidKey();
  }

  @Test
  public void testEncryptSomeColumns() throws IOException {
    String[] encryptColumns = {"DocId", "Name", "Links.Forward"};
    columnEncryptor.encryptColumns(inputFile, outputFile, Arrays.asList(encryptColumns),
      EncDecProperties.getFileEncryptionProperties(encryptColumns, ColumnEncryptor.EncryptMode.AES_GCM_CTR));

    ParquetMetadata metaData = getParquetMetadata(EncDecProperties.getFileDecryptionProperties());
    assertTrue(metaData.getBlocks().size() > 0);
    List<ColumnChunkMetaData> columns = metaData.getBlocks().get(0).getColumns();
    Set<String> set = new HashSet<>(Arrays.asList(encryptColumns));
    for (ColumnChunkMetaData column : columns) {
      if (set.contains(column.getPath().toDotString())) {
        assertTrue(column.isEncrypted());
      } else {
        assertFalse(column.isEncrypted());
      }
    }
  }

  private void verifyResultDecryptionWithValidKey() throws IOException  {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(),
      new Path(outputFile)).withConf(conf).withDecryption(EncDecProperties.getFileDecryptionProperties()).build();

    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      assertTrue(group.getLong("DocId", 0) == testDocs.docId[i]);
      assertArrayEquals(group.getBinary("Name", 0).getBytes(), testDocs.name[i].getBytes());
      assertArrayEquals(group.getBinary("Gender", 0).getBytes(), testDocs.gender[i].getBytes());
      Group subGroup = group.getGroup("Links", 0);
      assertArrayEquals(subGroup.getBinary("Forward", 0).getBytes(), testDocs.linkForward[i].getBytes());
      assertArrayEquals(subGroup.getBinary("Backward", 0).getBytes(), testDocs.linkBackward[i].getBytes());
    }
    reader.close();
  }

  private ParquetMetadata getParquetMetadata(FileDecryptionProperties decryptionProperties) throws IOException {
    ParquetMetadata metaData;
    ParquetReadOptions readOptions = ParquetReadOptions.builder()
      .withDecryption(decryptionProperties)
      .build();
    InputFile file = HadoopInputFile.fromPath(new Path(outputFile), conf);
    try (SeekableInputStream in = file.newStream()) {
      metaData  = ParquetFileReader.readFooter(file, readOptions, in);
    }
    return metaData;
  }
}
