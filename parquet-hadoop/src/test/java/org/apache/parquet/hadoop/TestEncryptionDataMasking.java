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
package org.apache.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.ColumnEncryptor;
import org.apache.parquet.hadoop.util.DataMaskingEncDecProperties;
import org.apache.parquet.hadoop.util.EncryptionTestFile;
import org.apache.parquet.hadoop.util.TestFileBuilder;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.parquet.hadoop.util.DataMaskingEncDecProperties.DATA_MASKING_TEST_COL_KEY_1;
import static org.apache.parquet.hadoop.util.DataMaskingEncDecProperties.DATA_MASKING_TEST_COL_KEY_2;
import static org.apache.parquet.hadoop.util.DataMaskingEncDecProperties.DATA_MASKING_TEST_COL_KEY_ID_1;
import static org.apache.parquet.hadoop.util.DataMaskingEncDecProperties.DATA_MASKING_TEST_COL_KEY_ID_2;
import static org.apache.parquet.hadoop.util.DataMaskingEncDecProperties.getFileDecryptionProperties;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestEncryptionDataMasking {
  private Configuration conf = new Configuration();
  private ColumnEncryptor columnEncryptor = null;
  private final int numRecord = 100000;
  private EncryptionTestFile inputFile = null;
  private String outputFile = null;

  private void testSetup(String compression) throws IOException {
    MessageType schema = createSchema();
    columnEncryptor = new ColumnEncryptor(conf);
    inputFile = new TestFileBuilder(conf, schema)
      .withNumRecord(numRecord)
      .withCodec(compression)
      .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
      .build();
    outputFile = TestFileBuilder.createTempFile("test");
  }

  @Test
  public void testNoEncryption() throws IOException {
    String[] encryptColumns = {};
    testSetup("GZIP");
    Map<String, String> encryptColumnToKeyIdMap = new HashMap<>();
    columnEncryptor.encryptColumns(inputFile.getFileName(), outputFile, Arrays.asList(encryptColumns),
      DataMaskingEncDecProperties.getFileEncryptionProperties(encryptColumnToKeyIdMap, ParquetCipher.AES_GCM_CTR_V1, false));
    Map<String, byte[]> decryptKeyMap = new HashMap<>();
    verifyResultDecryptionDataMasking(conf, new HashSet<>(), decryptKeyMap);
  }

  @Test
  public void testSimpleEncDecryption() throws IOException {
    String[] encryptColumns = {"DocId"};
    testSetup("GZIP");
    Map<String, String> encryptColumnToKeyIdMap = new HashMap<>();
    encryptColumnToKeyIdMap.put("DocId", DATA_MASKING_TEST_COL_KEY_ID_1);
    columnEncryptor.encryptColumns(inputFile.getFileName(), outputFile, Arrays.asList(encryptColumns),
      DataMaskingEncDecProperties.getFileEncryptionProperties(encryptColumnToKeyIdMap, ParquetCipher.AES_GCM_CTR_V1, false));
    Map<String, byte[]> decryptKeyMap = new HashMap<>();

    // decrypt with right key
    decryptKeyMap.put(DATA_MASKING_TEST_COL_KEY_ID_1, DATA_MASKING_TEST_COL_KEY_1);
    verifyResultDecryptionDataMasking(conf, new HashSet<>(), decryptKeyMap);

    // decrypt with wrong key
    decryptKeyMap.put(DATA_MASKING_TEST_COL_KEY_ID_1, DATA_MASKING_TEST_COL_KEY_2);
    try {
      verifyResultDecryptionDataMasking(conf, new HashSet<>(), decryptKeyMap);
      fail("Data shouldn't be decrypted!");
    } catch (ParquetDecodingException pde) {
      // decrypt with data masking
      conf.set("parquet.data.masking.columns", "DocId");
      verifyResultDecryptionDataMasking(conf, new HashSet<>(Arrays.asList(encryptColumns)), decryptKeyMap);
    }
  }

  @Test
  public void testNestedEncDecryption() throws IOException {
    String[] encryptColumns = {"Links.Backward"};
    testSetup("GZIP");
    Map<String, String> encryptColumnToKeyIdMap = new HashMap<>();
    encryptColumnToKeyIdMap.put("Links.Backward", DATA_MASKING_TEST_COL_KEY_ID_1);
    columnEncryptor.encryptColumns(inputFile.getFileName(), outputFile, Arrays.asList(encryptColumns),
      DataMaskingEncDecProperties.getFileEncryptionProperties(encryptColumnToKeyIdMap, ParquetCipher.AES_GCM_CTR_V1, false));
    Map<String, byte[]> decryptKeyMap = new HashMap<>();

    // decrypt with correct key
    decryptKeyMap.put(DATA_MASKING_TEST_COL_KEY_ID_1, DATA_MASKING_TEST_COL_KEY_1);
    verifyResultDecryptionDataMasking(conf, new HashSet<>(), decryptKeyMap);

    // decrypt with wrong key
    decryptKeyMap.put(DATA_MASKING_TEST_COL_KEY_ID_1, DATA_MASKING_TEST_COL_KEY_2);
    try {
      verifyResultDecryptionDataMasking(conf, new HashSet<>(), decryptKeyMap);
      fail("Data shouldn't be decrypted!");
    } catch (ParquetDecodingException pde) {
      // decrypt with data masking
      conf.set("parquet.data.masking.columns", "Links.Backward");
      verifyResultDecryptionDataMasking(conf, new HashSet<>(Arrays.asList(encryptColumns)), decryptKeyMap);
    }
  }

  @Test
  public void testAllColumnsEncDecryption() throws IOException {
    String[] encryptColumns = {"DocId","Name","Gender","Links.Forward","Links.Backward"};
    testSetup("GZIP");
    Map<String, String> encryptColumnToKeyIdMap = new HashMap<>();
    encryptColumnToKeyIdMap.put("DocId", DATA_MASKING_TEST_COL_KEY_ID_1);
    encryptColumnToKeyIdMap.put("Name", DATA_MASKING_TEST_COL_KEY_ID_1);
    encryptColumnToKeyIdMap.put("Gender", DATA_MASKING_TEST_COL_KEY_ID_2);
    encryptColumnToKeyIdMap.put("Links.Forward", DATA_MASKING_TEST_COL_KEY_ID_1);
    encryptColumnToKeyIdMap.put("Links.Backward", DATA_MASKING_TEST_COL_KEY_ID_2);
    columnEncryptor.encryptColumns(inputFile.getFileName(), outputFile, Arrays.asList(encryptColumns),
      DataMaskingEncDecProperties.getFileEncryptionProperties(encryptColumnToKeyIdMap, ParquetCipher.AES_GCM_CTR_V1, false));
    Map<String, byte[]> decryptKeyMap = new HashMap<>();

    // decrypt with correct key
    decryptKeyMap.put(DATA_MASKING_TEST_COL_KEY_ID_1, DATA_MASKING_TEST_COL_KEY_1);
    decryptKeyMap.put(DATA_MASKING_TEST_COL_KEY_ID_2, DATA_MASKING_TEST_COL_KEY_2);
    verifyResultDecryptionDataMasking(conf, new HashSet<>(), decryptKeyMap);

    // decrypt with wrong key
    decryptKeyMap.put(DATA_MASKING_TEST_COL_KEY_ID_1, DATA_MASKING_TEST_COL_KEY_2);
    try {
      verifyResultDecryptionDataMasking(conf, new HashSet<>(), decryptKeyMap);
      fail("Data shouldn't be decrypted!");
    } catch (ParquetDecodingException pde) {
      // decrypt with partial data masking
      conf.set("parquet.data.masking.columns", "DocId,Name,Links.Forward");
      String[] dataMaskingPartialColumns = {"DocId","Name","Links.Forward"};
      verifyResultDecryptionDataMasking(conf,
        new HashSet<>(Arrays.asList(dataMaskingPartialColumns)), decryptKeyMap);

      // decrypt with all columns data masking
      conf.set("parquet.data.masking.columns", "DocId,Name,Links.Forward");
      String[] dataMaskingAllColumns = {"DocId","Name","Links.Forward"};
      verifyResultDecryptionDataMasking(conf, new HashSet<>(Arrays.asList(dataMaskingAllColumns)), decryptKeyMap);

    }
  }

  private void verifyResultDecryptionDataMasking(Configuration conf, Set<String> dataMaskingColumns, Map<String, byte[]> keyMap) throws IOException  {
    ParquetReader<Group> reader = createReader(conf, outputFile, keyMap);
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      if (!dataMaskingColumns.contains("DocId")) {
        assertTrue(group.getLong("DocId", 0) ==
          inputFile.getFileContent()[i].getLong("DocId", 0));
      }

      if (!dataMaskingColumns.contains("Name")) {
        assertArrayEquals(group.getBinary("Name", 0).getBytes(),
          inputFile.getFileContent()[i].getString("Name", 0).getBytes(StandardCharsets.UTF_8));
      }

      if (!dataMaskingColumns.contains("Gender")) {
        assertArrayEquals(group.getBinary("Gender", 0).getBytes(),
          inputFile.getFileContent()[i].getString("Gender", 0).getBytes(StandardCharsets.UTF_8));
      }

      if (dataMaskingColumns.contains("Links.Forward")
        && dataMaskingColumns.contains("Links.Backward")) {
        continue;
      }

      Group subGroupInRead = group.getGroup("Links", 0);
      Group expectedSubGroup = inputFile.getFileContent()[i].getGroup("Links", 0);

      if (!dataMaskingColumns.contains("Links.Forward")) {
        assertArrayEquals(subGroupInRead.getBinary("Forward", 0).getBytes(),
          expectedSubGroup.getBinary("Forward", 0).getBytes());
      }

      if (!dataMaskingColumns.contains("Links.Backward")) {
        assertArrayEquals(subGroupInRead.getBinary("Backward", 0).getBytes(),
          expectedSubGroup.getBinary("Backward", 0).getBytes());
      }
    }
    reader.close();
  }

  private static ParquetReader<Group> createReader(Configuration conf, String path, Map<String, byte[]> keyMap) throws IOException {
    return ParquetReader.builder(new GroupReadSupport(),
      new Path(path)).withConf(conf).withDecryption(getFileDecryptionProperties(keyMap)).build();
  }

  private MessageType createSchema() {
    return new MessageType("schema",
      new PrimitiveType(OPTIONAL, INT64, "DocId"),
      new PrimitiveType(REQUIRED, BINARY, "Name"),
      new PrimitiveType(OPTIONAL, BINARY, "Gender"),
      new GroupType(OPTIONAL, "Links",
        new PrimitiveType(REPEATED, BINARY, "Backward"),
        new PrimitiveType(REPEATED, BINARY, "Forward")));
  }
}
