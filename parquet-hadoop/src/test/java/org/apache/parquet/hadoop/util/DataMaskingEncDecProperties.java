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
package org.apache.parquet.hadoop.util;

import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class DataMaskingEncDecProperties {
  public static final String DATA_MASKING_TEST_FOOTER_KEY_ID = "foot_key";
  public static final byte[] DATA_MASKING_TEST_FOOTER_KEY_METADATA = DATA_MASKING_TEST_FOOTER_KEY_ID.getBytes(StandardCharsets.UTF_8);
  public static final String DATA_MASKING_TEST_COL_KEY_ID_1 = "data_masking_test_col_key_id_1";
  public static final String DATA_MASKING_TEST_COL_KEY_ID_2 = "data_masking_test_col_key_id_2";
  public static final byte[] DATA_MASKING_TEST_FOOTER_KEY = {0x01, 0x02, 0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
    0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10};
  public static final byte[] DATA_MASKING_TEST_COL_KEY_1 = {0x02, 0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
    0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11};
  public static final byte[] DATA_MASKING_TEST_COL_KEY_2 = {0x01, 0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
    0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11};
  private static final Map<String, byte[]> DATA_MASKING_TEST_KEY_MAP = new HashMap<String, byte[]>() {
    {
      put(DATA_MASKING_TEST_COL_KEY_ID_1, DATA_MASKING_TEST_COL_KEY_1);
      put(DATA_MASKING_TEST_COL_KEY_ID_2, DATA_MASKING_TEST_COL_KEY_2);
    }
  };

  public static class DecryptionKeyRetrieverMock implements DecryptionKeyRetriever
  {
    private final Map<String, byte[]> keyMap = new HashMap<>();

    public void putKey(String keyId, byte[] keyBytes) {
      keyMap.put(keyId, keyBytes);
    }

    @Override
    public byte[] getKey(byte[] keyMetaData) {
      String keyId = new String(keyMetaData, StandardCharsets.UTF_8);
      return keyMap.get(keyId);
    }
  }

  public static FileDecryptionProperties getFileDecryptionProperties(Map<String, byte[]> keyMap) {
    DecryptionKeyRetrieverMock keyRetriever = new DecryptionKeyRetrieverMock();
    keyRetriever.putKey(DATA_MASKING_TEST_FOOTER_KEY_ID, DATA_MASKING_TEST_FOOTER_KEY);
    for (Map.Entry<String, byte[]> entry : keyMap.entrySet()) {
      keyRetriever.putKey(entry.getKey(), entry.getValue());
    }
    return FileDecryptionProperties.builder().withPlaintextFilesAllowed().withKeyRetriever(keyRetriever).build();
  }

  public static FileEncryptionProperties getFileEncryptionProperties(
    Map<String, String> encryptColumnToKeyId,
    ParquetCipher cipher,
    Boolean encryptFooter) {
    if (encryptColumnToKeyId.isEmpty()) {
      return null;
    }

    Map<ColumnPath, ColumnEncryptionProperties> columnPropertyMap = new HashMap<>();
    for (Map.Entry<String, String> entry : encryptColumnToKeyId.entrySet()) {
      ColumnPath columnPath = ColumnPath.fromDotString(entry.getKey());
      String keyId = entry.getValue();
      byte[] columnKey = DATA_MASKING_TEST_KEY_MAP.get(keyId);
      byte[] columnKeyMetadata = keyId.getBytes(StandardCharsets.UTF_8);
      ColumnEncryptionProperties columnEncryptionProperties = ColumnEncryptionProperties.builder(columnPath)
        .withKey(columnKey)
        .withKeyMetaData(columnKeyMetadata)
        .build();
      columnPropertyMap.put(columnPath, columnEncryptionProperties);
    }

    FileEncryptionProperties.Builder encryptionPropertiesBuilder =
      FileEncryptionProperties.builder(DATA_MASKING_TEST_FOOTER_KEY)
        .withFooterKeyMetadata(DATA_MASKING_TEST_FOOTER_KEY_METADATA)
        .withAlgorithm(cipher)
        .withEncryptedColumns(columnPropertyMap);

    if (!encryptFooter) {
      encryptionPropertiesBuilder.withPlaintextFooter();
    }

    return encryptionPropertiesBuilder.build();
  }
}
