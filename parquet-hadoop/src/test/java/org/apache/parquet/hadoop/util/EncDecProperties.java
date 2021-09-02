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

import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EncDecProperties {

  public static class DecryptionKeyRetrieverMock implements DecryptionKeyRetriever {
    private final Map<String, byte[]> keyMap = new HashMap<>();

    public DecryptionKeyRetrieverMock putKey(String keyId, byte[] keyBytes) {
      keyMap.put(keyId, keyBytes);
      return this;
    }

    @Override
    public byte[] getKey(byte[] keyMetaData) {
      String keyId = new String(keyMetaData, StandardCharsets.UTF_8);
      return keyMap.get(keyId);
    }
  }

  private static final byte[] FOOTER_KEY = {0x01, 0x02, 0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
    0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10};
  private static final byte[] FOOTER_KEY_METADATA = "footkey".getBytes(StandardCharsets.UTF_8);
  private static final byte[] COL_KEY = {0x02, 0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
    0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11};
  private static final byte[] COL_KEY_METADATA = "col".getBytes(StandardCharsets.UTF_8);

  public static FileDecryptionProperties getFileDecryptionProperties() throws IOException {
    DecryptionKeyRetrieverMock keyRetriever = new DecryptionKeyRetrieverMock();
    keyRetriever.putKey("footkey", FOOTER_KEY);
    keyRetriever.putKey("col", COL_KEY);
    return FileDecryptionProperties.builder().withPlaintextFilesAllowed().withoutFooterSignatureVerification().withKeyRetriever(keyRetriever).build();
  }

  public static FileDecryptionProperties getFileDecryptionPropertiesNonExistKey() throws IOException {
    DecryptionKeyRetrieverMock keyRetriever = new DecryptionKeyRetrieverMock();
    keyRetriever.putKey("footkey", FOOTER_KEY);
    keyRetriever.putKey("col_non_exist", COL_KEY);
    return FileDecryptionProperties.builder().withPlaintextFilesAllowed().withoutFooterSignatureVerification().withKeyRetriever(keyRetriever).build();
  }

  public static FileEncryptionProperties getFileEncryptionProperties(String[] encrCols, ColumnEncryptor.EncryptMode mode) {
    if (encrCols.length == 0) {
      return null;
    }

    Map<ColumnPath, ColumnEncryptionProperties> columnPropertyMap = new HashMap<>();
    for (String encrCol : encrCols) {
      ColumnPath columnPath = ColumnPath.fromDotString(encrCol);
      ColumnEncryptionProperties colEncProp = ColumnEncryptionProperties.builder(columnPath)
        .withKey(COL_KEY)
        .withKeyMetaData(COL_KEY_METADATA)
        .build();
      columnPropertyMap.put(columnPath, colEncProp);
    }

    FileEncryptionProperties.Builder encryptionPropertiesBuilder =
      FileEncryptionProperties.builder(FOOTER_KEY)
        .withFooterKeyMetadata(FOOTER_KEY_METADATA)
        .withAlgorithm(ParquetCipher.valueOf(mode.getMode()))
        .withEncryptedColumns(columnPropertyMap)
        .withPlaintextFooter();

    return encryptionPropertiesBuilder.build();
  }
}
