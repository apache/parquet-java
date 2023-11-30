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

package org.apache.parquet.crypto.propertiesfactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.DecryptionKeyRetrieverMock;
import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.EncryptionPropertiesFactory;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaCryptoPropertiesFactory implements EncryptionPropertiesFactory, DecryptionPropertiesFactory {

  public static final String PATH_NAME_PREFIX = "column_encryption_1178_";

  private static Logger log = LoggerFactory.getLogger(SchemaCryptoPropertiesFactory.class);

  public static final String CONF_ENCRYPTION_ALGORITHM = "parquet.encryption.algorithm";
  public static final String CONF_ENCRYPTION_FOOTER = "parquet.encrypt.footer";
  private static final byte[] FOOTER_KEY = {
    0x01, 0x02, 0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10
  };
  private static final byte[] FOOTER_KEY_METADATA = "footkey".getBytes(StandardCharsets.UTF_8);
  private static final byte[] COL_KEY = {
    0x02, 0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11
  };
  private static final byte[] COL_KEY_METADATA = "col".getBytes(StandardCharsets.UTF_8);

  @Override
  public FileEncryptionProperties getFileEncryptionProperties(
      Configuration conf, Path tempFilePath, WriteContext fileWriteContext) throws ParquetCryptoRuntimeException {
    MessageType schema = fileWriteContext.getSchema();
    List<String[]> paths = schema.getPaths();
    if (paths == null || paths.isEmpty()) {
      throw new ParquetCryptoRuntimeException("Null or empty fields is found");
    }

    Map<ColumnPath, ColumnEncryptionProperties> columnPropertyMap = new HashMap<>();

    for (String[] path : paths) {
      getColumnEncryptionProperties(path, columnPropertyMap, conf);
    }

    if (columnPropertyMap.isEmpty()) {
      log.debug(
          "No column is encrypted. Returning null so that Parquet can skip. Empty properties will cause Parquet exception");
      return null;
    }

    /**
     * Why we still need footerKeyMetadata even withEncryptedFooter as false? According to the
     * 'Plaintext Footer' section of
     * https://github.com/apache/parquet-format/blob/encryption/Encryption.md, the plaintext footer
     * is signed in order to prevent tampering with the FileMetaData contents. So footerKeyMetadata
     * is always needed. This signature will be verified if parquet-mr code is with parquet-1178.
     * Otherwise, it will be ignored.
     */
    boolean shouldEncryptFooter = getEncryptFooter(conf);
    FileEncryptionProperties.Builder encryptionPropertiesBuilder = FileEncryptionProperties.builder(FOOTER_KEY)
        .withFooterKeyMetadata(FOOTER_KEY_METADATA)
        .withAlgorithm(getParquetCipherOrDefault(conf))
        .withEncryptedColumns(columnPropertyMap);
    if (!shouldEncryptFooter) {
      encryptionPropertiesBuilder = encryptionPropertiesBuilder.withPlaintextFooter();
    }
    FileEncryptionProperties encryptionProperties = encryptionPropertiesBuilder.build();
    log.info(
        "FileEncryptionProperties is built with, algorithm:{}, footerEncrypted:{}",
        encryptionProperties.getAlgorithm(),
        encryptionProperties.encryptedFooter());
    return encryptionProperties;
  }

  private ParquetCipher getParquetCipherOrDefault(Configuration conf) {
    String algorithm = conf.get(CONF_ENCRYPTION_ALGORITHM, "AES_GCM_CTR_V1");
    log.debug("Encryption algorithm is {}", algorithm);
    return ParquetCipher.valueOf(algorithm.toUpperCase());
  }

  private boolean getEncryptFooter(Configuration conf) {
    boolean encryptFooter = conf.getBoolean(CONF_ENCRYPTION_FOOTER, false);
    log.debug("Encrypt Footer: {}", encryptFooter);
    return encryptFooter;
  }

  private void getColumnEncryptionProperties(
      String[] path, Map<ColumnPath, ColumnEncryptionProperties> columnPropertyMap, Configuration conf)
      throws ParquetCryptoRuntimeException {
    String pathName = String.join(".", path);
    String columnKeyName = conf.get(PATH_NAME_PREFIX + pathName, null);
    if (columnKeyName != null) {
      ColumnPath columnPath = ColumnPath.get(path);
      ColumnEncryptionProperties colEncProp = ColumnEncryptionProperties.builder(columnPath)
          .withKey(COL_KEY)
          .withKeyMetaData(COL_KEY_METADATA)
          .build();
      columnPropertyMap.put(columnPath, colEncProp);
    }
  }

  @Override
  public FileDecryptionProperties getFileDecryptionProperties(Configuration hadoopConfig, Path filePath)
      throws ParquetCryptoRuntimeException {
    DecryptionKeyRetrieverMock keyRetriever = new DecryptionKeyRetrieverMock();
    keyRetriever.putKey("footkey", FOOTER_KEY);
    keyRetriever.putKey("col", COL_KEY);
    return FileDecryptionProperties.builder()
        .withPlaintextFilesAllowed()
        .withKeyRetriever(keyRetriever)
        .build();
  }
}
