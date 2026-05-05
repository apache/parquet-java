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

package org.apache.parquet.crypto.keytools;

import static org.apache.parquet.crypto.keytools.KeyToolkit.stringIsEmpty;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.EncryptionPropertiesFactory;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesDrivenCryptoFactory implements EncryptionPropertiesFactory, DecryptionPropertiesFactory {
  private static final Logger LOG = LoggerFactory.getLogger(PropertiesDrivenCryptoFactory.class);

  private static final int[] ACCEPTABLE_DATA_KEY_LENGTHS = {128, 192, 256};

  /**
   * List of columns to encrypt, with master key IDs (see HIVE-21848).
   * Format: "masterKeyID:colName,colName;masterKeyID:colName...".
   * Unlisted columns are not encrypted.
   */
  public static final String COLUMN_KEYS_PROPERTY_NAME = "parquet.encryption.column.keys";
  /**
   * Master key ID for footer encryption/signing.
   */
  public static final String FOOTER_KEY_PROPERTY_NAME = "parquet.encryption.footer.key";
  /**
   * Encrypt unlisted columns using footer key.
   * By default, false - unlisted columns are not encrypted.
   */
  public static final String COMPLETE_COLUMN_ENCRYPTION_PROPERTY_NAME = "parquet.encryption.complete.columns";
  /**
   * Master key ID for uniform encryption (same key for all columns and footer).
   */
  public static final String UNIFORM_KEY_PROPERTY_NAME = "parquet.encryption.uniform.key";
  /**
   * Parquet encryption algorithm. Can be "AES_GCM_V1" (default), or "AES_GCM_CTR_V1".
   */
  public static final String ENCRYPTION_ALGORITHM_PROPERTY_NAME = "parquet.encryption.algorithm";
  /**
   * Write files with plaintext footer.
   * By default, false - Parquet footers are encrypted.
   */
  public static final String PLAINTEXT_FOOTER_PROPERTY_NAME = "parquet.encryption.plaintext.footer";

  public static final String ENCRYPTION_ALGORITHM_DEFAULT = ParquetCipher.AES_GCM_V1.toString();
  public static final boolean PLAINTEXT_FOOTER_DEFAULT = false;
  public static final boolean COMPLETE_COLUMN_ENCRYPTION_DEFAULT = false;

  private static final SecureRandom RANDOM = new SecureRandom();

  @Override
  public FileEncryptionProperties getFileEncryptionProperties(
      Configuration fileHadoopConfig, Path tempFilePath, WriteContext fileWriteContext)
      throws ParquetCryptoRuntimeException {

    String footerKeyId = fileHadoopConfig.getTrimmed(FOOTER_KEY_PROPERTY_NAME);
    String columnKeysStr = fileHadoopConfig.getTrimmed(COLUMN_KEYS_PROPERTY_NAME);
    String uniformKeyId = fileHadoopConfig.getTrimmed(UNIFORM_KEY_PROPERTY_NAME);
    boolean completeColumnEncryption = fileHadoopConfig.getBoolean(
        COMPLETE_COLUMN_ENCRYPTION_PROPERTY_NAME, COMPLETE_COLUMN_ENCRYPTION_DEFAULT);

    boolean emptyFooterKeyId = stringIsEmpty(footerKeyId);
    boolean emptyColumnKeyIds = stringIsEmpty(columnKeysStr);
    boolean emptyUniformKeyId = stringIsEmpty(uniformKeyId);

    // File shouldn't be encrypted
    if (emptyFooterKeyId && emptyColumnKeyIds && emptyUniformKeyId) {
      LOG.debug("Unencrypted file: {}", tempFilePath);
      return null;
    }

    if (emptyUniformKeyId) {
      // Non-uniform encryption.Must have both footer and column key ids
      if (emptyFooterKeyId) {
        throw new ParquetCryptoRuntimeException("No footer key configured in " + FOOTER_KEY_PROPERTY_NAME);
      }
      if (emptyColumnKeyIds) {
        throw new ParquetCryptoRuntimeException("No column keys configured in " + COLUMN_KEYS_PROPERTY_NAME);
      }
    } else {
      // Uniform encryption. Can't have configuration of footer and column key ids
      if (!emptyFooterKeyId) {
        throw new ParquetCryptoRuntimeException(
            "Uniform encryption. Cant have footer key configured in " + FOOTER_KEY_PROPERTY_NAME);
      }
      if (!emptyColumnKeyIds) {
        throw new ParquetCryptoRuntimeException(
            "Uniform encryption. Cant have column keys configured in " + COLUMN_KEYS_PROPERTY_NAME);
      }
      if (completeColumnEncryption) {
        throw new ParquetCryptoRuntimeException(
            "Complete column encryption cant be applied in uniform encryption mode");
      }

      // Now assign footer key id to uniform key id
      footerKeyId = uniformKeyId;
    }

    FileKeyMaterialStore keyMaterialStore = null;
    boolean keyMaterialInternalStorage = fileHadoopConfig.getBoolean(
        KeyToolkit.KEY_MATERIAL_INTERNAL_PROPERTY_NAME, KeyToolkit.KEY_MATERIAL_INTERNAL_DEFAULT);
    if (!keyMaterialInternalStorage) {
      if (tempFilePath == null) {
        throw new ParquetCryptoRuntimeException("Output file path cannot be null");
      }
      try {
        keyMaterialStore = new HadoopFSKeyMaterialStore(tempFilePath.getFileSystem(fileHadoopConfig));
        keyMaterialStore.initialize(tempFilePath, fileHadoopConfig, false);
      } catch (IOException e) {
        throw new ParquetCryptoRuntimeException("Failed to get key material store", e);
      }
    }

    FileKeyWrapper keyWrapper = new FileKeyWrapper(fileHadoopConfig, keyMaterialStore);

    String algo = fileHadoopConfig.getTrimmed(ENCRYPTION_ALGORITHM_PROPERTY_NAME, ENCRYPTION_ALGORITHM_DEFAULT);
    ParquetCipher cipher;
    try {
      cipher = ParquetCipher.valueOf(algo);
    } catch (IllegalArgumentException e) {
      throw new ParquetCryptoRuntimeException("Wrong encryption algorithm: " + algo);
    }

    int dekLengthBits =
        fileHadoopConfig.getInt(KeyToolkit.DATA_KEY_LENGTH_PROPERTY_NAME, KeyToolkit.DATA_KEY_LENGTH_DEFAULT);

    if (Arrays.binarySearch(ACCEPTABLE_DATA_KEY_LENGTHS, dekLengthBits) < 0) {
      throw new ParquetCryptoRuntimeException("Wrong data key length : " + dekLengthBits);
    }

    int dekLength = dekLengthBits / 8;

    byte[] footerKeyBytes = new byte[dekLength];
    RANDOM.nextBytes(footerKeyBytes);
    byte[] footerKeyMetadata = keyWrapper.getEncryptionKeyMetadata(footerKeyBytes, footerKeyId, true);

    boolean plaintextFooter = fileHadoopConfig.getBoolean(PLAINTEXT_FOOTER_PROPERTY_NAME, PLAINTEXT_FOOTER_DEFAULT);

    FileEncryptionProperties.Builder propertiesBuilder = FileEncryptionProperties.builder(footerKeyBytes)
        .withFooterKeyMetadata(footerKeyMetadata)
        .withAlgorithm(cipher);

    if (emptyUniformKeyId) {
      Map<ColumnPath, ColumnEncryptionProperties> encryptedColumns =
          getColumnEncryptionProperties(dekLength, columnKeysStr, keyWrapper);
      propertiesBuilder = propertiesBuilder.withEncryptedColumns(encryptedColumns);

      if (completeColumnEncryption) {
        propertiesBuilder = propertiesBuilder.withCompleteColumnEncryption();
      }
    }

    if (plaintextFooter) {
      propertiesBuilder = propertiesBuilder.withPlaintextFooter();
    }

    if (null != keyMaterialStore) {
      keyMaterialStore.saveMaterial();
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "File encryption properties for {} - algo: {}; footer key id: {}; uniform key id: {}; " + ""
              + "plaintext footer: {}; internal key material: {}; encrypted columns: {}",
          tempFilePath,
          cipher,
          footerKeyId,
          uniformKeyId,
          plaintextFooter,
          keyMaterialInternalStorage,
          columnKeysStr);
    }

    return propertiesBuilder.build();
  }

  private Map<ColumnPath, ColumnEncryptionProperties> getColumnEncryptionProperties(
      int dekLength, String columnKeys, FileKeyWrapper keyWrapper) throws ParquetCryptoRuntimeException {
    Map<ColumnPath, ColumnEncryptionProperties> encryptedColumns =
        new HashMap<ColumnPath, ColumnEncryptionProperties>();
    String keyToColumns[] = columnKeys.split(";");
    for (int i = 0; i < keyToColumns.length; ++i) {
      final String curKeyToColumns = keyToColumns[i].trim();
      if (curKeyToColumns.isEmpty()) {
        continue;
      }

      String[] parts = curKeyToColumns.split(":");
      if (parts.length != 2) {
        throw new ParquetCryptoRuntimeException("Incorrect key to columns mapping in "
            + COLUMN_KEYS_PROPERTY_NAME + ": [" + curKeyToColumns + "]");
      }

      String columnKeyId = parts[0].trim();
      if (columnKeyId.isEmpty()) {
        throw new ParquetCryptoRuntimeException("Empty key name in " + COLUMN_KEYS_PROPERTY_NAME);
      }

      String columnNamesStr = parts[1].trim();
      String[] columnNames = columnNamesStr.split(",");
      if (0 == columnNames.length) {
        throw new ParquetCryptoRuntimeException("No columns to encrypt defined for key: " + columnKeyId);
      }

      for (int j = 0; j < columnNames.length; ++j) {
        final String columnName = columnNames[j].trim();
        if (columnName.isEmpty()) {
          throw new ParquetCryptoRuntimeException(
              "Empty column name in " + COLUMN_KEYS_PROPERTY_NAME + " for key: " + columnKeyId);
        }

        final ColumnPath columnPath = ColumnPath.fromDotString(columnName);
        if (encryptedColumns.containsKey(columnPath)) {
          throw new ParquetCryptoRuntimeException("Multiple keys defined for the same column: " + columnName);
        }

        byte[] columnKeyBytes = new byte[dekLength];
        RANDOM.nextBytes(columnKeyBytes);
        byte[] columnKeyKeyMetadata = keyWrapper.getEncryptionKeyMetadata(columnKeyBytes, columnKeyId, false);

        ColumnEncryptionProperties cmd = ColumnEncryptionProperties.builder(columnPath)
            .withKey(columnKeyBytes)
            .withKeyMetaData(columnKeyKeyMetadata)
            .build();
        encryptedColumns.put(columnPath, cmd);
      }
    }
    if (encryptedColumns.isEmpty()) {
      throw new ParquetCryptoRuntimeException("No column keys configured in " + COLUMN_KEYS_PROPERTY_NAME);
    }

    return encryptedColumns;
  }

  @Override
  public FileDecryptionProperties getFileDecryptionProperties(Configuration hadoopConfig, Path filePath)
      throws ParquetCryptoRuntimeException {

    DecryptionKeyRetriever keyRetriever = new FileKeyUnwrapper(hadoopConfig, filePath);

    if (LOG.isDebugEnabled()) {
      LOG.debug("File decryption properties for {}", filePath);
    }

    return FileDecryptionProperties.builder()
        .withKeyRetriever(keyRetriever)
        .withPlaintextFilesAllowed()
        .build();
  }
}
