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

package org.apache.parquet.crypto;


import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.FileCryptoMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.format.EncryptionAlgorithm;

import java.io.IOException;
import java.util.HashMap;


public class InternalFileEncryptor {

  private final EncryptionAlgorithm algorithm;
  private final FileEncryptionProperties fileEncryptionProperties;
  private final byte[] footerEncryptionKey;
  private final byte[] footerEncryptionKeyMetadata;
  private final byte[] footerSigningKey;
  private final byte[] footerSigningKeyMetadata;
  private final HashMap<ColumnPath, InternalColumnEncryptionSetup> columnMap;
  private final byte[] fileAAD;
  private final boolean encryptFooter;
  
  private BlockCipher.Encryptor aesGcmEncryptorWithFooterKey;
  private BlockCipher.Encryptor aesCtrEncryptorWithFooterKey;
  private boolean fileCryptoMetaDataCreated;

  public InternalFileEncryptor(FileEncryptionProperties fileEncryptionProperties) {
    algorithm = fileEncryptionProperties.getAlgorithm();
    footerEncryptionKey = fileEncryptionProperties.getFooterEncryptionKey();
    encryptFooter =  fileEncryptionProperties.encryptedFooter();
    footerEncryptionKeyMetadata = fileEncryptionProperties.getFooterEncryptionKeyMetadata();
    footerSigningKey = fileEncryptionProperties.getFooterSigningKey();
    footerSigningKeyMetadata = fileEncryptionProperties.getFooterSigningKeyMetadata();
    fileAAD = fileEncryptionProperties.getFileAAD();
    this.fileEncryptionProperties = fileEncryptionProperties;
    columnMap = new HashMap<ColumnPath, InternalColumnEncryptionSetup>();
    fileCryptoMetaDataCreated = false;
  }
  

  private BlockCipher.Encryptor getThriftModuleEncryptor(byte[] columnKey) throws IOException {
    if (null == columnKey) { // Encryptor with footer key
      if (null == aesGcmEncryptorWithFooterKey) {
        aesGcmEncryptorWithFooterKey = new AesEncryptor(AesEncryptor.Mode.GCM, footerEncryptionKey);
      }
      return aesGcmEncryptorWithFooterKey;
    }
    else { // Encryptor with column key
      return new AesEncryptor(AesEncryptor.Mode.GCM, columnKey);
    }
  }
  
  private BlockCipher.Encryptor getDataModuleEncryptor(byte[] columnKey) throws IOException {
    if (algorithm.isSetAES_GCM_V1()) {
      return getThriftModuleEncryptor(columnKey);
    }
    // AES_GCM_CTR_V1
    if (null == columnKey) { // Encryptor with footer key
      if (null == aesCtrEncryptorWithFooterKey) {
        aesCtrEncryptorWithFooterKey = new AesEncryptor(AesEncryptor.Mode.CTR, footerEncryptionKey);
      }
      return aesCtrEncryptorWithFooterKey;
    }
    else { // Encryptor with column key
      return new AesEncryptor(AesEncryptor.Mode.CTR, columnKey);
    }
  }
    
  /**
   * 
   * @param columnPath
   * @param createIfNull Use true for pages, false for metadata. 
   * @return
   * @throws IOException
   */
  public InternalColumnEncryptionSetup getColumnSetup(ColumnPath columnPath, boolean createIfNull, short ordinal) throws IOException {
    InternalColumnEncryptionSetup internalColumnProperties = columnMap.get(columnPath);
    if (null != internalColumnProperties) {
      if (ordinal != internalColumnProperties.getOrdinal()) {
        throw new IOException("Column ordinal doesnt match " + columnPath + ": " + ordinal + ", "+internalColumnProperties.getOrdinal());
      }
      return internalColumnProperties;
    }
    if (!createIfNull) {
      throw new IOException("No encryption setup found for column " + columnPath);
    }
    if (fileCryptoMetaDataCreated) {
      throw new IOException("Re-use: No encryption setup for column " + columnPath);
    }

    ColumnEncryptionProperties columnProperties = fileEncryptionProperties.getColumnProperties(columnPath);
    if (null == columnProperties) {
      throw new IOException("No encryption properties for column " + columnPath);
    }
    if (columnProperties.isEncrypted()) {
      if (columnProperties.isEncryptedWithFooterKey()) {
        internalColumnProperties = new InternalColumnEncryptionSetup(columnProperties, ordinal,
            getDataModuleEncryptor(null), getThriftModuleEncryptor(null));
      }
      else {
        internalColumnProperties = new InternalColumnEncryptionSetup(columnProperties, ordinal,
            getDataModuleEncryptor(columnProperties.getKeyBytes()), getThriftModuleEncryptor(columnProperties.getKeyBytes()));
      }
    }
    else {
      // unencrypted column
      internalColumnProperties = new InternalColumnEncryptionSetup(columnProperties, ordinal, null, null);
    }
    columnMap.put(columnPath, internalColumnProperties);
    return internalColumnProperties;
  }

  public BlockCipher.Encryptor getFooterEncryptor() throws IOException  {
    if (!encryptFooter) return null;
    return getThriftModuleEncryptor(null);
  }

  public FileCryptoMetaData getFileCryptoMetaData() throws IOException {
    if (!encryptFooter) throw new IOException("Requesting FileCryptoMetaData in file with unencrypted footer");
    FileCryptoMetaData fileCryptoMetaData = new FileCryptoMetaData(algorithm);
    if (null != footerEncryptionKeyMetadata) fileCryptoMetaData.setKey_metadata(footerEncryptionKeyMetadata);
    fileCryptoMetaDataCreated = true;
    return fileCryptoMetaData;
  }

  public boolean encryptColumnMetaData(InternalColumnEncryptionSetup columnSetup) {
    if (!columnSetup.isEncrypted()) return false;
    if (!encryptFooter) return true;
    return !columnSetup.isEncryptedWithFooterKey();
  }


  public boolean isFooterEncrypted() {
    return encryptFooter;
  }


  public EncryptionAlgorithm getEncryptionAlgorithm() {
    return algorithm;
  }
  
  public byte[] getFileAAD() {
    return this.fileAAD;
  }


  public byte[] getFooterSigningKeyMetaData()  throws IOException {
    if (encryptFooter) throw new IOException("Requesting signing footer key metadata in file with encrypted footer");
    return footerSigningKeyMetadata;
  }


  public BlockCipher.Encryptor getSignedFooterEncryptor() throws IOException  {
    if (encryptFooter) throw new IOException("Requesting signed footer encryptor in file with encrypted footer");
    return new AesEncryptor(AesEncryptor.Mode.GCM, footerSigningKey);
  }
}
