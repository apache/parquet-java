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
  private final byte[] footerKeyBytes;
  private final FileEncryptionProperties fileEncryptionProperties;
  private final byte[] footerKeyMetaDataBytes;
  private final HashMap<ColumnPath, InternalColumnEncryptionSetup> columnMap;
  private final byte[] aadBytes;
  private final boolean encryptFooter;
  
  private BlockCipher.Encryptor aesGcmEncryptorWithFooterKey;
  private BlockCipher.Encryptor aesCtrEncryptorWithFooterKey;
  private boolean fileCryptoMetaDataCreated;

  public InternalFileEncryptor(FileEncryptionProperties fileEncryptionProperties) {
    algorithm = fileEncryptionProperties.getAlgorithm();
    footerKeyBytes = fileEncryptionProperties.getFooterKeyBytes();
    encryptFooter =  (null != footerKeyBytes);
    footerKeyMetaDataBytes = fileEncryptionProperties.getFooterKeyMetaData();
    columnMap = new HashMap<ColumnPath, InternalColumnEncryptionSetup>();
    aadBytes = fileEncryptionProperties.getAAD();
    this.fileEncryptionProperties = fileEncryptionProperties;
    fileCryptoMetaDataCreated = false;
  }
  

  private BlockCipher.Encryptor getMetaDataEncryptor(byte[] columnKey) throws IOException {
    if (null == columnKey) { // Encryptor with footer key
      if (null == aesGcmEncryptorWithFooterKey) {
        aesGcmEncryptorWithFooterKey = new AesEncryptor(AesEncryptor.Mode.GCM, footerKeyBytes, aadBytes);
      }
      return aesGcmEncryptorWithFooterKey;
    }
    else {
      return new AesEncryptor(AesEncryptor.Mode.GCM, columnKey, aadBytes);
    }
  }
  
  private BlockCipher.Encryptor getDataEncryptor(byte[] columnKey) throws IOException {
    if (algorithm.isSetAES_GCM_V1()) {
      return getMetaDataEncryptor(columnKey);
    }
    // AES_GCM_CTR_V1
    if (null == columnKey) { // Encryptor with footer key
      if (null == aesCtrEncryptorWithFooterKey) {
        aesCtrEncryptorWithFooterKey = new AesEncryptor(AesEncryptor.Mode.CTR, footerKeyBytes, null);
      }
      return aesCtrEncryptorWithFooterKey;
    }
    else {
      return new AesEncryptor(AesEncryptor.Mode.CTR, columnKey, null);
    }
  }
    
  public InternalColumnEncryptionSetup getColumnSetup(ColumnPath columnPath, boolean createIfNull) throws IOException {
    InternalColumnEncryptionSetup internalColumnProperties = columnMap.get(columnPath);
    if (null != internalColumnProperties) return internalColumnProperties;
    if (!createIfNull) {
      throw new IOException("No encryption setup found for column " + columnPath);
    }
    if (fileCryptoMetaDataCreated) throw new IOException("Re-use: No encryption setup for column " + columnPath);

    ColumnEncryptionProperties columnProperties = fileEncryptionProperties.getColumnProperties(columnPath);
    if (null == columnProperties) {
      throw new IOException("No encryption properties for column " + columnPath);
    }
    if (columnProperties.isEncrypted()) {
      if (columnProperties.isEncryptedWithFooterKey()) {
        internalColumnProperties = new InternalColumnEncryptionSetup(columnProperties, 
            getDataEncryptor(null), getMetaDataEncryptor(null));
      }
      else {
        internalColumnProperties = new InternalColumnEncryptionSetup(columnProperties, 
            getDataEncryptor(columnProperties.getKeyBytes()), getMetaDataEncryptor(columnProperties.getKeyBytes()));
      }
    }
    else {
      // unencrypted column
      internalColumnProperties = new InternalColumnEncryptionSetup(columnProperties, null, null);
    }
    columnMap.put(columnPath, internalColumnProperties);
    return internalColumnProperties;
  }

  public BlockCipher.Encryptor getFooterEncryptor() throws IOException  {
    if (!encryptFooter) return null;
    return getMetaDataEncryptor(null);
  }

  public FileCryptoMetaData getFileCryptoMetaData(long footerIndex) throws IOException {
    if (!encryptFooter) throw new IOException("Requesting FileCryptoMetaData in file with unencrypted footer");
    FileCryptoMetaData fileCryptoMetaData = new FileCryptoMetaData(algorithm, footerIndex);
    if (null != footerKeyMetaDataBytes) fileCryptoMetaData.setFooter_key_metadata(footerKeyMetaDataBytes);
    fileCryptoMetaDataCreated = true;
    return fileCryptoMetaData;
  }

  public boolean splitColumnMetaData(InternalColumnEncryptionSetup columnSetup) {
    if (!columnSetup.getColumnEncryptionProperties().isEncrypted()) return false;
    if (!encryptFooter) return true;
    return !columnSetup.getColumnEncryptionProperties().isEncryptedWithFooterKey();
  }


  public boolean isFooterEncrypted() {
    return encryptFooter;
  }


  public EncryptionAlgorithm getEncryptionAlgorithm() {
    return algorithm;
  }
}
