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
import org.apache.parquet.format.ColumnCryptoMetaData;
import org.apache.parquet.format.FileCryptoMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.parquet.format.EncryptionAlgorithm;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import javax.crypto.Cipher;

public class ParquetFileEncryptor {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetFileEncryptor.class);

  private EncryptionAlgorithm algorithm;
  private byte[] footerKeyBytes;
  private BlockCipher.Encryptor aesGcmBlockEncryptor;
  private BlockCipher.Encryptor aesCtrBlockEncryptor;
  private ColumnEncryptors footerKeyEncryptors;
  private EncryptionSetup encryptionSetup;
  private byte[] footerKeyMetaDataBytes = null;
  //Uniform encryption means footer and all columns are encrypted, with same key
  private boolean uniformEncryption;
  private List<ColumnMetadata> columnMDList; // TODO replace with Map
  private byte[] aadBytes;
  private boolean encryptFooter;
  private boolean fileCryptoMDSet;

  ParquetFileEncryptor(EncryptionSetup eSetup) throws IOException {
    algorithm = eSetup.getAlgorithm();
    if (null == algorithm) throw new IOException("Null algorithm");
    uniformEncryption = eSetup.isUniformEncryption();
    footerKeyBytes = eSetup.getFooterKeyBytes();
    if (null ==  footerKeyBytes) {
      if (uniformEncryption) throw new IOException("Null key in uniform encryption");
      encryptFooter = false;
    }
    else {
      encryptFooter = true;
    }
    footerKeyMetaDataBytes = eSetup.getFooterKeyMetadata();
    if (!uniformEncryption) columnMDList = new LinkedList<ColumnMetadata>();
    try {
     LOG.info("AES-GCM cipher provider: {}", Cipher.getInstance("AES/GCM/NoPadding").getProvider());
     if (algorithm.isSetAES_GCM_CTR_V1()) {
       LOG.info("AES-CTR cipher provider: {}", Cipher.getInstance("AES/CTR/NoPadding").getProvider());
     }
    } catch (GeneralSecurityException e) {
      throw new IOException("Failed to get cipher", e);
    }
    aadBytes = eSetup.getAAD();
    encryptionSetup = eSetup;
    fileCryptoMDSet = false;
    LOG.info("File encryptor. Uniform encryption: {}. Key metadata set: {}", 
        uniformEncryption, (null != footerKeyMetaDataBytes));
  }
  
  private ColumnEncryptors getFooterKeyEncryptors() throws IOException {
    if (null != footerKeyEncryptors) return footerKeyEncryptors;
    footerKeyEncryptors = new ColumnEncryptors();
    if (null == aesGcmBlockEncryptor) aesGcmBlockEncryptor = new AesGcmEncryptor(footerKeyBytes, aadBytes);
    footerKeyEncryptors.metadataEncryptor = aesGcmBlockEncryptor;
    if (algorithm.isSetAES_GCM_CTR_V1()) {
      if (null == aesCtrBlockEncryptor) aesCtrBlockEncryptor = new AesCtrEncryptor(footerKeyBytes);
      footerKeyEncryptors.dataEncryptor = aesCtrBlockEncryptor;
    }
    else {
      footerKeyEncryptors.dataEncryptor = aesGcmBlockEncryptor;
    }
    return footerKeyEncryptors;
  }

  public ColumnEncryptors getColumnEncryptors(ColumnMetadata cmd) throws IOException {
    if (cmd.getEncryptors() != null) return cmd.getEncryptors();
    return getColumnEncryptors(cmd.getPath());
  }
    
  // Returns two encryptors - for page headers, and for page contents (can be the same)
  public synchronized ColumnEncryptors getColumnEncryptors(String[] columnPath) throws IOException {
    if (uniformEncryption) return getFooterKeyEncryptors();
    
    // Non-uniform encryption
    ColumnMetadata cmd = findColumn(columnPath);
    if (null != cmd) {
      if (!cmd.isEncrypted()) return null;
      return cmd.getEncryptors();
    }
    if (fileCryptoMDSet) throw new IOException("Re-use: No encryption metadata for column " + Arrays.toString(columnPath));
    cmd = encryptionSetup.getColumnMetadata(columnPath);
    if (null == cmd) {
      throw new IOException("No encryption metadata for column " + Arrays.toString(columnPath));
    }
    columnMDList.add(cmd);
    if (!cmd.isEncrypted()) return null;
    if (cmd.isEncryptedWithFooterKey()) {
      cmd.setEncryptors(getFooterKeyEncryptors());
    }
    else {
      ColumnEncryptors encryptors = new ColumnEncryptors();
      byte[] column_key_bytes =  cmd.getKeyBytes();
      if (null == column_key_bytes) throw new IOException("Null key in encrypted column " + Arrays.toString(columnPath));
      encryptors.metadataEncryptor = new AesGcmEncryptor(column_key_bytes, aadBytes);
      if (algorithm.isSetAES_GCM_CTR_V1()) {
        encryptors.dataEncryptor = new AesCtrEncryptor(column_key_bytes);
      }
      else {
        encryptors.dataEncryptor = encryptors.metadataEncryptor;
      }
      cmd.setEncryptors(encryptors);
    }
    return cmd.getEncryptors();
  }

  public synchronized BlockCipher.Encryptor getFooterEncryptor() throws IOException  {
    if (!encryptFooter) return null;
    if (null == aesGcmBlockEncryptor) aesGcmBlockEncryptor = new AesGcmEncryptor(footerKeyBytes, aadBytes);
    return aesGcmBlockEncryptor;
  }

  public synchronized FileCryptoMetaData getFileCryptoMetaData(long footer_index) throws IOException {
    FileCryptoMetaData fcmd = new FileCryptoMetaData(algorithm, encryptFooter, footer_index);
    if (null != footerKeyMetaDataBytes) fcmd.setFooter_key_metadata(footerKeyMetaDataBytes);
    fileCryptoMDSet = true;
    return fcmd;
  }

  public boolean isUniformEncryption() {
    return uniformEncryption;
  }

  public ColumnMetadata getColumnMetaData(String[] path) throws IOException {
    ColumnMetadata cmd = findColumn(path);
    if (null == cmd) throw new IOException("No encryption metadata for column " + Arrays.toString(path));
    return cmd;
  }

  public boolean splitColumnMetaData(ColumnMetadata cmd) {
    if (!cmd.isEncrypted()) return false;
    if (!encryptFooter) return true;
    return !cmd.isEncryptedWithFooterKey();
  }
  
  // Find column in a list
  // TODO replace with a Map lookup
  private ColumnMetadata findColumn(String[] path) {
    for (ColumnMetadata cmd: columnMDList) {
      if (Arrays.deepEquals(path, cmd.getPath())) {
        return cmd;
      }
    }
    return null;
  }

  public ColumnCryptoMetaData getColumnCryptoMetaData(ColumnMetadata cmd) {
    return cmd.getColumnCryptoMetaData();
  }
}
