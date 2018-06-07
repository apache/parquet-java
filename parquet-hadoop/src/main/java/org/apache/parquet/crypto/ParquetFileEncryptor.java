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


import org.apache.parquet.format.BlockCrypto;
import org.apache.parquet.format.FileCryptoMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.parquet.format.ColumnCryptoMetaData;
import org.apache.parquet.format.EncryptionAlgorithm;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import javax.crypto.Cipher;

public class ParquetFileEncryptor {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetFileEncryptor.class);

  private EncryptionAlgorithm algorithmId;
  private byte[] footerKeyBytes;
  private BlockCrypto.Encryptor aesGcmBlockEncryptor;
  private BlockCrypto.Encryptor aesCtrBlockEncryptor;
  private EncryptionSetup encryptionSetup;
  private byte[] footerKeyMetaDataBytes = null;
  //Uniform encryption means footer and all columns are encrypted, with same key
  private boolean uniformEncryption;
  private List<ColumnCryptoMetaData> columnMDList;
  private boolean columnMDListSet;
  private byte[] aadBytes;
  
  private boolean encryptFooter;
  private boolean singleKeyEncryption;

  ParquetFileEncryptor(EncryptionSetup eSetup) throws IOException {
    algorithmId = eSetup.getAlgorithmID();
    uniformEncryption = eSetup.isUniformEncryption();
    singleKeyEncryption = eSetup.isSingleKeyEncryption();
    if (EncryptionAlgorithm.AES_GCM_V1 != algorithmId && 
        EncryptionAlgorithm.AES_GCM_CTR_V1 != algorithmId) {
      throw new IOException("Algorithm " + eSetup.getAlgorithmID() + " is not supported");
    }
    footerKeyBytes = eSetup.getFooterKeyBytes();
    if (null ==  footerKeyBytes) {
      if (uniformEncryption) throw new IOException("Null key in uniform encryption");
      encryptFooter = false;
    }
    else {
      encryptFooter = true;
    }
    footerKeyMetaDataBytes = eSetup.getFooterKeyMetadata();
    if (null != footerKeyMetaDataBytes) {
      if (footerKeyMetaDataBytes.length > 256) { // TODO 
        throw new IOException("Key MetaData is too long "+footerKeyMetaDataBytes.length);
      }
    }
    columnMDListSet = false;
    if (!uniformEncryption) columnMDList = new LinkedList<ColumnCryptoMetaData>();
    try {
     LOG.info("AES-GCM cipher provider: {}", Cipher.getInstance("AES/GCM/NoPadding").getProvider());
     if (EncryptionAlgorithm.AES_GCM_CTR_V1 == algorithmId) {
       LOG.info("AES-CTR cipher provider: {}", Cipher.getInstance("AES/CTR/NoPadding").getProvider());
     }
    } catch (GeneralSecurityException e) {
      throw new IOException("Failed to get cipher", e);
    }
    aadBytes = eSetup.getAAD();
    encryptionSetup = eSetup;
    LOG.info("File encryptor. Uniform encryption: {}. Key metadata set: {}", 
        uniformEncryption, (null != footerKeyMetaDataBytes));
  }

  // Find column in a list
  static ColumnCryptoMetaData findColumn(String[] path, List<ColumnCryptoMetaData> columnMDList) {
    for (ColumnCryptoMetaData ccmd: columnMDList) {
      List<String> cpath = ccmd.getPath_in_schema();
      int i=0;
      boolean match = true;
      for (String col : cpath) {
        if (i >= path.length) {
          match = false;
          break;
        }
        if (!col.equals(path[i])) {
          match = false;
          break;
        }
        i++;
      }
      if (match) return ccmd;
    }
    return null;
  }

  // Returns two encryptors - for page headers, and for page contents (can be the same)
  public synchronized ColumnEncryptors getColumnEncryptors(String[] columnPath) throws IOException {
    ColumnEncryptors encryptors = new ColumnEncryptors();
    if (uniformEncryption || singleKeyEncryption) {
      if (null == aesGcmBlockEncryptor) aesGcmBlockEncryptor = new AesGcmEncryptor(footerKeyBytes, aadBytes);
      encryptors.metadataEncryptor = aesGcmBlockEncryptor;
      if (EncryptionAlgorithm.AES_GCM_CTR_V1 == algorithmId) {
        if (null == aesCtrBlockEncryptor) aesCtrBlockEncryptor = new AesCtrEncryptor(footerKeyBytes);
        encryptors.dataEncryptor = aesCtrBlockEncryptor;
      }
      else {
        encryptors.dataEncryptor = aesGcmBlockEncryptor;
      }
    }
    if (uniformEncryption) return encryptors;
    
    ColumnMetadata cmd = encryptionSetup.getColumnMetadata(columnPath);
    if (null == cmd) {
      throw new IOException("No encryption metadata for column " + Arrays.toString(columnPath));
    }
    if (null == findColumn(columnPath, columnMDList)) {
      columnMDList.add(cmd.getColumnCryptoMetaData());
    }
    if (cmd.isEncrypted()) {
      // TODO if encrypt is always true, set uniformEncryption = true for single key encryption
      if (singleKeyEncryption) return encryptors;
      
      byte[] key_bytes =  cmd.getKeyBytes();
      if (null == key_bytes) key_bytes = footerKeyBytes;
      if (null == key_bytes) throw new IOException("Null key in encrypted column");
      encryptors.metadataEncryptor = new AesGcmEncryptor(key_bytes, aadBytes);
      
      if (EncryptionAlgorithm.AES_GCM_CTR_V1 == algorithmId) {
        encryptors.dataEncryptor = new AesCtrEncryptor(key_bytes);
      }
      else {
        encryptors.dataEncryptor = encryptors.metadataEncryptor;
      }
      return encryptors;
    }
    else {
      if (LOG.isDebugEnabled()) LOG.debug("Column {} is not encrypted", Arrays.toString(columnPath));
      // TODO check is no column is encrypted, and footer is not encrypted
      return null;
    }
  }

  public synchronized BlockCrypto.Encryptor getFooterEncryptor() throws IOException  {
    if (!encryptFooter) return null;
    if (null == aesGcmBlockEncryptor) aesGcmBlockEncryptor = new AesGcmEncryptor(footerKeyBytes, aadBytes);
    return aesGcmBlockEncryptor;
  }

  public synchronized FileCryptoMetaData getFileCryptoMetaData(long footer_index) throws IOException {
    FileCryptoMetaData fcmd = new FileCryptoMetaData(algorithmId, encryptFooter, footer_index, uniformEncryption);
    if (null != footerKeyMetaDataBytes) fcmd.setKey_metadata(footerKeyMetaDataBytes);
    if (!uniformEncryption) {
      if (columnMDListSet) {
        throw new IOException("Re-using file encryptor with non-uniform encryption");
      }
      fcmd.setColumn_crypto_meta_data(columnMDList);
      columnMDListSet = true;      
    }
    return fcmd;
  }

  //Single key means: footer and columns are encrypted with the same key. Some columns can be plaintext, but footer must be encrypted.
//TODO: split into two: encr footer, and multiple keys
  public boolean isSingleKeyEncryption() {
    return singleKeyEncryption;
  }

  public boolean isUniformEncryption() {
    return uniformEncryption;
  }
}
