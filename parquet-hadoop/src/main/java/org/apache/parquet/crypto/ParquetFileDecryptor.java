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
import org.apache.parquet.format.ColumnCryptoMetaData;
import org.apache.parquet.format.EncryptionAlgorithm;
import org.apache.parquet.format.FileCryptoMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;

import javax.crypto.Cipher;


public class ParquetFileDecryptor {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetFileDecryptor.class);

  private BlockCrypto.Decryptor aesGcmBlockDecryptor;
  private BlockCrypto.Decryptor aesCtrBlockDecryptor;
  private byte[] footerKeyBytes;
  private boolean fileCryptoMDSet = false;
  private boolean uniformEncryption = false;
  private List<ColumnCryptoMetaData> columnMDList;
  private EncryptionAlgorithm algorithmId;
  private DecryptionKeyRetriever keyRetriever;
  private byte[] aadBytes;
  private boolean footerEncrypted;
  private DecryptionSetup dSetup;
  
  
  ParquetFileDecryptor(DecryptionSetup dSetup) throws IOException {
    this.dSetup= dSetup;
    footerKeyBytes = dSetup.getFooterKeyBytes();
    keyRetriever = dSetup.getKeyRetriever();
    if ((null != footerKeyBytes) && (null != keyRetriever)) {
      throw new IOException("Can't set both explicit key and key retriever");
    }
    aadBytes = dSetup.getAAD();
    try {
      LOG.info("AES-GCM cipher provider: {}", Cipher.getInstance("AES/GCM/NoPadding").getProvider());
    } catch (GeneralSecurityException e) {
      throw new IOException("Failed to get cipher", e);
    }
    LOG.info("File decryptor. Explicit footer key: {}. Key retriever: {}", 
        (null != footerKeyBytes), (null != keyRetriever));
  }
  
  // TODO optimize: store in a list/map
  public synchronized ColumnDecryptors getColumnDecryptors(String[] path) throws IOException {
    if (!fileCryptoMDSet) {
      throw new IOException("Haven't parsed the footer yet");
    }
    ColumnDecryptors decryptors = new ColumnDecryptors();
    //Uniform encryption means footer and all columns are encrypted, with same key
    if (uniformEncryption)  {
      decryptors.status = ColumnDecryptors.Status.KEY_AVAILABLE;
      if (null == aesGcmBlockDecryptor) aesGcmBlockDecryptor = new AesGcmDecryptor(footerKeyBytes, aadBytes);
      decryptors.metadataDecryptor = aesGcmBlockDecryptor;
      if (EncryptionAlgorithm.AES_GCM_CTR_V1 == algorithmId) {
        if (null == aesCtrBlockDecryptor) aesCtrBlockDecryptor = new AesCtrDecryptor(footerKeyBytes);
        decryptors.dataDecryptor = aesCtrBlockDecryptor;
      }
      else {
        decryptors.dataDecryptor = aesGcmBlockDecryptor;
      }
      return decryptors;
    }
    // Column crypto metadata should have been extracted from crypto footer
    if (null == columnMDList) {
      throw new IOException("Non-uniform encryption: column crypto metadata unavailable");
    }
    ColumnCryptoMetaData ccmd = ParquetFileEncryptor.findColumn(path, columnMDList);
    if (null == ccmd) {
      throw new IOException("Failed to find crypto metadata for column " + Arrays.toString(path));
    }
    if (ccmd.isEncrypted()) {
      byte[] columnKeyBytes = dSetup.getColumnKey(path);
      if (null == columnKeyBytes) {
        if (null == keyRetriever)  throw new IOException("No column key or key retriever");
        columnKeyBytes = keyRetriever.getKey(ccmd.getKey_metadata()); 
      }
      if (null == columnKeyBytes) {
        if (LOG.isDebugEnabled()) LOG.debug("Column {} is encrypted, but key unavailable", Arrays.toString(path));
        decryptors.status = ColumnDecryptors.Status.KEY_UNAVAILABLE;
        return decryptors;
      }
      decryptors.status = ColumnDecryptors.Status.KEY_AVAILABLE;
      decryptors.metadataDecryptor = new AesGcmDecryptor(columnKeyBytes, aadBytes);
      if (EncryptionAlgorithm.AES_GCM_CTR_V1 == algorithmId) {
        decryptors.dataDecryptor = new AesCtrDecryptor(columnKeyBytes);
      }
      else {
        decryptors.dataDecryptor = decryptors.metadataDecryptor;
      }
    }
    else {
      if (LOG.isDebugEnabled()) LOG.debug("Column {} is not encrypted", Arrays.toString(path));
      decryptors.status = ColumnDecryptors.Status.PLAINTEXT;
    }
    
    return decryptors;
  }

  public synchronized BlockCrypto.Decryptor getFooterDecryptor() throws IOException {
    if (!fileCryptoMDSet) 
      throw new IOException("Haven't parsed the file crypto metadata yet");
    if (!footerEncrypted) return null;
    return aesGcmBlockDecryptor;
  }

  public synchronized void setFileCryptoMetaData(FileCryptoMetaData fcmd) throws IOException {
    // first use of the decryptor
    if (!fileCryptoMDSet) { 
      algorithmId = fcmd.getEncryption_algorithm();
      if (EncryptionAlgorithm.AES_GCM_V1 != algorithmId &&
          EncryptionAlgorithm.AES_GCM_CTR_V1 != algorithmId) {
        throw new IOException("Unsupported algorithm: " + algorithmId);
      }
      uniformEncryption = fcmd.isUniform_encryption();
      footerEncrypted = fcmd.isEncrypted_footer();
      
      // ignore key metadata if key is explicitly set via API
      if (footerEncrypted && (null == footerKeyBytes)) { 
        if (fcmd.isSetKey_metadata()) {
          byte[] key_meta_data = fcmd.getKey_metadata();
          if (null == keyRetriever) throw new IOException("No footer key or key retriever");
          footerKeyBytes = keyRetriever.getKey(key_meta_data);
        }
      }
      if (footerEncrypted && (null == footerKeyBytes)) {
        throw new IOException("Footer decryption key unavailable");
      }
      if  (footerEncrypted) aesGcmBlockDecryptor = new AesGcmDecryptor(footerKeyBytes, aadBytes);
      if (fcmd.isSetColumn_crypto_meta_data()) {
        columnMDList = fcmd.getColumn_crypto_meta_data();
      }
      fileCryptoMDSet = true;
    }
    // re-use of the decryptor. checking the parameters.
    // TODO check multi-key re-use
    else {
      if (algorithmId != fcmd.getEncryption_algorithm()) {
        throw new IOException("Re-use with different algorithm: " + fcmd.getEncryption_algorithm());
      }
      if (!fcmd.isUniform_encryption()) {
        if (uniformEncryption) {
          throw new IOException("Re-use with non-uniform encryption");
        }
        if (!fcmd.isSetColumn_crypto_meta_data()) {
          throw new IOException("Re-use with non-uniform encryption: No column metadata");
        }
        if (!columnMDList.equals(fcmd.getColumn_crypto_meta_data())) {
          throw new IOException("Re-use with non-uniform encryption: Different metadata");
        }
      }
      else {
        if (!uniformEncryption) {
          throw new IOException("Re-use with uniform encryption");
        }
      }
      if (fcmd.isSetKey_metadata()) {
        byte[] key_meta_data = fcmd.getKey_metadata();
        byte[] key_bytes = keyRetriever.getKey(key_meta_data);
        if (!Arrays.equals(key_bytes, footerKeyBytes)) {
          throw new IOException("Re-use with different footer key");
        }
      }
    }
  }

  public boolean isUniformEncryption() {
    return uniformEncryption;
  }
}

