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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.crypto.Cipher;


public class ParquetFileDecryptor {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetFileDecryptor.class);

  private BlockCrypto.Decryptor aesGcmBlockDecryptor;
  private BlockCrypto.Decryptor aesCtrBlockDecryptor;
  private byte[] footerKeyBytes;
  private boolean uniformEncryption;
  private List<ColumnCryptoMetaData> columnMDList;
  private EncryptionAlgorithm algorithmId;
  private DecryptionKeyRetriever keyRetriever;
  private byte[] aadBytes;
  private boolean footerEncrypted;
  private DecryptionSetup dSetup;
  private boolean fileCryptoMDSet = false;
  
  
  ParquetFileDecryptor(DecryptionSetup dSetup) throws IOException {
    this.dSetup= dSetup;
    footerKeyBytes = dSetup.getFooterKeyBytes();
    keyRetriever = dSetup.getKeyRetriever();
    if ((null != footerKeyBytes) && (null != keyRetriever)) {
      throw new IOException("Can't set both explicit key and key retriever");
    }
    aadBytes = dSetup.getAAD();
    uniformEncryption = true;
    try {
      LOG.info("AES-GCM cipher provider: {}", Cipher.getInstance("AES/GCM/NoPadding").getProvider());
    } catch (GeneralSecurityException e) {
      throw new IOException("Failed to get cipher", e);
    }
    LOG.info("File decryptor. Explicit footer key: {}. Key retriever: {}", 
        (null != footerKeyBytes), (null != keyRetriever));
  }
  
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
    // Non-uniform encryption
    if (null == columnMDList) {
      throw new IOException("Non-uniform encryption: column crypto metadata unavailable in " + Arrays.toString(path));
    }
    ColumnCryptoMetaData ccmd = ParquetFileEncryptor.findColumn(path, columnMDList);
    if (null == ccmd) {
      throw new IOException("Failed to find crypto metadata for column " + Arrays.toString(path));
    }
    // Unencrypted column
    if (!ccmd.isEncrypted()) {
      if (LOG.isDebugEnabled()) LOG.debug("Column {} is not encrypted", Arrays.toString(path));
      decryptors.status = ColumnDecryptors.Status.PLAINTEXT;
      return decryptors;
    }
    // TODO optimize (store in a map)? called up to twice in a file (?)
    // Column is encrypted, find the key
    byte[] columnKeyBytes = dSetup.getColumnKey(path);
    // No explicit column key given via API
    if (null == columnKeyBytes) {
      // Use footer key
      if (ccmd.isEncrypted_with_footer_key()) {
        columnKeyBytes = footerKeyBytes;
        if (null == columnKeyBytes) {
          throw new IOException("Column encrypted with null footer key " + Arrays.toString(path));
        }
      }
      // Column-specific key. Retrieve via metadata.
      else {
        byte[] columnKeyMeta = ccmd.getColumn_key_metadata();
        if (null == columnKeyMeta)  {
          throw new IOException("No column key or key metadata " + Arrays.toString(path));
        }
        if (null == keyRetriever)  {
          throw new IOException("No column key or key retriever " + Arrays.toString(path));
        }
        columnKeyBytes = keyRetriever.getKey(columnKeyMeta);
      }
    }
    // Column is encrypted, but key unavailable
    if (null == columnKeyBytes) {
      if (LOG.isDebugEnabled()) LOG.debug("Column {} is encrypted, but key unavailable", Arrays.toString(path));
      decryptors.status = ColumnDecryptors.Status.KEY_UNAVAILABLE;
      return decryptors;
    }
    // Key is available, return decryptors
    decryptors.status = ColumnDecryptors.Status.KEY_AVAILABLE;
    decryptors.metadataDecryptor = new AesGcmDecryptor(columnKeyBytes, aadBytes);
    if (EncryptionAlgorithm.AES_GCM_CTR_V1 == algorithmId) {
      decryptors.dataDecryptor = new AesCtrDecryptor(columnKeyBytes);
    }
    else {
      decryptors.dataDecryptor = decryptors.metadataDecryptor;
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
      footerEncrypted = fcmd.isEncrypted_footer();
      // ignore key metadata if key is explicitly set via API
      if (footerEncrypted && (null == footerKeyBytes)) { 
        byte[] footer_key_meta_data = fcmd.getFooter_key_metadata();
        if (null == footer_key_meta_data) throw new IOException("No footer key or key metadata");
        if (null == keyRetriever) throw new IOException("No footer key or key retriever");
        footerKeyBytes = keyRetriever.getKey(footer_key_meta_data);
      }
      if (footerEncrypted && (null == footerKeyBytes)) {
        throw new IOException("Footer decryption key unavailable");
      }
      if (footerEncrypted) aesGcmBlockDecryptor = new AesGcmDecryptor(footerKeyBytes, aadBytes);
      fileCryptoMDSet = true;
    }
    // re-use of the decryptor. compare the crypto metadata.
    else {
      // can't compare fileCryptoMetaData directly to fcmd (footer offset, etc)
      if (fcmd.getEncryption_algorithm() != algorithmId) {
        throw new IOException("Decryptor re-use: Different algorithm");
      }
      if (fcmd.isEncrypted_footer() != footerEncrypted) {
        throw new IOException("Decryptor re-use: Encrypted vs plaintext footer");
      }
      // TODO compare key metadata
      // TODO compare iv prefix
    }
  }

  public synchronized void setColumnCryptoMetadata(ColumnCryptoMetaData cryptoMetaData) throws IOException {
    if (null ==cryptoMetaData) throw new IOException("null crypto metadata");
    uniformEncryption = false;
    if (null == columnMDList) columnMDList = new ArrayList<ColumnCryptoMetaData>();
    //TODO check re-use
    columnMDList.add(cryptoMetaData);
  }
}

