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
  
  private final DecryptionSetup dSetup;
  private final DecryptionKeyRetriever keyRetriever;
  private final AADRetriever aadRetriever;

  private BlockCipher.Decryptor aesGcmBlockDecryptor;
  private BlockCipher.Decryptor aesCtrBlockDecryptor;
  private byte[] footerKeyBytes;
  private boolean uniformEncryption;
  private List<ColumnDecryptionSetup> columnMDList;
  private EncryptionAlgorithm algorithm;
  private byte[] aadBytes;
  private boolean footerEncrypted;
  private boolean fileCryptoMDSet = false;
  private boolean columnCryptoMDRead = false;
  private ColumnDecryptors footerDecryptors;
  private byte[] ivPrefix;


  ParquetFileDecryptor(DecryptionSetup dSetup) throws IOException {
    this.dSetup= dSetup;
    footerKeyBytes = dSetup.getFooterKeyBytes();
    keyRetriever = dSetup.getKeyRetriever();
    if ((null != footerKeyBytes) && (null != keyRetriever)) {
      throw new IOException("Can't set both explicit key and key retriever");
    }
    aadBytes = dSetup.getAAD();
    aadRetriever = dSetup.getAadRetriever();
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
    columnCryptoMDRead = true;
    if (uniformEncryption)  return getFooterKeyDecryptors();
    
    // Non-uniform encryption
    if (null == columnMDList) {
      throw new IOException("Non-uniform encryption: column crypto metadata unavailable");
    }
    ColumnDecryptionSetup cdmd = findColumn(path);
    if (null == cdmd) {
      throw new IOException("Failed to find crypto metadata for column " + Arrays.toString(path));
    }
    
    return getColumnDecryptors(cdmd);
  }
    
  public synchronized ColumnDecryptors getColumnDecryptors(ColumnDecryptionSetup cdmd) throws IOException {
    if (!fileCryptoMDSet) {
      throw new IOException("Haven't parsed the footer yet");
    }
    columnCryptoMDRead = true;
    if (uniformEncryption)  return getFooterKeyDecryptors();

    if (cdmd.getDecryptors() != null) return cdmd.getDecryptors();

    if (!cdmd.isEncrypted()) {
      ColumnDecryptors decryptors = new ColumnDecryptors();
      decryptors.status = ColumnDecryptors.Status.PLAINTEXT;
      cdmd.setDecryptors(decryptors);
      return decryptors;
    }

    if (cdmd.isEncryptedWithFooterKey()) {
      ColumnDecryptors decryptors = getFooterKeyDecryptors();
      cdmd.setDecryptors(decryptors);
      return decryptors;
    }

    // Column is encrypted with column key, find the key
    byte[] columnKeyBytes = dSetup.getColumnKey(cdmd.getPath());
    
    // No explicit column key given via API. Retrieve via key metadata.
    if (null == columnKeyBytes) {
      byte[] columnKeyMeta = cdmd.getColumnKeyMetadata();
      if (null == columnKeyMeta)  {
        throw new IOException("No column key or key metadata. " + Arrays.toString(cdmd.getPath()));
      }
      if (null == keyRetriever)  {
        throw new IOException("No column key or key retriever. " + Arrays.toString(cdmd.getPath()));
      }
      columnKeyBytes = keyRetriever.getKey(columnKeyMeta);
    }
    
    ColumnDecryptors decryptors = new ColumnDecryptors();
    
    // Column is encrypted, but key unavailable
    if (null == columnKeyBytes) {
      if (LOG.isDebugEnabled()) LOG.debug("Column {} is encrypted, but key unavailable", Arrays.toString(cdmd.getPath()));
      decryptors.status = ColumnDecryptors.Status.KEY_UNAVAILABLE;
      cdmd.setDecryptors(decryptors);
      return decryptors;
    }
    
    // Key is available
    decryptors.status = ColumnDecryptors.Status.KEY_AVAILABLE;
    decryptors.metadataDecryptor = new AesGcmDecryptor(columnKeyBytes, aadBytes, ivPrefix);
    if (algorithm.isSetAES_GCM_CTR_V1()) {
      decryptors.dataDecryptor = new AesCtrDecryptor(columnKeyBytes, ivPrefix);
    }
    else {
      decryptors.dataDecryptor = decryptors.metadataDecryptor;
    }
    cdmd.setDecryptors(decryptors);
    return decryptors;
  }

  private ColumnDecryptors getFooterKeyDecryptors() throws IOException {
    if (null != footerDecryptors) return footerDecryptors;
    footerDecryptors = new ColumnDecryptors();
    footerDecryptors.status = ColumnDecryptors.Status.KEY_AVAILABLE;
    footerDecryptors.metadataDecryptor = aesGcmBlockDecryptor;
    if (algorithm.isSetAES_GCM_CTR_V1()) {
      if (null == aesCtrBlockDecryptor) aesCtrBlockDecryptor = new AesCtrDecryptor(footerKeyBytes, ivPrefix);
      footerDecryptors.dataDecryptor = aesCtrBlockDecryptor;
    }
    else {
      footerDecryptors.dataDecryptor = aesGcmBlockDecryptor;
    }
    return footerDecryptors;
  }

  public synchronized BlockCipher.Decryptor getFooterDecryptor() throws IOException {
    if (!fileCryptoMDSet) 
      throw new IOException("Haven't parsed the file crypto metadata yet");
    if (!footerEncrypted) return null;
    return aesGcmBlockDecryptor;
  }

  public synchronized void setFileCryptoMetaData(FileCryptoMetaData fcmd) throws IOException {
    // first use of the decryptor
    if (!fileCryptoMDSet) { 
      algorithm = fcmd.getEncryption_algorithm();
      byte[] aadMetadata;
      if (algorithm.isSetAES_GCM_V1()) {
        aadMetadata = algorithm.getAES_GCM_V1().getAad_metadata();
      }
      else if (algorithm.isSetAES_GCM_CTR_V1()) {
        aadMetadata = algorithm.getAES_GCM_CTR_V1().getAad_metadata();
      }
      else {
        throw new IOException("Unsupported algorithm: " + algorithm);
      }
      footerEncrypted = fcmd.isEncrypted_footer();
      if (!footerEncrypted) uniformEncryption = false;
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
      if ((null == aadBytes) && (null != aadRetriever) && (null != aadMetadata)) {
        aadBytes = aadRetriever.getAAD(aadMetadata);
      }
      ivPrefix = fcmd.getIv_prefix();
      if (footerEncrypted) aesGcmBlockDecryptor = new AesGcmDecryptor(footerKeyBytes, aadBytes, ivPrefix);
      fileCryptoMDSet = true;
    }
    // re-use of the decryptor. compare the crypto metadata.
    else {
      // can't compare fileCryptoMetaData directly to fcmd (footer offset, etc)
      if (!fcmd.getEncryption_algorithm().equals(algorithm)) {
        throw new IOException("Decryptor re-use: Different algorithm");
      }
      if (fcmd.isEncrypted_footer() != footerEncrypted) {
        throw new IOException("Decryptor re-use: Encrypted vs plaintext footer");
      }
      // TODO compare key metadata
      // TODO compare iv prefix
      // TODO compare aad metadata
    }
  }

  public synchronized ColumnDecryptionSetup setColumnCryptoMetadata(String[] path, boolean encrypted, 
      byte[] keyMetadata, ColumnCryptoMetaData cryptoMetaData) throws IOException {

    if (null == columnMDList) columnMDList = new ArrayList<ColumnDecryptionSetup>();

    boolean reuse = false;
    ColumnDecryptionSetup cdmd = findColumn(path);

    if (null != cdmd) {
      reuse = true;
      if (!columnCryptoMDRead) throw new IOException("Two identical columns " + Arrays.toString(path));
      if (cdmd.isEncrypted() != encrypted) throw new IOException("Re-use with different encryption");
    }
    else {
      cdmd = new ColumnDecryptionSetup(encrypted, path);
    }

    if (!encrypted) {
      columnMDList.add(cdmd);
      return cdmd;
    }

    if (cryptoMetaData.isSetENCRYPTION_WITH_COLUMN_KEY()) {
      uniformEncryption = false;
      if (reuse) {
        if (cdmd.isEncryptedWithFooterKey()) throw new IOException("Re-use with footer-key encryption");
      }
      else {
        cdmd.setEncryptedWithFooterKey(false);
      }
      cdmd.setKeyMetadata(keyMetadata);
    }
    else {
      if (reuse) {
        if (!cdmd.isEncryptedWithFooterKey()) throw new IOException("Re-use with column-key encryption");
      }
      else {
        cdmd.setEncryptedWithFooterKey(true);
      }
    }
    if (!reuse) columnMDList.add(cdmd);
    return cdmd;
  }

  // TODO replace with a Map lookup
  private ColumnDecryptionSetup findColumn(String[] path) {
    for (ColumnDecryptionSetup cmd: columnMDList) {
      if (Arrays.deepEquals(path, cmd.getPath())) {
        return cmd;
      }
    }
    return null;
  }

  public synchronized void columnCryptoMetaDataProcessed() {
    columnCryptoMDRead = true;
  }
}

