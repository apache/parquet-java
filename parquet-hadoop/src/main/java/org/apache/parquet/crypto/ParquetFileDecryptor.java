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
  private byte[] fileKeyBytes;
  private boolean fileCryptoMDSet = false;
  private boolean uniformEncryption = false;
  private List<ColumnCryptoMetaData> columnMDList;
  private int algorithmId;
  private DecryptionKeyRetriever keyRetriever;
  private byte[] aadBytes;
  private boolean footerEncrypted;
  private DecryptionSetup dSetup;
  
  
  ParquetFileDecryptor(DecryptionSetup dSetup) throws IOException {
    this.dSetup= dSetup;
    fileKeyBytes = dSetup.getKeyBytes();
    if (null != fileKeyBytes) {
      if (! (fileKeyBytes.length == 16 || fileKeyBytes.length == 24 || fileKeyBytes.length == 32)) {
        throw new IOException("Wrong key length "+fileKeyBytes.length);
      }
    }
    keyRetriever = dSetup.getKeyRetriever();
    if ((null != fileKeyBytes) && (null != keyRetriever)) {
      throw new IOException("Can't set both explicit key and key retriever");
    }
    aadBytes = dSetup.getAAD();
    try {
      LOG.info("AES-GCM cipher provider: {}", Cipher.getInstance("AES/GCM/NoPadding").getProvider());
    } catch (GeneralSecurityException e) {
      throw new IOException("Failed to get cipher", e);
    }
    LOG.info("File decryptor. Explicit key: {}. Key retriever: {}", 
        (null != fileKeyBytes), (null != keyRetriever));
  }
  
  // Returns two decryptors - for page headers, and for page contents (can be the same)
  // TODO optimize: store in a list/map
  public synchronized BlockCrypto.Decryptor[] getColumnDecryptors(String[] path) throws IOException {
    if (!fileCryptoMDSet) {
      throw new IOException("Haven't parsed the footer yet");
    }
    BlockCrypto.Decryptor[] decryptors = new BlockCrypto.Decryptor[2];
    //Uniform encryption means footer and all columns are encrypted, with same key
    if (uniformEncryption)  {
      if (null == aesGcmBlockDecryptor) aesGcmBlockDecryptor = new AesGcmDecryptor(fileKeyBytes, aadBytes);
      decryptors[0] = aesGcmBlockDecryptor;
      if (ParquetEncryptionFactory.PARQUET_AES_GCM_CTR_V1 == algorithmId) {
        if (null == aesCtrBlockDecryptor) aesCtrBlockDecryptor = new AesCtrDecryptor(fileKeyBytes);
        decryptors[1] = aesCtrBlockDecryptor;
      }
      else {
        decryptors[1] = aesGcmBlockDecryptor;
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
        // TODO check if retriever != null
        columnKeyBytes = keyRetriever.getKey(ccmd.getKey_metadata()); // chk null
      }
      if (null == columnKeyBytes) throw new IOException("Column decryption key unavailable");
      decryptors[0] = new AesGcmDecryptor(columnKeyBytes, aadBytes);
      if (ParquetEncryptionFactory.PARQUET_AES_GCM_CTR_V1 == algorithmId) {
        decryptors[1] = new AesCtrDecryptor(columnKeyBytes);
      }
      else {
        decryptors[1] = decryptors[0];
      }
      return decryptors;
    }
    else {
      if (LOG.isDebugEnabled()) LOG.debug("Column {} is not encrypted", Arrays.toString(path));
      return null;
    }
  }

  public synchronized BlockCrypto.Decryptor getFooterDecryptor() throws IOException {
    if (!fileCryptoMDSet) 
      throw new IOException("Haven't parsed the file crypto metadata yet");
    return aesGcmBlockDecryptor;
  }

  public synchronized void setFileCryptoMetaData(FileCryptoMetaData fcmd) throws IOException {
    // first use of the decryptor
    if (!fileCryptoMDSet) { 
      algorithmId = fcmd.getAlgorithm_id();
      if (ParquetEncryptionFactory.PARQUET_AES_GCM_V1 != algorithmId &&
          ParquetEncryptionFactory.PARQUET_AES_GCM_CTR_V1 != algorithmId) {
        throw new IOException("Unsupported algorithm: " + algorithmId);
      }
      uniformEncryption = fcmd.isUniform_encryption();
      footerEncrypted = fcmd.isEncrypted_footer();
      
      // ignore key metadata if key is explicitly set via API
      if (null == fileKeyBytes) { 
        if (fcmd.isSetKey_metadata()) {
          byte[] key_meta_data = fcmd.getKey_metadata();
          // TODO check retriever != null
          fileKeyBytes = keyRetriever.getKey(key_meta_data);
        }
      }
      if (footerEncrypted && (null == fileKeyBytes)) {
        throw new IOException("Footer decryption key unavailable");
      }
      aesGcmBlockDecryptor = new AesGcmDecryptor(fileKeyBytes, aadBytes);
      if (fcmd.isSetColumn_crypto_meta_data()) {
        columnMDList = fcmd.getColumn_crypto_meta_data();
      }
      fileCryptoMDSet = true;
    }
    // re-use of the decryptor. checking the parameters.
    // TODO check multi-key re-use
    else {
      if (algorithmId != fcmd.getAlgorithm_id()) {
        throw new IOException("Re-use with different algorithm: " + fcmd.getAlgorithm_id());
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
        if (!Arrays.equals(key_bytes, fileKeyBytes)) {
          throw new IOException("Re-use with different file key");
        }
      }
    }
  }


  // TODO optimize, get from list/map
  public boolean decryptStats(String[] path) throws IOException {
    if (!fileCryptoMDSet) {
      throw new IOException("Haven't parsed the footer yet");
    }
    //Uniform encryption means footer and all columns are encrypted, with same key
    if (uniformEncryption)  return false;
    
    if (null == fileKeyBytes) return true;
    
    ColumnCryptoMetaData ccmd = ParquetFileEncryptor.findColumn(path, columnMDList);
    if (null == ccmd) {
      throw new IOException("Failed to find crypto metadata for column " + Arrays.toString(path));
    }
    if (!ccmd.isEncrypted()) return false;
    byte[] columnKeyBytes = dSetup.getColumnKey(path);
    if (null == columnKeyBytes) {
      // TODO check if retriever != null
      columnKeyBytes = keyRetriever.getKey(ccmd.getKey_metadata()); // chk null
    }
    if (null == columnKeyBytes) throw new IOException("Column decryption key unavailable");
    return !Arrays.equals(fileKeyBytes, columnKeyBytes);
  }
}

