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
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;

public class ParquetFileDecryptor {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetFileDecryptor.class);

  private BlockCrypto.Decryptor blockDecryptor;
  private byte[] keyBytes;
  private boolean fileCryptoMDSet = false;
  private boolean uniformEncryption = false;
  private List<ColumnCryptoMetaData> columnMDList;
  private int algorithmId;
  private DecryptionKeyRetriever keyRetriever;
  private byte[] aadBytes;
  
  
  ParquetFileDecryptor(DecryptionSetup dSetup) throws IOException {
    keyBytes = dSetup.getKeyBytes();
    if (null != keyBytes) {
      if (! (keyBytes.length == 16 || keyBytes.length == 24 || keyBytes.length == 32)) {
        throw new IOException("Wrong key length "+keyBytes.length);
      }
    }
    keyRetriever = dSetup.getKeyRetriever();
    if ((null != keyBytes) && (null != keyRetriever)) {
      throw new IOException("Can't set both explicit key and key retriever");
    }
    aadBytes = dSetup.getAAD();
    try {
      LOG.info("AES-GCM cipher provider: {}", Cipher.getInstance("AES/GCM/NoPadding").getProvider());
    } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
      throw new IOException("Failed to get cipher", e);
    }
    LOG.info("File decryptor. Explicit key: {}. Key retriever: {}", 
        (null != keyBytes), (null != keyRetriever));
  }
  
  public synchronized BlockCrypto.Decryptor getColumnDecryptor(String[] path) throws IOException {
    if (!fileCryptoMDSet) {
      throw new IOException("Haven't parsed the footer yet");
    }
    //Uniform encryption means footer and all columns are encrypted, with same key
    if (uniformEncryption) return blockDecryptor;
    // Column crypto metadata should have been extracted from crypto footer
    if (null == columnMDList) {
      throw new IOException("Non-uniform encryption: column crypto metadata unavailable");
    }
    ColumnCryptoMetaData ccmd = ParquetFileEncryptor.findColumn(path, columnMDList);
    if (null == ccmd) {
      throw new IOException("Failed to find crypto metadata for column " + Arrays.toString(path));
    }
    if (ccmd.isEncrypted()) {
      return blockDecryptor;
    }
    else {
      if (LOG.isDebugEnabled()) LOG.debug("Column {} is not encrypted", Arrays.toString(path));
      return null;
    }
  }

  public synchronized BlockCrypto.Decryptor getFooterDecryptor() throws IOException {
    if (!fileCryptoMDSet) 
      throw new IOException("Haven't parsed the file crypto metadata yet");
    return blockDecryptor;
  }

  public synchronized void setFileCryptoMetaData(FileCryptoMetaData fcmd) throws IOException {
    // first use of the decryptor
    if (!fileCryptoMDSet) { 
      algorithmId = fcmd.getAlgorithm_id();
      // Initially, support one algorithm only
      if (ParquetEncryptionFactory.PARQUET_AES_GCM_V1 != algorithmId) {
        throw new IOException("Unsupported algorithm: " + algorithmId);
      }
      uniformEncryption = fcmd.isUniform_encryption();
      // ignore key metadata if key is explicitly set via API
      if (null == keyBytes) { 
        if (fcmd.isSetKey_metadata()) {
          byte[] key_meta_data = fcmd.getKey_metadata();
          keyBytes = keyRetriever.getKey(key_meta_data);
        }
      }
      if (null == keyBytes) {
        throw new IOException("Decryption key unavailable");
      }
      blockDecryptor = new AesGcmDecryptor(keyBytes, aadBytes);
      if (fcmd.isSetColumn_crypto_meta_data()) {
        columnMDList = fcmd.getColumn_crypto_meta_data();
      }
      fileCryptoMDSet = true;
    }
    // re-use of the decryptor. checking the parameters.
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
        if (!Arrays.equals(key_bytes, keyBytes)) {
          throw new IOException("Re-use with different key");
        }
      }
    }
  }
}

