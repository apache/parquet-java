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

  private BlockCrypto.Decryptor blockDecryptor;
  private byte[] keyBytes;
  private boolean fileCryptoMDSet = false;
  private boolean uniformEncryption = false;
  private List<ColumnCryptoMetaData> columnMDList;
  private int algorithmId;
  private KeyRetriever keyRetriever;
  private byte[] aadBytes;
  
  ParquetFileDecryptor() throws IOException {
    try {
      LOG.info("AES-GCM cipher provider: {}", Cipher.getInstance("AES/GCM/NoPadding").getProvider());
     } catch (GeneralSecurityException e) {
       throw new IOException("Failed to get cipher", e);
     }
  }

  ParquetFileDecryptor(byte[] keyBytes) throws IOException {
    this();
    this.keyBytes = keyBytes;
  }
  
  ParquetFileDecryptor(KeyRetriever keyRetriever) throws IOException {
    this();
    this.keyRetriever = keyRetriever;
  }
  
  public synchronized BlockCrypto.Decryptor getColumnDecryptor(String[] path) throws IOException {
    if (!fileCryptoMDSet) throw new IOException("Haven't parsed the footer yet");
    if (uniformEncryption) return blockDecryptor;
    if (null == columnMDList) throw new IOException("Non-uniform encryption: column crypto metadata unavailable");
    ColumnCryptoMetaData ccmd = ParquetFileEncryptor.findColumn(path, columnMDList);
    if (null == ccmd) throw new IOException("Failed to find crypto metadata for column " + Arrays.toString(path));
    if (ccmd.isEncrypted()) {
      return blockDecryptor;
    }
    else {
      if (LOG.isDebugEnabled()) LOG.debug("Column {} is not encrypted", Arrays.toString(path));
      return null;
    }
  }

  public synchronized BlockCrypto.Decryptor getFooterDecryptor() throws IOException {
    if (!fileCryptoMDSet) throw new IOException("Haven't parsed the footer yet");
    return blockDecryptor;
  }

  public synchronized void setAAD(byte[] aad)  throws IOException {
    if (fileCryptoMDSet) throw new IOException("Cant set AAD at this stage");
    aadBytes = aad;
  }

  public synchronized void setFileCryptoMetaData(FileCryptoMetaData fcmd) throws IOException {
    // first use of the decryptor
    if (!fileCryptoMDSet) { 
      algorithmId = fcmd.getAlgorithm_id();
      // Initially, support one algorithm only
      if (ParquetEncryptionFactory.PARQUET_AES_GCM_V1 != algorithmId) 
        throw new IOException("Unsupported algorithm: " + algorithmId);
      uniformEncryption = fcmd.isUniform_encryption();
      // ignore key metadata if key is explicitly set via API
      if (null == keyBytes) { 
        if (fcmd.isSetKey_metadata()) {
          byte[] key_meta_data = fcmd.getKey_metadata();
          keyBytes = keyRetriever.getKey(key_meta_data);
        }
      }
      if (null == keyBytes) throw new IOException("Decryption key unavailable");
      blockDecryptor = new AesGcmDecryptor(keyBytes, aadBytes);
      if (fcmd.isSetColumn_crypto_meta_data()) {
        columnMDList = fcmd.getColumn_crypto_meta_data();
      }
      fileCryptoMDSet = true;
    }
    // re-use of the decryptor. checking the parameters.
    else {
      if (algorithmId != fcmd.getAlgorithm_id()) 
        throw new IOException("Re-use with different algorithm: " + fcmd.getAlgorithm_id());
      if (!fcmd.isUniform_encryption())
        throw new IOException("Re-use with non-uniform encryption");
      if (fcmd.isSetKey_metadata()) {
        byte[] key_meta_data = fcmd.getKey_metadata();
        byte[] key_bytes = keyRetriever.getKey(key_meta_data);
        if (!Arrays.equals(key_bytes, keyBytes)) throw new IOException("Re-use with different key");
      }
    }
  }
}

