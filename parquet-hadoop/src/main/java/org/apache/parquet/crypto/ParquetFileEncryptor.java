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

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import javax.crypto.Cipher;

public class ParquetFileEncryptor {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetFileEncryptor.class);

  private int algorithmId;
  private byte[] keyBytes;
  private BlockCrypto.Encryptor blockEncryptor;
  private EncryptionSetup encryptionSetup;
  private byte[] keyMetaDataBytes = null;
  //Uniform encryption means footer and all columns are encrypted, with same key
  private boolean uniformEncryption;
  private List<ColumnCryptoMetaData> columnMDList;
  private boolean columnMDListSet;
  private byte[] aadBytes;

  ParquetFileEncryptor(EncryptionSetup eSetup) throws IOException {
    // Default algorithm
    algorithmId = ParquetEncryptionFactory.PARQUET_AES_GCM_V1;
    if (eSetup.getAlgorithmID() != algorithmId) {
      throw new IOException("Algorithm " + eSetup.getAlgorithmID() + " is not supported");
    }
    // Footer and columns are encrypted with same key, for now
    keyBytes = eSetup.getFooterKeyBytes();
    if (null ==  keyBytes) {
      throw new IOException("No key provided");
    }
    if (! (keyBytes.length == 16 || keyBytes.length == 24 || keyBytes.length == 32)) {
      throw new IOException("Wrong key length "+keyBytes.length);
    }
    uniformEncryption = eSetup.isUniformEncryption();
    if (!eSetup.isSingleKeyEncryption()) {
      throw new IOException("Multi-key encryption not supported");
    }
    keyMetaDataBytes = eSetup.getFooterKeyMetadata();
    if (null != keyMetaDataBytes) {
      if (keyMetaDataBytes.length > 256) { // TODO 
        throw new IOException("Key MetaData is too long "+keyMetaDataBytes.length);
      }
    }
    columnMDListSet = false;
    if (!uniformEncryption) columnMDList = new LinkedList<ColumnCryptoMetaData>();
    try {
     LOG.info("AES-GCM cipher provider: {}", Cipher.getInstance("AES/GCM/NoPadding").getProvider());
    } catch (GeneralSecurityException e) {
      throw new IOException("Failed to get cipher", e);
    }
    aadBytes = eSetup.getAAD();
    encryptionSetup = eSetup;
    LOG.info("File encryptor. Uniform encryption: {}. Key metadata set: {}", 
        uniformEncryption, (null != keyMetaDataBytes));
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

  public synchronized BlockCrypto.Encryptor getColumnEncryptor(String[] columnPath) throws IOException {
    if (null == blockEncryptor) blockEncryptor = new AesGcmEncryptor(keyBytes, aadBytes);
    if (uniformEncryption) return blockEncryptor;
    // check if column has to be encrypted
    ColumnCryptoMetaData ccmd = encryptionSetup.getColumnMetadata(columnPath);
    if (null == ccmd) {
      throw new IOException("No encryption metadata for column " + Arrays.toString(columnPath));
    }
    if (null == findColumn(columnPath, columnMDList)) {
      columnMDList.add(ccmd);
    }
    if (ccmd.isEncrypted()) {
      // TODO if encrypt is always true, set uniformEncryption = true for single key encryption
      return blockEncryptor;
    }
    else {
      if (LOG.isDebugEnabled()) LOG.debug("Column {} is not encrypted", Arrays.toString(columnPath));
      return null;
    }
  }

  public synchronized BlockCrypto.Encryptor getFooterEncryptor() throws IOException  {
    if (null == blockEncryptor) blockEncryptor = new AesGcmEncryptor(keyBytes, aadBytes);
    return blockEncryptor;
  }

  public synchronized FileCryptoMetaData getFileCryptoMetaData(long footer_index) throws IOException {
    FileCryptoMetaData fcmd = new FileCryptoMetaData(algorithmId, footer_index, uniformEncryption);
    if (null != keyMetaDataBytes) fcmd.setKey_metadata(keyMetaDataBytes);
    if (!uniformEncryption) {
      if (columnMDListSet) {
        throw new IOException("Re-using file encryptor with non-uniform encryption");
      }
      fcmd.setColumn_crypto_meta_data(columnMDList);
      columnMDListSet = true;      
    }
    return fcmd;
  }
}
