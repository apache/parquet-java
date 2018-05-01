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
  private ColumnEncryptionFilter columnFilter;
  private byte[] keyMetaDataBytes = null;
  private boolean supportsAAD;
  private boolean uniformEncryption;
  private List<ColumnCryptoMetaData> columnMDList;
  private boolean columnMDListSet;
  private boolean initialized;
  private byte[] aadBytes;

  ParquetFileEncryptor(byte[] keyBytes, KeyMetaData keyMD, ColumnEncryptionFilter filter) throws IOException {
    // Default Encryption algorithm
    algorithmId = ParquetEncryptionFactory.PARQUET_AES_GCM_V1;
    this.keyBytes = keyBytes;
    columnFilter = filter;
    supportsAAD = true;
    uniformEncryption = (null == columnFilter);
    if (null != keyMD) keyMetaDataBytes = keyMD.getBytes();
    columnMDListSet = false;
    initialized = false;
    if (null != columnFilter) columnMDList = new LinkedList<ColumnCryptoMetaData>();
    try {
     LOG.info("AES-GCM cipher provider: {}", Cipher.getInstance("AES/GCM/NoPadding").getProvider());
    } catch (GeneralSecurityException e) {
      throw new IOException("Failed to get cipher", e);
    }
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
    initialized = true;
    if (null == blockEncryptor) blockEncryptor = new AesGcmEncryptor(keyBytes, aadBytes);
    if (uniformEncryption) return blockEncryptor;
    boolean encryptCol = columnFilter.encryptColumn(columnPath);
    if (null == findColumn(columnPath, columnMDList)) {
      ColumnCryptoMetaData ccmd = new ColumnCryptoMetaData(Arrays.asList(columnPath), encryptCol);
      columnMDList.add(ccmd);
    }
    if (encryptCol) {
      // TODO if encrypt is always true, set uniformEncryption = true
      return blockEncryptor;
    }
    else {
      if (LOG.isDebugEnabled()) LOG.debug("Column {} is not encrypted", Arrays.toString(columnPath));
      return null;
    }
  }

  public synchronized BlockCrypto.Encryptor getFooterEncryptor() throws IOException  {
    initialized = true;
    if (null == blockEncryptor) blockEncryptor = new AesGcmEncryptor(keyBytes, aadBytes);
    return blockEncryptor;
  }

  public synchronized void setAAD(byte[] aad)  throws IOException {
    if (!supportsAAD) throw new IOException("Algorithm "+ algorithmId +" doesnt support AAD");
    if (initialized) throw new IOException("Cant set AAD at this stage");
    aadBytes = aad;
  }

  public synchronized FileCryptoMetaData getFileCryptoMetaData(long footer_index) throws IOException {
    FileCryptoMetaData fcmd = new FileCryptoMetaData(algorithmId, footer_index, uniformEncryption);
    if (null != keyMetaDataBytes) fcmd.setKey_metadata(keyMetaDataBytes);
    if (!uniformEncryption) {
      if (columnMDListSet) throw new IOException("Re-using file encryptor with non-uniform encryption");
      fcmd.setColumn_crypto_meta_data(columnMDList);
      columnMDListSet = true;      
    }
    return fcmd;
  }
}

