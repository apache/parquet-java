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

import java.util.Arrays;
import java.util.List;

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.format.ColumnCryptoMetaData;

public class EncryptionSetup {
  
  private byte[] footerKeyBytes;
  private byte[] footerKeyMetadata;
  private byte[] aadBytes;
  private List<String[]> columnList;
  private boolean encryptColumnsInList;
  //Uniform encryption means footer and all columns are encrypted, with same key
  private boolean uniformEncryption;
  
  /**
   * Constructor with a custom key metadata.
   * 
   * @param keyBytes Encryption key for file footer and some (or all) columns.
   * @param keyMetadata Key metadata, to be written in a file for key retrieval upon decryption. Can be null.
   */
  public EncryptionSetup(byte[] keyBytes, byte[] keyMetadata) {
    footerKeyBytes = keyBytes;
    footerKeyMetadata = keyMetadata;
    uniformEncryption = true;
  }
  
  /**
   * Constructor with a 4-byte key metadata derived from an integer key ID.
   * 
   * @param keyBytes Encryption key for file footer and some (or all) columns.
   * @param keyId Key id - will be converted to a 4-byte metadata and written in a file for key retrieval upon decryption.
   */
  public EncryptionSetup(byte[] keyBytes, int keyId) {
    this(keyBytes, BytesUtils.intToBytes(keyId));
  }
  
  /**
   * Set the list of columns to encrypt. Other columns will be left unencrypted.
   * 
   * @param columnList
   */
  public void setEncryptedColumns(String[][] columnList) {
    encryptColumnsInList = true;
    uniformEncryption = false;
    this.columnList = Arrays.asList(columnList);
  }
  
  /**
   * Set the list of columns that will be left unencrypted. Other columns will be encrypted.
   * 
   * @param columnList
   */
  public void setUnencryptedColumns(String[][] columnList) {
    encryptColumnsInList = false;
    uniformEncryption = false;
    this.columnList = Arrays.asList(columnList);
  }
  
  /**
   * Set the AES-GCM additional authenticated data (AAD).
   * 
   * @param aad
   */
  public void setAAD(byte[] aad) {
    aadBytes = aad;
  }
  
  int getAlgorithmID() {
    return ParquetEncryptionFactory.PARQUET_AES_GCM_V1;
  }

  byte[] getFooterKeyBytes() {
    return footerKeyBytes;
  }

  byte[] getFooterKeyMetadata() {
    return footerKeyMetadata;
  }

  boolean isUniformEncryption() {
    return uniformEncryption;
  }

  boolean isSingleKeyEncryption() {
    return true;
  }

  ColumnCryptoMetaData getColumnMetadata(String[] columnPath) {
    boolean in_list = columnList.contains(columnPath); 
    boolean encrypt;
    if (in_list) {
      encrypt = encryptColumnsInList;
    }
    else {
      encrypt = !encryptColumnsInList;
    }
    return new ColumnCryptoMetaData(Arrays.asList(columnPath), encrypt);
  }

  byte[] getAAD() {
    return aadBytes;
  }
}
