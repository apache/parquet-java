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

public class EncryptionSetup {
  
  private int algorithmID;
  private byte[] footerKeyBytes;
  private byte[] footerKeyMetadata;
  private byte[] aadBytes;
  private List<ColumnMetadata> columnList;
  private boolean encryptTheRest;
  //Uniform encryption means footer and all columns are encrypted, with same key
  private boolean uniformEncryption;
  private boolean singleKeyEncryption;
  
  /**
   * Constructor with a custom key metadata.
   * 
   * @param keyBytes Encryption key for file footer and some (or all) columns.
   * @param keyMetadata Key metadata, to be written in a file for key retrieval upon decryption. Can be null.
   */
  public EncryptionSetup(int algorithmID, byte[] keyBytes, byte[] keyMetadata) {
    footerKeyBytes = keyBytes;
    footerKeyMetadata = keyMetadata;
    uniformEncryption = true;
    this.algorithmID = algorithmID;
    singleKeyEncryption = (null != footerKeyBytes);
  }
  
  /**
   * Constructor with a 4-byte key metadata derived from an integer key ID.
   * 
   * @param keyBytes Encryption key for file footer and some (or all) columns.
   * @param keyId Key id - will be converted to a 4-byte metadata and written in a file for key retrieval upon decryption.
   */
  public EncryptionSetup(int algorithmID, byte[] keyBytes, int keyId) {
    this(algorithmID, keyBytes, BytesUtils.intToBytes(keyId));
  }
  
  /**
   * Set column metadata (eg what columns should be encrypted). Each column in the list has a boolean 'encrypted' flag.
   * The list doesn't have to include all columns in a file. If encryptTheRest is true, the rest of the columns (not in the list)
   * will be encrypted with the file footer key. If encryptTheRest is false, the rest of the columns will be left unencrypted.
   * @param columnList
   * @param encryptTheRest  
   */
  public void setColumnMetadata(List<ColumnMetadata> columnList, boolean encryptTheRest) {
    uniformEncryption = false;
    encryptTheRest = true;
    this.columnList = columnList;
    if (null != footerKeyBytes && singleKeyEncryption) {
      for (ColumnMetadata cmd : columnList) {
        if (!Arrays.equals(cmd.getKeyBytes(), footerKeyBytes))  {
          singleKeyEncryption = false;
          break;
        }
      }
    }
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
    return algorithmID;
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
    return singleKeyEncryption;
  }

  ColumnMetadata getColumnMetadata(String[] columnPath) {
    boolean in_list = false;
    ColumnMetadata cmd = null;
    for (ColumnMetadata col : columnList) {
      if (col.getPath().length != columnPath.length) continue;
      boolean equal = true;
      for (int i =0; i < col.getPath().length; i++) {
        if (!col.getPath()[i].equals(columnPath[i])) {
          equal = false;
          break;
        }
      }
      if (equal) {
        in_list = true;
        cmd = col;
        break;
      }
      else {
        continue;
      }
    }
    
    boolean encrypt;
    if (in_list) {
      return cmd;
    }
    else {
      encrypt = encryptTheRest;
      return new ColumnMetadata(encrypt, columnPath);
    }
  }

  byte[] getAAD() {
    return aadBytes;
  }
}
