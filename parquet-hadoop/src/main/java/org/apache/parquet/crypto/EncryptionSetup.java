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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.format.EncryptionAlgorithm;

public class EncryptionSetup {
  
  private final EncryptionAlgorithm algorithm;
  private final byte[] footerKeyBytes;
  private final byte[] footerKeyMetadata;
  
  private byte[] aadBytes;
  private List<ColumnCryptodata> columnList;
  private boolean encryptTheRest;
  //Uniform encryption means footer and all columns are encrypted, with same key
  private boolean uniformEncryption;
  private boolean setupProcessed;
  
  /**
   * Constructor with a custom key metadata.
   * 
   * @param keyBytes Encryption key for file footer and some (or all) columns.
   * @param keyMetadata Key metadata, to be written in a file for key retrieval upon decryption. Can be null.
   * @throws IOException 
   */
  public EncryptionSetup(ParquetCipher cipher, byte[] keyBytes, byte[] keyMetadata) throws IOException {
    footerKeyBytes = keyBytes;
    footerKeyMetadata = keyMetadata;
    if (null != footerKeyBytes) {
      if (! (footerKeyBytes.length == 16 || footerKeyBytes.length == 24 || footerKeyBytes.length == 32)) {
        throw new IOException("Wrong key length " + footerKeyBytes.length);
      }
    }
    if ((null != footerKeyMetadata) && (footerKeyMetadata.length > 256)) { // TODO 
      throw new IOException("Footer key meta data is too long: " + footerKeyMetadata.length);
    }
    uniformEncryption = true;
    algorithm = cipher.getEncryptionAlgorithm();
    setupProcessed = false;
  }
  
  /**
   * Constructor with a 4-byte key metadata derived from an integer key ID.
   * 
   * @param keyBytes Encryption key for file footer and some (or all) columns.
   * @param keyId Key id - will be converted to a 4-byte metadata and written in a file for key retrieval upon decryption.
   * @throws IOException 
   */
  public EncryptionSetup(ParquetCipher algorithm, byte[] keyBytes, int keyId) throws IOException {
    this(algorithm, keyBytes, BytesUtils.intToBytes(keyId));
  }
  
  /**
   * Set column crypto metadata (eg what columns should be encrypted). Each column in the list has a boolean 'encrypted' flag.
   * The list doesn't have to include all columns in a file. If encryptTheRest is true, the rest of the columns (not in the list)
   * will be encrypted with the file footer key. If encryptTheRest is false, the rest of the columns will be left unencrypted.
   * @param columnList
   * @param encryptTheRest  
   * @throws IOException 
   */
  public void setColumns(List<ColumnCryptodata> columnList, boolean encryptTheRest) throws IOException {
    if (setupProcessed) throw new IOException("Setup already processed");
    // TODO if set, throw an exception? or allow to replace
    uniformEncryption = false;
    this.encryptTheRest = encryptTheRest;
    this.columnList = columnList;
    if (null == footerKeyBytes) {
      if (encryptTheRest) throw new IOException("Encrypt the rest with null footer key");
      boolean all_are_unencrypted = true;
      for (ColumnCryptodata cmd : columnList) {
        if (cmd.isEncrypted()) {
          if (null == cmd.getKeyBytes()) {
            throw new IOException("Encrypt column with null footer key");
          }
          all_are_unencrypted = false;
        }
      }
      if (all_are_unencrypted) throw new IOException("Footer and all columns unencrypted");
    }
  }
  
  /**
   * Set the AES-GCM additional authenticated data (AAD).
   * 
   * @param aad
   * @throws IOException 
   */
  public void setAAD(byte[] aad, byte[] aadMetadata) throws IOException {
    if (setupProcessed) throw new IOException("Setup already processed");
    if (null == aad) throw new IOException("Null AAD");
    // TODO if set, throw an exception? or allow to replace
    aadBytes = aad;
    if (null != aadMetadata) {
      if (aadMetadata.length > 256) throw new IOException("AAD metadata is too long: " + aadMetadata.length); //TODO
      if (algorithm.isSetAES_GCM_V1()) {
        algorithm.getAES_GCM_V1().setAad_metadata(aadMetadata);
      }
      else {
        algorithm.getAES_GCM_CTR_V1().setAad_metadata(aadMetadata);
      }
    }
  }
  
  EncryptionAlgorithm getAlgorithm() {
    setupProcessed = true;
    return algorithm;
  }

  byte[] getFooterKeyBytes() {
    setupProcessed = true;
    return footerKeyBytes;
  }

  byte[] getFooterKeyMetadata() {
    setupProcessed = true;
    return footerKeyMetadata;
  }

  boolean isUniformEncryption() {
    setupProcessed = true;
    return uniformEncryption;
  }

  ColumnCryptodata getColumnMetadata(String[] columnPath) {
    setupProcessed = true;
    boolean in_list = false;
    ColumnCryptodata cmd = null;
    for (ColumnCryptodata col : columnList) {
      if (Arrays.deepEquals(columnPath, col.getPath())) {
        in_list = true;
        cmd = col;
        break;
      }
    }
    if (in_list) {
      return cmd;
    }
    else {
      return new ColumnCryptodata(encryptTheRest, columnPath);
    }
  }

  byte[] getAAD() {
    setupProcessed = true;
    return aadBytes;
  }
}
