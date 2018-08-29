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

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.parquet.format.EncryptionAlgorithm;
import org.apache.parquet.hadoop.metadata.ColumnPath;

public class FileEncryptionProperties {
  
  static final int MAXIMAL_KEY_METADATA_LENGTH = 256;
  static final int MAXIMAL_AAD_METADATA_LENGTH = 256;
  
  private static final ParquetCipher ALGORITHM_DEFAULT = ParquetCipher.AES_GCM_V1;
  private static final boolean ENCRYPT_THE_REST_DEFAULT = true;
  
  private final EncryptionAlgorithm algorithm;
  private final byte[] footerKeyBytes;
  private final byte[] footerKeyMetaData;
  private final byte[] aadBytes;
  private final Map<ColumnPath, ColumnEncryptionProperties> columnPropertyMap;
  private final boolean encryptTheRest;

  
  private FileEncryptionProperties(ParquetCipher cipher, byte[] footerKeyBytes, byte[] footerKeyMetaData, 
      byte[] aadBytes, byte[] aadMetaData, Map<ColumnPath, ColumnEncryptionProperties> columnPropertyMap, 
      boolean encryptTheRest) {

    if (null == footerKeyBytes) {
      if (encryptTheRest) {
        throw new IllegalArgumentException("Encrypt the rest with null footer key");
      }
      if (null != footerKeyMetaData) {
        throw new IllegalArgumentException("Setting key metadata for null footer key");
      }
      if (null == columnPropertyMap) {
        throw new IllegalArgumentException("Footer and all columns are unencrypted (no properties set)");
      }
      else {
        // Check column properties
        boolean allAreUnencrypted = true;
        for (ColumnEncryptionProperties columnProperties : columnPropertyMap.values()) {
          if (columnProperties.isEncrypted()) {
            if (null == columnProperties.getKeyBytes()) {
              throw new IllegalArgumentException("Encrypt column with null footer key. Column: " + 
                  columnProperties.getPath());
            }
            allAreUnencrypted = false;
          }
        }
        if (allAreUnencrypted) {
          throw new IllegalArgumentException("Footer and all columns are unencrypted");
        }
      }
    }
    else {
      if (! (footerKeyBytes.length == 16 || footerKeyBytes.length == 24 || footerKeyBytes.length == 32)) {
           throw new IllegalArgumentException("Wrong key length " + footerKeyBytes.length);
      }
    }
    
    if ((null != footerKeyMetaData) && (footerKeyMetaData.length > MAXIMAL_KEY_METADATA_LENGTH)) {
      throw new IllegalArgumentException("Footer key meta data is longer than " + 
          MAXIMAL_KEY_METADATA_LENGTH +" bytes: " + footerKeyMetaData.length);
    }
    
    this.algorithm = cipher.getEncryptionAlgorithm();
    if (null != aadMetaData) {
      if (aadMetaData.length > MAXIMAL_AAD_METADATA_LENGTH) {
        throw new IllegalArgumentException("AAD meta data is longer than " + 
            MAXIMAL_AAD_METADATA_LENGTH +" bytes: " + aadMetaData.length);
      }
      if (algorithm.isSetAES_GCM_V1()) {
        algorithm.getAES_GCM_V1().setAad_metadata(aadMetaData);
      }
      else {
        algorithm.getAES_GCM_CTR_V1().setAad_metadata(aadMetaData);
      }
    }

    this.footerKeyBytes = footerKeyBytes;
    this.footerKeyMetaData = footerKeyMetaData;
    this.aadBytes = aadBytes;
    this.columnPropertyMap = columnPropertyMap;
    this.encryptTheRest = encryptTheRest;
  }
  
  /**
   * 
   * @param keyBytes Encryption key for file footer and some (or all) columns. 
   * Key length must be either 16, 24 or 32 bytes.
   * If null, footer won't be encrypted. At least one column must be encrypted then.
   */
  public static Builder builder(byte[] keyBytes) {
    return new Builder(keyBytes);
  }
  
  public static class Builder {
    private final byte[] footerKeyBytes;
    private ParquetCipher parquetCipher;
    private byte[] footerKeyMetaData;
    private byte[] aadBytes;
    private byte[] aadMetaData;
    private Map<ColumnPath, ColumnEncryptionProperties> columnPropertyMap;
    private boolean encryptTheRest;
    
    
    private Builder(byte[] footerKeyBytes) {
      this.footerKeyBytes = footerKeyBytes;
      this.parquetCipher = ALGORITHM_DEFAULT;
      this.encryptTheRest = ENCRYPT_THE_REST_DEFAULT;
    }
    
    public Builder withAlgorithm(ParquetCipher parquetCipher) {
      this.parquetCipher = parquetCipher;
      return this;
    }
    
    /**
    * Set a key retrieval meta data.
    * use either withKeyMetaData or withKeyID, not both
    * @param footerKeyMetaData maximal length is 256 bytes.
    */
    public Builder withKeyMetaData(byte[] footerKeyMetaData) {
      if (null == footerKeyMetaData) {
        return this;
      }
      if (null != this.footerKeyMetaData) {
        throw new IllegalArgumentException("Footer key metadata already set");
      }
      this.footerKeyMetaData = footerKeyMetaData;
      return this;
    }
    
    /**
     * Set a key retrieval meta data (converted from String).
     * use either withKeyMetaData or withKeyID, not both
     * @param keyId will be converted to metadata (UTF-8 array).
     */
    public Builder withKeyID(String keyId) {
      if (null == keyId) {
        return this;
      }
      byte[] metadata = keyId.getBytes(StandardCharsets.UTF_8);
      return withKeyMetaData(metadata);
    }
    
    /**
     * Set the AES-GCM additional authenticated data (AAD).
     * @param aadBytes
     */
    public Builder withAAD(byte[] aadBytes) {
      if (null == aadBytes) {
        return this;
      }
      if (null != this.aadBytes) {
        throw new IllegalArgumentException("AAD already set");
      }
      this.aadBytes = aadBytes;
      return this;
    }
    
    /**
     * Set AAD retrieval meta data.
     * @param aadMetadata maximal length is 256 bytes
     */
    public Builder withAADMetaData(byte[] aadMetadata) {
      if (null == aadMetadata) {
        return this;
      }
      if (null != this.aadMetaData) {
        throw new IllegalArgumentException("AAD metadata already set");
      }
      this.aadMetaData = aadMetadata;
      return this;
    }
    
    /**
     * Set column encryption properties. 
     * The map doesn't have to include all columns in a file. If encryptTheRest is true, 
     * the rest of the columns (not in the map) will be encrypted with the file footer key. 
     * If encryptTheRest is false, the rest of the columns will be left unencrypted.
     * @param columnPropertyMap
     * @param encryptTheRest  
     */
    public Builder withColumnProperties(Map<ColumnPath, ColumnEncryptionProperties> columnPropertyMap, 
        boolean encryptTheRest)  {
      if (null == columnPropertyMap) {
        return this;
      }
      if (null != this.columnPropertyMap) {
        throw new IllegalArgumentException("Column properties already set");
      }
      this.columnPropertyMap = columnPropertyMap;
      this.encryptTheRest = encryptTheRest;
      return this;
    }
    
    public FileEncryptionProperties build() {
      return new FileEncryptionProperties(parquetCipher, footerKeyBytes, footerKeyMetaData, aadBytes, 
        aadMetaData, columnPropertyMap, encryptTheRest);
    }
  }
  
  public EncryptionAlgorithm getAlgorithm() {
    return algorithm;
  }

  public byte[] getFooterKeyBytes() {
    return footerKeyBytes;
  }

  public byte[] getFooterKeyMetaData() {
    return footerKeyMetaData;
  }

  public ColumnEncryptionProperties getColumnProperties(ColumnPath columnPath) {
    if (null != columnPropertyMap) {
      ColumnEncryptionProperties columnProperties = columnPropertyMap.get(columnPath);
      if (null != columnProperties) {
        return columnProperties;
      }
    }
    // Not in the map. Create using the encryptTheRest flag.
    return ColumnEncryptionProperties.builder(columnPath, encryptTheRest).build();
  }

  public byte[] getAAD() {
    return aadBytes;
  }
}
