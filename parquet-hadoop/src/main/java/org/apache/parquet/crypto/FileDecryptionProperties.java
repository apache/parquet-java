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

import java.util.Map;

import org.apache.parquet.hadoop.metadata.ColumnPath;

public class FileDecryptionProperties {


  private final byte[] footerKeyBytes;
  private final DecryptionKeyRetriever keyRetriever;
  private final AADRetriever aadRetriever;
  private final byte[] aadBytes;
  private final Map<ColumnPath, ColumnDecryptionProperties> columnPropertyMap;
  
  private FileDecryptionProperties(byte[] footerKeyBytes, DecryptionKeyRetriever keyRetriever,
      AADRetriever aadRetriever, byte[] aadBytes, Map<ColumnPath, ColumnDecryptionProperties> columnPropertyMap) {
    
    if ((null == footerKeyBytes) && (null == keyRetriever) && (null == columnPropertyMap)) {
      throw new IllegalArgumentException("No crypto meta data specified");
    }
    if ((null != aadBytes) && (null != aadRetriever)) {
      throw new IllegalArgumentException("Can't set both AAD and AAD retriever");
    }
    if ((null != footerKeyBytes) && 
        !(footerKeyBytes.length == 16 || footerKeyBytes.length == 24 || footerKeyBytes.length == 32)) {
      throw new IllegalArgumentException("Wrong key length " + footerKeyBytes.length);
    }
    
    this.footerKeyBytes = footerKeyBytes;
    this.keyRetriever = keyRetriever;
    this.aadRetriever = aadRetriever;
    this.aadBytes = aadBytes;
    this.columnPropertyMap = columnPropertyMap;
  }

  public static Builder builder() {
    return new Builder();
  }
  
  public static class Builder {
    private byte[] footerKeyBytes;
    private DecryptionKeyRetriever keyRetriever;
    private AADRetriever aadRetriever;
    private byte[] aadBytes;
    private Map<ColumnPath, ColumnDecryptionProperties> columnPropertyMap;

    /**
     * Set an explicit footer key. If applied on a file that contains footer key metadata - 
     * the metadata will be ignored, the footer will be decrypted with this key.
     * If explicit key is not set, decryption key will be fetched from key retriever.
     * @param footerKey Key length must be either 16, 24 or 32 bytes. 
     */
    public Builder withFooterKey(byte[] footerKeyBytes) {
      if (null == footerKeyBytes) {
        return this;
      }
      if (null != this.footerKeyBytes) {
        throw new IllegalArgumentException("Footer key already set");
      }
      this.footerKeyBytes = footerKeyBytes;
      return this;
    }

    /**
     * Set the column encryption properties.
     * @param columnPropertyMap
     * @return
     */
    public Builder withColumnProperties(Map<ColumnPath, ColumnDecryptionProperties> columnPropertyMap) {
      if (null == columnPropertyMap) {
        return this;
      }
      if (null != this.columnPropertyMap) {
        throw new IllegalArgumentException("Column properties already set");
      }
      this.columnPropertyMap = columnPropertyMap;
      return this;
    }
    
    /**
     * Set a key retriever callback. Its also possible to
     * set explicit footer or column keys on this property object. Upon file decryption, 
     * availability of explicit keys is checked before invocation of the retriever callback.
     * If an explicit key is available for a footer or a column, its key metadata will
     * be ignored. 
     * @param keyRetriever
     */
    public Builder withKeyRetriever(DecryptionKeyRetriever keyRetriever) {
      if (null == keyRetriever) {
        return this;
      }
      if (null != this.keyRetriever) {
        throw new IllegalArgumentException("Key retriever already set");
      }
      this.keyRetriever = keyRetriever;
      return this;
    }
    
    /**
     * Set the AES-GCM additional authenticated data (AAD).
     * @param aad
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
     * Set an AAD retrieval callback.
     * @param aadRetriever
     */
    public Builder withAADRetriever(AADRetriever aadRetriever) {
      if (null == aadRetriever) {
        return this;
      }
      if (null != this.aadRetriever) {
        throw new IllegalArgumentException("AAD retriever already set");
      }
      this.aadRetriever = aadRetriever;
      return this;
    }
    
    public FileDecryptionProperties build() {
      return new FileDecryptionProperties(footerKeyBytes, keyRetriever, aadRetriever, aadBytes, columnPropertyMap);
    }
  }
  
  public byte[] getFooterKey() {
    return footerKeyBytes;
  }
  
  public byte[] getColumnKey(ColumnPath path) {
    if (null == columnPropertyMap) return null;
    ColumnDecryptionProperties columnDecryptionProperties = columnPropertyMap.get(path);
    if (null == columnDecryptionProperties) return null;
    return columnDecryptionProperties.getKeyBytes();
  }

  public DecryptionKeyRetriever getKeyRetriever() {
    return keyRetriever;
  }

  public byte[] getAAD() {
    return aadBytes;
  }

  public AADRetriever getAADRetriever() {
    return aadRetriever;
  }
}
