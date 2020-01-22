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
import java.util.HashMap;
import java.util.Map;

import org.apache.parquet.hadoop.metadata.ColumnPath;

public class FileDecryptionProperties {

  private static final boolean CHECK_SIGNATURE = true;
  private static final boolean ALLOW_PLAINTEXT_FILES = false;

  private final byte[] footerKey;
  private final DecryptionKeyRetriever keyRetriever;
  private final byte[] aadPrefix;
  private final AADPrefixVerifier aadPrefixVerifier;
  private final Map<ColumnPath, ColumnDecryptionProperties> columnPropertyMap;
  private final boolean checkPlaintextFooterIntegrity;
  private final boolean allowPlaintextFiles;

  private boolean utilized;

  private FileDecryptionProperties(byte[] footerKey, DecryptionKeyRetriever keyRetriever,
      boolean checkPlaintextFooterIntegrity,  byte[] aadPrefix, AADPrefixVerifier aadPrefixVerifier,
      Map<ColumnPath, ColumnDecryptionProperties> columnPropertyMap, boolean allowPlaintextFiles) {

    if ((null == footerKey) && (null == keyRetriever) && (null == columnPropertyMap)) {
      throw new IllegalArgumentException("No decryption properties are specified");
    }
    if ((null != footerKey) && 
        !(footerKey.length == 16 || footerKey.length == 24 || footerKey.length == 32)) {
      throw new IllegalArgumentException("Wrong footer key length " + footerKey.length);
    }
    if ((null == footerKey) && checkPlaintextFooterIntegrity && (null == keyRetriever)) {
      throw new IllegalArgumentException("Can't check footer integrity with null footer key and null key retriever");
    }

    this.footerKey = footerKey;
    this.checkPlaintextFooterIntegrity = checkPlaintextFooterIntegrity;
    this.keyRetriever = keyRetriever;
    this.aadPrefix = aadPrefix;
    this.columnPropertyMap = columnPropertyMap;
    this.aadPrefixVerifier = aadPrefixVerifier;
    this.allowPlaintextFiles = allowPlaintextFiles;
    this.utilized = false;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private byte[] footerKeyBytes;
    private DecryptionKeyRetriever keyRetriever;
    private byte[] aadPrefixBytes;
    private AADPrefixVerifier aadPrefixVerifier;
    private Map<ColumnPath, ColumnDecryptionProperties> columnPropertyMap;
    private boolean checkPlaintextFooterIntegrity;
    private boolean plaintextFilesAllowed;

    private Builder() {
      this.checkPlaintextFooterIntegrity = CHECK_SIGNATURE;
      this.plaintextFilesAllowed = ALLOW_PLAINTEXT_FILES;
    }

    /**
     * Set an explicit footer key. If applied on a file that contains footer key metadata - 
     * the metadata will be ignored, the footer will be decrypted/verified with this key.
     * If explicit key is not set, footer key will be fetched from key retriever.
     * 
     * With explicit keys or AAD prefix, new encryption properties object must be created for each encrypted file.
     * Explicit encryption keys (footer and column) are cloned.
     * Upon completion of file reading, the cloned encryption keys in the properties will 
     * be wiped out (array values set to 0).
     * Caller is responsible for wiping out the input key array. 
     * 
     * @param footerKey Key length must be either 16, 24 or 32 bytes.
     * @return Builder 
     */
    public Builder withFooterKey(byte[] footerKey) {
      if (null == footerKey) {
        return this;
      }
      if (null != this.footerKeyBytes) {
        throw new IllegalArgumentException("Footer key already set");
      }
      this.footerKeyBytes = new byte[footerKey.length];
      System.arraycopy(footerKey, 0, this.footerKeyBytes, 0, footerKey.length);
      return this;
    }

    /**
     * Set explicit column keys (decryption properties).
     * Its also possible to set a key retriever on this file decryption properties object. 
     * Upon reading, availability of explicit keys is checked before invocation of the retriever callback.
     * If an explicit key is available for a footer or a column, its key metadata will be ignored.
     * 
     * @param columnProperties Explicit column decryption keys
     * @return Builder
     */
    public Builder withColumnKeys(Map<ColumnPath, ColumnDecryptionProperties> columnProperties) {
      if (null == columnProperties) {
        return this;
      }
      if (null != this.columnPropertyMap) {
        throw new IllegalArgumentException("Column properties already set");
      }
      for (Map.Entry<ColumnPath, ColumnDecryptionProperties> entry : columnProperties.entrySet()) {
        if(entry.getValue().isUtilized()) {
          throw new IllegalArgumentException("Column properties re-used in another file");
        }
        entry.getValue().setUtilized();
      }
      // Copy the map to make column properties immutable
      this.columnPropertyMap = new HashMap<ColumnPath, ColumnDecryptionProperties>(columnProperties);
      return this;
    }

    /**
     * Set a key retriever callback. It is also possible to
     * set explicit footer or column keys on this file property object. Upon file decryption, 
     * availability of explicit keys is checked before invocation of the retriever callback.
     * If an explicit key is available for a footer or a column, its key metadata will
     * be ignored. 
     * 
     * @param keyRetriever Key retriever object
     * @return Builder
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
     * Skip integrity verification of plaintext footers.
     * If not called, integrity of plaintext footers will be checked in runtime, and an exception will 
     * be thrown in the following situations:
     * - footer signing key is not available (not passed, or not found by key retriever)
     * - footer content doesn't match the signature
     * 
     * @return Builder
     */
    public Builder withoutFooterSignatureVerification() {
      this.checkPlaintextFooterIntegrity = false;
      return this;
    }


    /**
     * Explicitly supply the file AAD prefix.
     * A must when a prefix is used for file encryption, but not stored in file.
     * If AAD prefix is stored in file, it will be compared to the explicitly supplied value 
     * and an exception will be thrown if they differ.
     * 
     * @param aadPrefixBytes AAD Prefix
     * @return Builder
     */
    public Builder withAADPrefix(byte[] aadPrefixBytes) {
      if (null == aadPrefixBytes) {
        return this;
      }
      if (null != this.aadPrefixBytes) {
        throw new IllegalArgumentException("AAD Prefix already set");
      }
      this.aadPrefixBytes = aadPrefixBytes;
      return this;
    }

    /**
     * Set callback for verification of AAD Prefixes stored in file.
     * 
     * @param aadPrefixVerifier AAD prefix verification object
     * @return Builder
     */
    public Builder withAADPrefixVerifier(AADPrefixVerifier aadPrefixVerifier) {
      if (null == aadPrefixVerifier) {
        return this;
      }
      if (null != this.aadPrefixVerifier) {
        throw new IllegalArgumentException("AAD Prefix verifier already set");
      }
      this.aadPrefixVerifier = aadPrefixVerifier;
      return this;
    }

    /**
     * By default, reading plaintext (unencrypted) files is not allowed when using a decryptor 
     * - in order to detect files that were not encrypted by mistake. 
     * However, the default behavior can be overriden by calling this method.
     * The caller should use then a different method to ensure encryption of files with sensitive data.
     * 
     * @return Builder
     */
    public Builder withPlaintextFilesAllowed() {
      this.plaintextFilesAllowed  = true;
      return this;
    }

    public FileDecryptionProperties build() {
      return new FileDecryptionProperties(footerKeyBytes, keyRetriever, checkPlaintextFooterIntegrity, 
          aadPrefixBytes, aadPrefixVerifier, columnPropertyMap, plaintextFilesAllowed);
    }
  }

  public byte[] getFooterKey() {
    return footerKey;
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

  public byte[] getAADPrefix() {
    return aadPrefix;
  }

  public boolean checkFooterIntegrity() {
    return checkPlaintextFooterIntegrity;
  }

  boolean plaintextFilesAllowed() {
    return allowPlaintextFiles;
  }

  AADPrefixVerifier getAADPrefixVerifier() {
    return aadPrefixVerifier;
  }

  void wipeOutDecryptionKeys() {
    if (null != footerKey) Arrays.fill(footerKey, (byte)0);

    if (null != columnPropertyMap) {
      for (Map.Entry<ColumnPath, ColumnDecryptionProperties> entry : columnPropertyMap.entrySet()) {
        entry.getValue().wipeOutDecryptionKey();
      }
    }
  }

  boolean isUtilized() {
    // can be re-used if no explicit keys and no AAD prefix are specified
    if (null == footerKey && null == columnPropertyMap && null == aadPrefix) return false;

    return utilized;
  }

  void setUtilized() {
    utilized = true;
  }

  /** 
   * DecryptionProperties object can be used for reading one file only.
   * (unless this object keeps the keyRetrieval callback only, and no explicit keys or aadPrefix).
   * At the end, keys are wiped out in the memory.
   * This method allows to clone identical properties for another file, 
   * with an option to update the AAD Prefix (if newAadPrefix is null, 
   * aadPrefix will be cloned too) 
   * 
   * @param newAadPrefix AAD prefix
   * @return Cloned properties
   */
  public FileDecryptionProperties deepClone(byte[] newAadPrefix) {

    byte[] footerKeyBytes = (null == footerKey?null:footerKey.clone());
    Map<ColumnPath, ColumnDecryptionProperties> columnProps = null;
    if (null != columnPropertyMap) {
      columnProps = new HashMap<ColumnPath, ColumnDecryptionProperties>();
      for (Map.Entry<ColumnPath, ColumnDecryptionProperties> entry : columnPropertyMap.entrySet()) {
        columnProps.put(entry.getKey(), entry.getValue().deepClone());
      }
    }

    if (null == newAadPrefix) newAadPrefix = aadPrefix;

    return new FileDecryptionProperties(footerKeyBytes, keyRetriever,
        checkPlaintextFooterIntegrity,  newAadPrefix, aadPrefixVerifier,
        columnProps, allowPlaintextFiles);
  }
}
