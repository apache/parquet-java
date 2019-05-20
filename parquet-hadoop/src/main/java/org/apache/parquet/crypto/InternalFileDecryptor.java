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


import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.EncryptionAlgorithm;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;


public class InternalFileDecryptor {
  
  private final FileDecryptionProperties fileDecryptionProperties;
  private final DecryptionKeyRetriever keyRetriever;
  private final boolean checkPlaintextFooterIntegrity;
  
  private byte[] footerKey;
  private HashMap<ColumnPath, InternalColumnDecryptionSetup> columnMap;
  private EncryptionAlgorithm algorithm;
  private byte[] aadPrefix;
  private AADPrefixVerifier aadPrefixVerifier;
  private byte[] fileAAD;
  private boolean encryptedFooter;
  private boolean fileCryptoMetaDataProcessed = false;
  private boolean allColumnCryptoMetaDataProcessed = false;
  private BlockCipher.Decryptor aesGcmDecryptorWithFooterKey;
  private BlockCipher.Decryptor aesCtrDecryptorWithFooterKey;
  private boolean plaintextFile;
  private LinkedList<AesDecryptor> allDecryptors;

  public InternalFileDecryptor(FileDecryptionProperties fileDecryptionProperties) throws IOException {
    
    if (fileDecryptionProperties.isUtilized()) {
      throw new IOException("Re-using decryption properties with explicit keys for another file");
    }
    fileDecryptionProperties.setUtilized();
    this.fileDecryptionProperties= fileDecryptionProperties;
    checkPlaintextFooterIntegrity = fileDecryptionProperties.checkFooterIntegrity();
    footerKey = fileDecryptionProperties.getFooterKey();
    keyRetriever = fileDecryptionProperties.getKeyRetriever();
    aadPrefix = fileDecryptionProperties.getAADPrefix();
    columnMap = new HashMap<ColumnPath, InternalColumnDecryptionSetup>();
    this.aadPrefixVerifier = fileDecryptionProperties.getAADPrefixVerifier();
    this.plaintextFile = false;
    allDecryptors = new LinkedList<AesDecryptor>();
  }
  
  private BlockCipher.Decryptor getThriftModuleDecryptor(byte[] columnKey) throws IOException {
    if (null == columnKey) { // Decryptor with footer key
      if (null == aesGcmDecryptorWithFooterKey) {
        aesGcmDecryptorWithFooterKey = new AesDecryptor(AesEncryptor.Mode.GCM, footerKey, allDecryptors);
      }
      return aesGcmDecryptorWithFooterKey;
    }
    else { // Decryptor with column key
      return new AesDecryptor(AesEncryptor.Mode.GCM, columnKey, allDecryptors);
    }
  }
  
  private BlockCipher.Decryptor getDataModuleDecryptor(byte[] columnKey) throws IOException {
    if (algorithm.isSetAES_GCM_V1()) {
      return getThriftModuleDecryptor(columnKey);
    }
    // AES_GCM_CTR_V1
    if (null == columnKey) { // Decryptor with footer key
      if (null == aesCtrDecryptorWithFooterKey) {
        aesCtrDecryptorWithFooterKey = new AesDecryptor(AesEncryptor.Mode.CTR, footerKey, allDecryptors);
      }
      return aesCtrDecryptorWithFooterKey;
    }
    else { // Decryptor with column key
      return new AesDecryptor(AesEncryptor.Mode.CTR, columnKey, allDecryptors);
    }
  }

  public InternalColumnDecryptionSetup getColumnSetup(ColumnPath path) throws IOException {
    if (!fileCryptoMetaDataProcessed) {
      throw new IOException("Haven't parsed the file crypto metadata yet");
    }
    InternalColumnDecryptionSetup columnDecryptionSetup = columnMap.get(path);
    if (null == columnDecryptionSetup) {
      throw new IOException("Failed to find decryption setup for column " + path);
    }
    return columnDecryptionSetup;
  }

  public BlockCipher.Decryptor getFooterDecryptor() throws IOException {
    if (!fileCryptoMetaDataProcessed) {
      throw new IOException("Haven't parsed the file crypto metadata yet");
    }
    if (!encryptedFooter) return null;
    return getThriftModuleDecryptor(null);
  }

  public void setFileCryptoMetaData(EncryptionAlgorithm algorithm, 
      boolean encryptedFooter, byte[] footerKeyMetaData) throws IOException {
    // first use of the decryptor
    if (!fileCryptoMetaDataProcessed) {
      fileCryptoMetaDataProcessed = true;
      this.encryptedFooter = encryptedFooter;
      this.algorithm = algorithm;
      byte[] aadFileUnique;
      
      if (algorithm.isSetAES_GCM_V1()) {
        if (algorithm.getAES_GCM_V1().isSetAad_prefix()) {
          if (null != aadPrefix) {
            if (!Arrays.equals(aadPrefix, algorithm.getAES_GCM_V1().getAad_prefix())) {
              throw new IOException("ADD Prefix in file and in properties is not the same");
            }
          }
          aadPrefix = algorithm.getAES_GCM_V1().getAad_prefix();
          if (null != aadPrefixVerifier) {
            aadPrefixVerifier.check(aadPrefix);
          }
        }
        if (algorithm.getAES_GCM_V1().isSupply_aad_prefix() && (null == aadPrefix)) {
          throw new IOException("AAD prefix used for file encryption, but not stored in file and not supplied in decryption properties");
        }
        aadFileUnique = algorithm.getAES_GCM_V1().getAad_file_unique();
      }
      else if (algorithm.isSetAES_GCM_CTR_V1()) {
        if (algorithm.getAES_GCM_CTR_V1().isSetAad_prefix()) {
          if (null != aadPrefix) {
            if (!Arrays.equals(aadPrefix, algorithm.getAES_GCM_CTR_V1().getAad_prefix())) {
              throw new IOException("ADD Prefix in file and in properties is not the same");
            }
          }
          aadPrefix = algorithm.getAES_GCM_CTR_V1().getAad_prefix();
          if (null != aadPrefixVerifier) {
            aadPrefixVerifier.check(aadPrefix);
          }
        }
        if (algorithm.getAES_GCM_CTR_V1().isSupply_aad_prefix() && (null == aadPrefix)) {
          throw new IOException("AAD prefix used for file encryption, but not stored in file and not supplied in decryption properties");
        }
        aadFileUnique = algorithm.getAES_GCM_CTR_V1().getAad_file_unique();
      }
      else {
        throw new IOException("Unsupported algorithm: " + algorithm);
      }
      
 
      // ignore footer key metadata if footer key is explicitly set via API
      if (null == footerKey) {
        if (encryptedFooter || checkPlaintextFooterIntegrity) {
          if (null == footerKeyMetaData) throw new IOException("No footer key or key metadata");
          if (null == keyRetriever) throw new IOException("No footer key or key retriever");
          try {
            footerKey = keyRetriever.getKey(footerKeyMetaData);
          } 
          catch (KeyAccessDeniedException e) {
            throw new IOException("Footer key: access denied", e);
          }
          if (null == footerKey) {
            throw new IOException("Footer key unavailable");
          }
        }
      }
      
      if (null == aadPrefix) {
        this.fileAAD = aadFileUnique;
      }
      else {
        this.fileAAD = AesEncryptor.concatByteArrays(aadPrefix, aadFileUnique);
      }
    }
    // re-use of the decryptor. compare the crypto metadata.
    else {
      if (!this.algorithm.equals(algorithm)) {
        throw new IOException("Decryptor re-use: Different algorithm");
      }
      if (encryptedFooter != this.encryptedFooter) {
        throw new IOException("Decryptor re-use: Different footer encryption");
      }
      // TODO check other fields?
    }
  }

  public InternalColumnDecryptionSetup setColumnCryptoMetadata(ColumnPath path, boolean encrypted, 
      boolean encryptedWithFooterKey, byte[] keyMetadata, short columnOrdinal) throws IOException {
    
    if (!fileCryptoMetaDataProcessed) {
      throw new IOException("Haven't parsed the file crypto metadata yet");
    }
    InternalColumnDecryptionSetup columnDecryptionSetup = columnMap.get(path);
    if (allColumnCryptoMetaDataProcessed && (null == columnDecryptionSetup)) {
      throw new IOException("Re-use with unknown column: " + path);
    }
    if (null != columnDecryptionSetup) {
      if (!allColumnCryptoMetaDataProcessed) {
        throw new IOException("File with identical columns: " + path);
      }
      if (columnDecryptionSetup.isEncrypted() != encrypted) {
        throw new IOException("Re-use: wrong encrypted flag. Column: " + path);
      }
      if (encrypted && (encryptedWithFooterKey != columnDecryptionSetup.isEncryptedWithFooterKey())) {
        throw new IOException("Re-use: wrong encryption key (column vs footer). Column: " + path);
      }
      return columnDecryptionSetup;
    }
    
    if (!encrypted) {
      columnDecryptionSetup = new InternalColumnDecryptionSetup(path, false, false,  false, null, null, columnOrdinal);
    }
    else {
      if (encryptedWithFooterKey) {
        if (null == footerKey) {
          throw new IOException("Column " + path + " is encrypted with NULL footer key");
        }
        columnDecryptionSetup = new InternalColumnDecryptionSetup(path, true, true, true, 
            getDataModuleDecryptor(null), getThriftModuleDecryptor(null), columnOrdinal);
      }
      else {
        // Column is encrypted with column-specific key
        byte[] columnKeyBytes = fileDecryptionProperties.getColumnKey(path);
        if ((null == columnKeyBytes) && (null != keyMetadata) && (null != keyRetriever)) {
          // No explicit column key given via API. Retrieve via key metadata.
          try {
            columnKeyBytes = keyRetriever.getKey(keyMetadata);
          } 
          catch (KeyAccessDeniedException e) {
            // Hidden column: encrypted, but key unavailable
            columnKeyBytes = null;
          }
        }

        if (null == columnKeyBytes) { // Hidden column: encrypted, but key unavailable
          columnDecryptionSetup = new InternalColumnDecryptionSetup(path, true, false,  false, null, null, columnOrdinal);
        }
        else { // Key is available
          columnDecryptionSetup = new InternalColumnDecryptionSetup(path, true, true, false, 
              getDataModuleDecryptor(columnKeyBytes), getThriftModuleDecryptor(columnKeyBytes), columnOrdinal);
        }
      }
    }
    columnMap.put(path, columnDecryptionSetup);
    return columnDecryptionSetup;
  }

  public void allColumnCryptoMetaDataProcessed() {
    allColumnCryptoMetaDataProcessed = true;
  }
  
  public byte[] getFileAAD() {
    return this.fileAAD;
  }
  
  public AesEncryptor getSignedFooterEncryptor() throws IOException  {
    if (!fileCryptoMetaDataProcessed) {
      throw new IOException("Haven't parsed the file crypto metadata yet");
    }
    if (encryptedFooter) {
      throw new IOException("Requesting signed footer encryptor in file with encrypted footer");
    }
    if (null == footerKey) throw new IOException("Footer key unavailable");
    return new AesEncryptor(AesEncryptor.Mode.GCM, footerKey, null);
  }

  public boolean checkFooterIntegrity() {
    return checkPlaintextFooterIntegrity;
  }

  public boolean plaintextFilesAllowed() {
    return fileDecryptionProperties.plaintextFilesAllowed();
  }

  public void setPlaintextFile() {
    plaintextFile = true;
  }

  public boolean plaintextFile() {
    return plaintextFile;
  }
  
  public void wipeOutDecryptionKeys() throws IOException {
    fileDecryptionProperties.wipeOutDecryptionKeys();
    for (AesDecryptor decryptor : allDecryptors) {
      decryptor.wipeOut();
    }
  }
}

