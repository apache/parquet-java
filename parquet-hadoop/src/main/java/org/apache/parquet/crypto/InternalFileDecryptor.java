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
  private final byte[] aadPrefixInProperties;
  private final AADPrefixVerifier aadPrefixVerifier;

  private byte[] footerKey;
  private HashMap<ColumnPath, InternalColumnDecryptionSetup> columnMap;
  private EncryptionAlgorithm algorithm;
  private byte[] fileAAD;
  private boolean encryptedFooter;
  private byte[] footerKeyMetaData;
  private boolean fileCryptoMetaDataProcessed = false;
  private BlockCipher.Decryptor aesGcmDecryptorWithFooterKey;
  private BlockCipher.Decryptor aesCtrDecryptorWithFooterKey;
  private boolean plaintextFile;
  private LinkedList<AesCipher> allDecryptors;
  private boolean wipedOut;

  public InternalFileDecryptor(FileDecryptionProperties fileDecryptionProperties) throws IOException {

    if (fileDecryptionProperties.isUtilized()) {
      throw new IOException("Re-using decryption properties with explicit keys for another file");
    }
    fileDecryptionProperties.setUtilized();
    this.fileDecryptionProperties= fileDecryptionProperties;
    checkPlaintextFooterIntegrity = fileDecryptionProperties.checkFooterIntegrity();
    footerKey = fileDecryptionProperties.getFooterKey();
    keyRetriever = fileDecryptionProperties.getKeyRetriever();
    aadPrefixInProperties = fileDecryptionProperties.getAADPrefix();
    columnMap = new HashMap<ColumnPath, InternalColumnDecryptionSetup>();
    this.aadPrefixVerifier = fileDecryptionProperties.getAADPrefixVerifier();
    this.plaintextFile = false;
    allDecryptors = new LinkedList<AesCipher>();
    wipedOut = false;
  }
  
  private BlockCipher.Decryptor createDecryptor(AesMode mode, byte[] key) throws IllegalArgumentException, IOException {
    BlockCipher.Decryptor decryptor = ModuleCipherFactory.getDecryptor(mode, key);
    allDecryptors.add((AesCipher)decryptor);
    return decryptor;
  }

  private BlockCipher.Decryptor getThriftModuleDecryptor(byte[] columnKey) throws IOException {
    if (null == columnKey) { // Decryptor with footer key
      if (null == aesGcmDecryptorWithFooterKey) {
        aesGcmDecryptorWithFooterKey = createDecryptor(AesMode.GCM, footerKey);
      }
      return aesGcmDecryptorWithFooterKey;
    } else { // Decryptor with column key
      return createDecryptor(AesMode.GCM, columnKey);
    }
  }

  private BlockCipher.Decryptor getDataModuleDecryptor(byte[] columnKey) throws IOException {
    if (algorithm.isSetAES_GCM_V1()) {
      return getThriftModuleDecryptor(columnKey);
    }
    // AES_GCM_CTR_V1
    if (null == columnKey) { // Decryptor with footer key
      if (null == aesCtrDecryptorWithFooterKey) {
        aesCtrDecryptorWithFooterKey = createDecryptor(AesMode.CTR, footerKey);
      }
      return aesCtrDecryptorWithFooterKey;
    } else { // Decryptor with column key
      return createDecryptor(AesMode.CTR, columnKey);
    }
  }

  public InternalColumnDecryptionSetup getColumnSetup(ColumnPath path) throws IOException {
    if (!fileCryptoMetaDataProcessed) {
      throw new IOException("Haven't parsed the file crypto metadata yet");
    }
    if (wipedOut) {
      throw new IOException("File decryptor is wiped out");
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
    if (wipedOut) {
      throw new IOException("File decryptor is wiped out");
    }
    if (!encryptedFooter) return null;
    return getThriftModuleDecryptor(null);
  }

  public void setFileCryptoMetaData(EncryptionAlgorithm algorithm, 
      boolean encryptedFooter, byte[] footerKeyMetaData) throws IOException {

    if (wipedOut) {
      throw new IOException("File decryptor is wiped out");
    }

    // first use of the decryptor
    if (!fileCryptoMetaDataProcessed) {
      fileCryptoMetaDataProcessed = true;
      this.encryptedFooter = encryptedFooter;
      this.algorithm = algorithm;
      this.footerKeyMetaData = footerKeyMetaData;

      byte[] aadFileUnique;
      boolean mustSupplyAadPrefix;
      boolean fileHasAadPrefix = false;
      byte[] aadPrefixInFile = null;

      // Process encryption algorithm metadata
      if (algorithm.isSetAES_GCM_V1()) {
        if (algorithm.getAES_GCM_V1().isSetAad_prefix()) {
          fileHasAadPrefix = true;
          aadPrefixInFile = algorithm.getAES_GCM_V1().getAad_prefix();
        }
        mustSupplyAadPrefix = algorithm.getAES_GCM_V1().isSupply_aad_prefix();
        aadFileUnique = algorithm.getAES_GCM_V1().getAad_file_unique();
      } else if (algorithm.isSetAES_GCM_CTR_V1()) {
        if (algorithm.getAES_GCM_CTR_V1().isSetAad_prefix()) {
          fileHasAadPrefix = true;
          aadPrefixInFile = algorithm.getAES_GCM_CTR_V1().getAad_prefix();
        }
        mustSupplyAadPrefix = algorithm.getAES_GCM_CTR_V1().isSupply_aad_prefix();
        aadFileUnique = algorithm.getAES_GCM_CTR_V1().getAad_file_unique();
      } else {
        throw new IOException("Unsupported algorithm: " + algorithm);
      }

      // Handle AAD prefix
      byte[] aadPrefix = aadPrefixInProperties;
      if (mustSupplyAadPrefix && (null == aadPrefixInProperties)) {
        throw new IOException("AAD prefix used for file encryption, but not stored in file and not supplied in decryption properties");
      }
      if (fileHasAadPrefix) {
        if (null != aadPrefixInProperties) {
          if (!Arrays.equals(aadPrefixInProperties, aadPrefixInFile)) {
            throw new IOException("AAD Prefix in file and in decryption properties is not the same");
          }
        }
        if (null != aadPrefixVerifier) {
          aadPrefixVerifier.verify(aadPrefixInFile);
        }
        aadPrefix = aadPrefixInFile;
      }
      else {
        if (!mustSupplyAadPrefix && (null != aadPrefixInProperties)) {
          throw new IOException("AAD Prefix set in decryption properties, but was not used for file encryption");
        }
        if (null != aadPrefixVerifier) {
          throw new IOException("AAD Prefix Verifier is set, but AAD Prefix not found in file");
        }
      }
      if (null == aadPrefix) {
        this.fileAAD = aadFileUnique;
      } else {
        this.fileAAD = AesCipher.concatByteArrays(aadPrefix, aadFileUnique);
      }

      // Get footer key
      if (null == footerKey) { // ignore footer key metadata if footer key is explicitly set via API
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
    } else {
      // re-use of the decryptor (for the same file, to save footer key KMS interaction). 
      // check the crypto metadata.
      if (!this.algorithm.equals(algorithm)) {
        throw new IOException("Decryptor re-use: Different algorithm");
      }
      if (encryptedFooter != this.encryptedFooter) {
        throw new IOException("Decryptor re-use: Different footer encryption");
      }
      if (!Arrays.equals(this.footerKeyMetaData, footerKeyMetaData)) {
        throw new IOException("Decryptor re-use: Different footer key metadata ");
      }
    }
  }

  public InternalColumnDecryptionSetup setColumnCryptoMetadata(ColumnPath path, boolean encrypted, 
      boolean encryptedWithFooterKey, byte[] keyMetadata, short columnOrdinal) throws IOException {

    if (!fileCryptoMetaDataProcessed) {
      throw new IOException("Haven't parsed the file crypto metadata yet");
    }
    if (wipedOut) {
      throw new IOException("File decryptor is wiped out");
    }

    InternalColumnDecryptionSetup columnDecryptionSetup = columnMap.get(path);
    if (null != columnDecryptionSetup) {
      if (columnDecryptionSetup.isEncrypted() != encrypted) {
        throw new IOException("Re-use: wrong encrypted flag. Column: " + path);
      }
      if (encrypted) {
        if (encryptedWithFooterKey != columnDecryptionSetup.isEncryptedWithFooterKey()) {
          throw new IOException("Re-use: wrong encryption key (column vs footer). Column: " + path);
        }
        if (!encryptedWithFooterKey && !Arrays.equals(columnDecryptionSetup.getKeyMetadata(), keyMetadata)) {
          throw new IOException("Decryptor re-use: Different footer key metadata ");
        }
      }
      return columnDecryptionSetup;
    }

    if (!encrypted) {
      columnDecryptionSetup = new InternalColumnDecryptionSetup(path, false,  false, null, null, columnOrdinal, null);
    } else {
      if (encryptedWithFooterKey) {
        if (null == footerKey) {
          throw new IOException("Column " + path + " is encrypted with NULL footer key");
        }
        columnDecryptionSetup = new InternalColumnDecryptionSetup(path, true, true, 
            getDataModuleDecryptor(null), getThriftModuleDecryptor(null), columnOrdinal, null);
      } else {
        // Column is encrypted with column-specific key
        byte[] columnKeyBytes = fileDecryptionProperties.getColumnKey(path);
        if ((null == columnKeyBytes) && (null != keyMetadata) && (null != keyRetriever)) {
          // No explicit column key given via API. Retrieve via key metadata.
          try {
            columnKeyBytes = keyRetriever.getKey(keyMetadata);
          } 
          catch (KeyAccessDeniedException e) {
            throw new IOException("Column " + path + ": key access denied", e);
          }
        }

        if (null == columnKeyBytes) { // Hidden column: encrypted, but key unavailable
          throw new IOException("Column " + path + ": key unavailable");
        } else { // Key is available
          columnDecryptionSetup = new InternalColumnDecryptionSetup(path, true, false, 
              getDataModuleDecryptor(columnKeyBytes), getThriftModuleDecryptor(columnKeyBytes), columnOrdinal, keyMetadata);
        }
      }
    }
    columnMap.put(path, columnDecryptionSetup);
    return columnDecryptionSetup;
  }

  public byte[] getFileAAD() {
    return this.fileAAD;
  }

  public AesGcmEncryptor getSignedFooterEncryptor() throws IOException  {
    if (!fileCryptoMetaDataProcessed) {
      throw new IOException("Haven't parsed the file crypto metadata yet");
    }
    if (wipedOut) {
      throw new IOException("File decryptor is wiped out");
    }
    if (encryptedFooter) {
      throw new IOException("Requesting signed footer encryptor in file with encrypted footer");
    }
    return (AesGcmEncryptor) ModuleCipherFactory.getEncryptor(AesMode.GCM, footerKey);
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

  public void wipeOutDecryptionKeys() {
    wipedOut = true;
    fileDecryptionProperties.wipeOutDecryptionKeys();
    for (AesCipher decryptor : allDecryptors) {
      decryptor.wipeOut();
    }
  }

  public boolean isWipedOut() {
    return wipedOut;
  }
}

