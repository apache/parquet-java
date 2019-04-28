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


package org.apache.parquet.crypto.keytools;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

import org.apache.parquet.crypto.AesDecryptor;
import org.apache.parquet.crypto.AesEncryptor;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.crypto.KeyAccessDeniedException;


public class WrappedKeyManager {

  private final KmsClient kmsClient;
  private final boolean wrapLocally;
  private final WrappedKeyStore wrappedKeyStore;
  private final String fileID;

  private final SecureRandom random;
  private short keyCounter;

  public static class WrappedKeyRetriever implements DecryptionKeyRetriever {
    private final KmsClient kmsClient;
    private final boolean unwrapLocally;
    private final WrappedKeyStore keyStore;
    private final String fileID;

    private WrappedKeyRetriever(KmsClient kmsClient, boolean unwrapLocally, WrappedKeyStore keyStore, String fileID) {
      this.kmsClient = kmsClient;
      this.keyStore = keyStore;
      this.fileID = fileID;
      this.unwrapLocally = unwrapLocally;
    }

    public byte[] getKey(byte[] keyMetaData) throws IOException, KeyAccessDeniedException {
      String keyMaterial;
      if (null != keyStore) {
        String keyIDinFile = new String(keyMetaData, StandardCharsets.UTF_8);
        keyMaterial = keyStore.getWrappedKey(fileID, keyIDinFile);
      }
      else {
        keyMaterial = new String(keyMetaData, StandardCharsets.UTF_8);
      }
      String[] parts = keyMaterial.split(":");
      if (parts.length != 2) throw new IOException("Wrong key material structure: " + keyMaterial);
      String encodedWrappedDatakey = parts[0];
      String masterKeyID = parts[1];
      byte[] dataKey = null;
      if (unwrapLocally) {
        byte[] wrappedDataKey = Base64.getDecoder().decode(encodedWrappedDatakey);
        String encodedMasterKey = null;
        try {
          encodedMasterKey = kmsClient.getKeyFromServer(masterKeyID);
        }
        catch (UnsupportedOperationException e) {
          throw new IOException("KMS client doesnt support key fetching", e);
        }
        if (null == encodedMasterKey) {
          throw new IOException("Failed to get from KMS the master key " + masterKeyID);
        }
        byte[] masterKey = Base64.getDecoder().decode(encodedMasterKey);
        AesDecryptor keyDecryptor = new AesDecryptor(AesEncryptor.Mode.GCM, masterKey);
        byte[] AAD = masterKeyID.getBytes(StandardCharsets.UTF_8);
        dataKey = keyDecryptor.decrypt(wrappedDataKey, 0, wrappedDataKey.length, AAD);
      }
      else {
        String encodedDataKey = null;
        try {
          encodedDataKey = kmsClient.unwrapDataKeyInServer(encodedWrappedDatakey, masterKeyID);
        }
        catch (UnsupportedOperationException e) {
          throw new IOException("KMS client doesnt support key wrapping", e);
        }
        if (null == encodedDataKey) {
          throw new IOException("Failed to unwrap in KMS with master key " + masterKeyID);
        }
        dataKey = Base64.getDecoder().decode(encodedDataKey);
      }
      return dataKey;
    }
  }
  
  public WrappedKeyManager(KmsClient kmsClient) {
    this(kmsClient, !kmsClient.supportsServerSideWrapping(), null, null);
  }

  public WrappedKeyManager(KmsClient kmsClient, boolean wrapLocally, WrappedKeyStore wrappedKeyStore, String fileID) {
    if (!wrapLocally && !kmsClient.supportsServerSideWrapping()) {
      throw new UnsupportedOperationException("KMS client doesn't support server-side wrapping");
    }
    if (null != wrappedKeyStore && null == fileID) {
      throw new IllegalArgumentException("File ID must be supplied to wrapped key store");
    }
    this.kmsClient = kmsClient;
    this.wrapLocally = wrapLocally;
    this.wrappedKeyStore = wrappedKeyStore;
    this.fileID = fileID;
    random = new SecureRandom();
    keyCounter = 0;
  }

  /**
   * Generates random data encryption key, and creates its metadata.
   * The metadata is comprised of the wrapped data key (encrypted with master key), and the identity of the master key.
   * @param masterKeyID
   * @return
   * @throws IOException
   */
  public ParquetKey generateKey(String masterKeyID) throws IOException {
    byte[] dataKey = new byte[16]; //TODO
    random.nextBytes(dataKey);
    String encodedWrappedDataKey = null;
    if (wrapLocally) {
      String encodedMasterKey;
      try {
        encodedMasterKey = kmsClient.getKeyFromServer(masterKeyID);
      } 
      catch (KeyAccessDeniedException e) {
        throw new IOException("Unauthorized to fetch key: " + masterKeyID, e);
      } 
      catch (UnsupportedOperationException e) {
        throw new IOException("KMS client doesnt support key fetching", e);
      }
      byte[] masterKey = Base64.getDecoder().decode(encodedMasterKey);
      AesEncryptor keyEncryptor = new AesEncryptor(AesEncryptor.Mode.GCM, masterKey);
      byte[] AAD = masterKeyID.getBytes(StandardCharsets.UTF_8);
      byte[] wrappedDataKey = keyEncryptor.encrypt(false, dataKey, AAD);
      encodedWrappedDataKey = Base64.getEncoder().encodeToString(wrappedDataKey);
    }
    else {
      String encodedDataKey = Base64.getEncoder().encodeToString(dataKey);
      try {
        encodedWrappedDataKey = kmsClient.wrapDataKeyInServer(encodedDataKey, masterKeyID);
      } 
      catch (KeyAccessDeniedException e) {
        throw new IOException("Unauthorized to wrap with master key: " + masterKeyID, e);
      } 
      catch (UnsupportedOperationException e) {
        throw new IOException("KMS client doesnt support key wrapping", e);
      }
    }
    String wrappedKeyMaterial = encodedWrappedDataKey + ":" + masterKeyID;
    byte[] keyMetadata = null;
    if (null != wrappedKeyStore) {
      String keyName = "k" + keyCounter;
      wrappedKeyStore.storeWrappedKey(wrappedKeyMaterial, fileID, keyName);
      keyMetadata = keyName.getBytes(StandardCharsets.UTF_8);
      keyCounter++;
    }
    else {
      keyMetadata  = wrappedKeyMaterial.getBytes(StandardCharsets.UTF_8);
    }
    ParquetKey key = new ParquetKey(dataKey, keyMetadata);
    return key;
  }

  public DecryptionKeyRetriever getKeyRetriever() {
    return new WrappedKeyRetriever(kmsClient, wrapLocally, wrappedKeyStore, fileID);
  }
}
