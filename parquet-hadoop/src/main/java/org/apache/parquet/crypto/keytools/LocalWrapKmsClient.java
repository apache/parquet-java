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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Typically, KMS systems support in-server key wrapping. Their clients should implement KmsClient interface directly.
 * An extension of the LocalWrapKmsClient class should used only in rare situations where in-server wrapping is not
 * supported. The wrapping will be done locally then - the MEKs will be fetched from the KMS server via the
 * getMasterKeyFromServer function, and used to encrypt a DEK or KEK inside the LocalWrapKmsClient code.
 * Note: master key rotation is not supported with local wrapping.
 */
public abstract class LocalWrapKmsClient implements KmsClient {

  public static final String LOCAL_WRAP_NO_KEY_VERSION = "NO_VERSION";

  protected String kmsInstanceID;
  protected String kmsInstanceURL;
  protected String kmsToken;
  protected Configuration hadoopConfiguration;

  // MasterKey cache: master keys per key ID (per KMS Client). For local wrapping only.
  private ConcurrentMap<String, byte[]> masterKeyCache;

  /**
   * KMS systems wrap keys by encrypting them by master keys, and attaching additional information (such as the version 
   * number of the masker key) to the result of encryption. The master key version is required in  key rotation.
   * Currently, the local wrapping mode does not support key rotation (because not all KMS systems allow to fetch a master
   * key by its ID and version number). Still, the local wrapping mode adds a placeholder for the master key version, that will
   * enable support for key rotation in this mode in the future, with appropriate KMS systems. This will also enable backward
   * compatibility, where future readers will be able to extract master key version in the files written by the current code.
   * 
   * LocalKeyWrap class writes (and reads) the "key wrap" as a flat json with the following fields:
   * 1. "masterKeyVersion" - a String, with the master key version. In the current version, only one value is allowed - "NO_VERSION".
   * 2. "encryptedKey" - a String, with the key encrypted by the master key (base64-encoded).
   */
  static class LocalKeyWrap {
    public static final String LOCAL_WRAP_KEY_VERSION_FIELD = "masterKeyVersion";
    public static final String LOCAL_WRAP_ENCRYPTED_KEY_FIELD = "encryptedKey";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private String encryptedEncodedKey;
    private String masterKeyVersion;

    private LocalKeyWrap(String masterKeyVersion, String encryptedEncodedKey) {
      this.masterKeyVersion = masterKeyVersion;
      this.encryptedEncodedKey = encryptedEncodedKey;
    }

    private static String createSerialized(String encryptedEncodedKey) {
      Map<String, String> keyWrapMap = new HashMap<String, String>(2);
      keyWrapMap.put(LOCAL_WRAP_KEY_VERSION_FIELD, LOCAL_WRAP_NO_KEY_VERSION);
      keyWrapMap.put(LOCAL_WRAP_ENCRYPTED_KEY_FIELD, encryptedEncodedKey);
      try {
        return OBJECT_MAPPER.writeValueAsString(keyWrapMap);
      } catch (IOException e) {
        throw new ParquetCryptoRuntimeException("Failed to serialize local key wrap map", e);
      }
    }

    private static LocalKeyWrap parse(String wrappedKey) {
      Map<String, String> keyWrapMap = null;
      try {
        keyWrapMap = OBJECT_MAPPER.readValue(new StringReader(wrappedKey),
            new TypeReference<Map<String, String>>() {});
      } catch (IOException e) {
        throw new ParquetCryptoRuntimeException("Failed to parse local key wrap json " + wrappedKey, e);
      }
      String encryptedEncodedKey = keyWrapMap.get(LOCAL_WRAP_ENCRYPTED_KEY_FIELD);
      String masterKeyVersion = keyWrapMap.get(LOCAL_WRAP_KEY_VERSION_FIELD);

      return new LocalKeyWrap(masterKeyVersion, encryptedEncodedKey);
    }

    private String getMasterKeyVersion() {
      return masterKeyVersion;
    }

    private String getEncryptedKey() {
      return encryptedEncodedKey;
    }
  }

  @Override
  public void initialize(Configuration configuration, String kmsInstanceID, String kmsInstanceURL, String accessToken) {
    this.kmsInstanceID = kmsInstanceID;
    this.kmsInstanceURL = kmsInstanceURL;
    
    masterKeyCache = new ConcurrentHashMap<>();
    hadoopConfiguration = configuration;
    kmsToken = accessToken;

    initializeInternal();
  }

  @Override
  public String wrapKey(byte[] key, String masterKeyIdentifier) throws KeyAccessDeniedException {
    byte[] masterKey =  masterKeyCache.computeIfAbsent(masterKeyIdentifier,
          (k) -> getKeyFromServer(masterKeyIdentifier));
    byte[] AAD = masterKeyIdentifier.getBytes(StandardCharsets.UTF_8);
    String encryptedEncodedKey =  KeyToolkit.encryptKeyLocally(key, masterKey, AAD);
    return LocalKeyWrap.createSerialized(encryptedEncodedKey);
  }

  @Override
  public byte[] unwrapKey(String wrappedKey, String masterKeyIdentifier) throws KeyAccessDeniedException {
    LocalKeyWrap keyWrap = LocalKeyWrap.parse(wrappedKey);
    String masterKeyVersion = keyWrap.getMasterKeyVersion();
    if (!LOCAL_WRAP_NO_KEY_VERSION.equals(masterKeyVersion)) {
      throw new ParquetCryptoRuntimeException("Master key versions are not supported for local wrapping: "
        + masterKeyVersion);
    }
    String encryptedEncodedKey = keyWrap.getEncryptedKey();
    byte[] masterKey = masterKeyCache.computeIfAbsent(masterKeyIdentifier,
        (k) -> getKeyFromServer(masterKeyIdentifier));
    byte[] AAD = masterKeyIdentifier.getBytes(StandardCharsets.UTF_8);
    return KeyToolkit.decryptKeyLocally(encryptedEncodedKey, masterKey, AAD);
  }

  private byte[] getKeyFromServer(String keyIdentifier) {
    // refresh token
    kmsToken = hadoopConfiguration.getTrimmed(KeyToolkit.KEY_ACCESS_TOKEN_PROPERTY_NAME);
    byte[] key = getMasterKeyFromServer(keyIdentifier);
    int keyLength = key.length;
    if (!(16 == keyLength || 24 == keyLength || 32 == keyLength)) {
      throw new ParquetCryptoRuntimeException( "Wrong length: "+ keyLength +
          " of AES key: "  + keyIdentifier);
    }
    return key;
  }

  /**
   * Get master key from the remote KMS server.
   * 
   * If your KMS client code throws runtime exceptions related to access/permission problems
   * (such as Hadoop AccessControlException), catch them and throw the KeyAccessDeniedException.
   * 
   * @param masterKeyIdentifier: a string that uniquely identifies the master key in a KMS instance
   * @return master key bytes
   * @throws KeyAccessDeniedException unauthorized to get the master key
   */
  protected abstract byte[] getMasterKeyFromServer(String masterKeyIdentifier) 
      throws KeyAccessDeniedException;

  /**
   * Pass configuration with KMS-specific parameters.
   */
  protected abstract void initializeInternal() 
      throws KeyAccessDeniedException;
}
