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

import static org.apache.parquet.crypto.keytools.KeyToolkit.stringIsEmpty;

public abstract class RemoteKmsClient implements KmsClient {

  public static final String LOCAL_WRAP_NO_KEY_VERSION = "NO_VERSION";

  protected String kmsInstanceID;
  protected String kmsInstanceURL;
  protected String kmsToken;
  protected Boolean isWrapLocally;
  protected Configuration hadoopConfiguration;
  protected boolean isDefaultToken;

  // MasterKey cache: master keys per key ID (per KMS Client). For local wrapping only.
  private ConcurrentMap<String, byte[]> masterKeyCache;

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

    this.isWrapLocally = configuration.getBoolean(KeyToolkit.WRAP_LOCALLY_PROPERTY_NAME, KeyToolkit.WRAP_LOCALLY_DEFAULT);
    if (isWrapLocally) {
      masterKeyCache = new ConcurrentHashMap<>();
    }

    hadoopConfiguration = configuration;
    kmsToken = accessToken;

    isDefaultToken = kmsToken.equals(KmsClient.KEY_ACCESS_TOKEN_DEFAULT);

    initializeInternal();
  }

  @Override
  public String wrapKey(byte[] key, String masterKeyIdentifier) throws KeyAccessDeniedException {
    if (isWrapLocally) {
      byte[] masterKey =  masterKeyCache.computeIfAbsent(masterKeyIdentifier,
          (k) -> getKeyFromServer(masterKeyIdentifier));
      byte[] AAD = masterKeyIdentifier.getBytes(StandardCharsets.UTF_8);
      String encryptedEncodedKey =  KeyToolkit.encryptKeyLocally(key, masterKey, AAD);
      return LocalKeyWrap.createSerialized(encryptedEncodedKey);
    } else {
      refreshToken();
      return wrapKeyInServer(key, masterKeyIdentifier);
    }
  }

  @Override
  public byte[] unwrapKey(String wrappedKey, String masterKeyIdentifier) throws KeyAccessDeniedException {
    if (isWrapLocally) {
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
    } else {
      refreshToken();
      return unwrapKeyInServer(wrappedKey, masterKeyIdentifier);
    }
  }

  private void refreshToken() {
    if (isDefaultToken) {
      return;
    }
    kmsToken = hadoopConfiguration.getTrimmed(KeyToolkit.KEY_ACCESS_TOKEN_PROPERTY_NAME);
    if (stringIsEmpty(kmsToken)) {
      throw new ParquetCryptoRuntimeException("Empty token");
    }
  }

  private byte[] getKeyFromServer(String keyIdentifier) {
    refreshToken();
    return getMasterKeyFromServer(keyIdentifier);
  }

  /**
   * Wrap a key with the master key in the remote KMS server.
   * 
   * If your KMS client code throws runtime exceptions related to access/permission problems
   * (such as Hadoop AccessControlException), catch them and throw the KeyAccessDeniedException.
   * 
   * @param keyBytes: key bytes to be wrapped
   * @param masterKeyIdentifier: a string that uniquely identifies the master key in a KMS instance
   * @return wrappedKey: Encrypts key bytes with the master key, encodes the result  and potentially adds a KMS-specific metadata.
   * @throws KeyAccessDeniedException unauthorized to encrypt with the given master key
   * @throws UnsupportedOperationException KMS does not support in-server wrapping 
   */
  protected abstract String wrapKeyInServer(byte[] keyBytes, String masterKeyIdentifier) 
      throws KeyAccessDeniedException, UnsupportedOperationException;

  /**
   * Unwrap a key with the master key in the remote KMS server. 
   * 
   * If your KMS client code throws runtime exceptions related to access/permission problems
   * (such as Hadoop AccessControlException), catch them and throw the KeyAccessDeniedException.
   * 
   * @param wrappedKey String produced by wrapKey operation
   * @param masterKeyIdentifier: a string that uniquely identifies the master key in a KMS instance
   * @return key bytes
   * @throws KeyAccessDeniedException unauthorized to unwrap with the given master key
   * @throws UnsupportedOperationException KMS does not support in-server unwrapping 
   */
  protected abstract byte[] unwrapKeyInServer(String wrappedKey, String masterKeyIdentifier) 
      throws KeyAccessDeniedException, UnsupportedOperationException;

  /**
   * Get master key from the remote KMS server.
   * Required only for local wrapping. No need to implement if KMS supports in-server wrapping/unwrapping.
   * 
   * If your KMS client code throws runtime exceptions related to access/permission problems
   * (such as Hadoop AccessControlException), catch them and throw the KeyAccessDeniedException.
   * 
   * @param masterKeyIdentifier: a string that uniquely identifies the master key in a KMS instance
   * @return master key bytes
   * @throws KeyAccessDeniedException unauthorized to get the master key
   * @throws UnsupportedOperationException If not implemented, or KMS does not support key fetching 
   */
  protected abstract byte[] getMasterKeyFromServer(String masterKeyIdentifier) 
      throws KeyAccessDeniedException, UnsupportedOperationException;

  /**
   * Pass configuration with KMS-specific parameters.
   */
  protected abstract void initializeInternal() 
      throws KeyAccessDeniedException;
}