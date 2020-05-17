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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.keytools.KeyTookit.KeyEncryptionKey;
import org.codehaus.jackson.map.ObjectMapper;

public class FileKeyWrapper {

  public static final int KEK_LENGTH = 16;
  public static final int KEK_ID_LENGTH = 16;

  // For every token: a map of MEK_ID to (KEK ID and KEK)
  private static final Map<String, ExpiringCacheEntry<HashMap<String,KeyEncryptionKey>>> KEKMapPerToken =
      new HashMap<>(KeyTookit.INITIAL_PER_TOKEN_CACHE_SIZE);
  private static volatile long lastKekCacheCleanupTimestamp = System.currentTimeMillis() + 60l * 1000; // grace period of 1 minute;

  //A map of MEK_ID to (KEK ID and KEK) - for the current token
  private final Map<String,KeyEncryptionKey> KEKPerMasterKeyID;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final long cacheEntryLifetime;

  private final KmsClient kmsClient;
  private final String kmsInstanceID;
  private final String kmsInstanceURL;
  private final FileKeyMaterialStore keyMaterialStore;
  private final Configuration hadoopConfiguration;
  private final SecureRandom random;
  private final boolean doubleWrapping;

  private short keyCounter;
  private String accessToken;

  public FileKeyWrapper(Configuration configuration, FileKeyMaterialStore keyMaterialStore) {
    this.hadoopConfiguration = configuration;

    cacheEntryLifetime = 1000l * hadoopConfiguration.getLong(KeyTookit.TOKEN_LIFETIME_PROPERTY_NAME, 
        KeyTookit.DEFAULT_CACHE_ENTRY_LIFETIME); 

    kmsInstanceID = hadoopConfiguration.getTrimmed(KeyTookit.KMS_INSTANCE_ID_PROPERTY_NAME, 
        KeyTookit.DEFAULT_KMS_INSTANCE_ID);

    doubleWrapping =  hadoopConfiguration.getBoolean(KeyTookit.DOUBLE_WRAPPING_PROPERTY_NAME, true);
    accessToken = hadoopConfiguration.getTrimmed(KeyTookit.KEY_ACCESS_TOKEN_PROPERTY_NAME, KeyTookit.DEFAULT_ACCESS_TOKEN);

    kmsClient = KeyTookit.getKmsClient(kmsInstanceID, configuration, accessToken, cacheEntryLifetime);

    kmsInstanceURL = hadoopConfiguration.getTrimmed(KeyTookit.KMS_INSTANCE_URL_PROPERTY_NAME, 
        KeyTookit.DEFAULT_KMS_INSTANCE_URL);

    this.keyMaterialStore = keyMaterialStore;

    random = new SecureRandom();
    keyCounter = 0;

    // Check caches upon each file writing (clean once in cacheEntryLifetime)
    KeyTookit.checkCacheEntriesForExpiredTokens(KEKMapPerToken, lastKekCacheCleanupTimestamp, cacheEntryLifetime);
    KeyTookit.checkKmsCacheForExpiredTokens(cacheEntryLifetime);

    ExpiringCacheEntry<HashMap<String, KeyEncryptionKey>> KEKCacheEntry;
    synchronized (KEKMapPerToken) {
      KEKCacheEntry = KEKMapPerToken.get(accessToken);
      if ((null == KEKCacheEntry) || KEKCacheEntry.isExpired()) {
        KEKCacheEntry = new ExpiringCacheEntry<>(new HashMap<String, KeyEncryptionKey>(), cacheEntryLifetime);
        KEKMapPerToken.put(accessToken, KEKCacheEntry);
      }
    }
    KEKPerMasterKeyID = KEKCacheEntry.getCachedItem();
  }

  public byte[] getEncryptionKeyMetadata(byte[] dataKey, String masterKeyID, boolean isFooterKey) {
    return getEncryptionKeyMetadata(dataKey, masterKeyID, isFooterKey, keyMaterialStore, null);
  }

  static void removeCacheEntriesForToken(String accessToken) {
    synchronized(KEKMapPerToken) {
      KEKMapPerToken.remove(accessToken);
    }
  }

  static void removeCacheEntriesForAllTokens() {
    synchronized (KEKMapPerToken) {
      KEKMapPerToken.clear();
    }
  }

  byte[] getEncryptionKeyMetadata(byte[] dataKey, String masterKeyID, boolean isFooterKey, 
      FileKeyMaterialStore targetKeyMaterialStore, String keyIdInFile) {
    if (null == kmsClient) {
      throw new ParquetCryptoRuntimeException("No KMS client available. See previous errors.");
    }

    KeyEncryptionKey keyEncryptionKey = null;
    String encodedWrappedDEK = null;
    if (!doubleWrapping) {
      encodedWrappedDEK = kmsClient.wrapKey(dataKey, masterKeyID);
    } else {
      // Find or generate KEK for Master Key ID
      synchronized (KEKPerMasterKeyID) {
        keyEncryptionKey = KEKPerMasterKeyID.get(masterKeyID);
        if (null == keyEncryptionKey) {
          keyEncryptionKey = createKeyEncryptionKey(masterKeyID);
          KEKPerMasterKeyID.put(masterKeyID, keyEncryptionKey);
        }
      }

      // Encrypt DEK with KEK
      byte[] AAD = keyEncryptionKey.getID();
      encodedWrappedDEK = KeyTookit.wrapKeyLocally(dataKey, keyEncryptionKey.getBytes(), AAD);
    }

    // Pack all into key material JSON
    Map<String, String> keyMaterialMap = new HashMap<String, String>(10);
    if (isFooterKey) {
      keyMaterialMap.put(KeyTookit.KEY_MATERIAL_TYPE_FIELD, KeyTookit.KEY_MATERIAL_TYPE);
      keyMaterialMap.put(KeyTookit.KMS_INSTANCE_ID_FIELD, kmsInstanceID);
      keyMaterialMap.put(KeyTookit.KMS_INSTANCE_URL_FIELD, kmsInstanceURL);
    }
    if (null == targetKeyMaterialStore) {
      keyMaterialMap.put(KeyTookit.KEY_MATERIAL_INTERNAL_STORAGE_FIELD, "true"); // TODO use/check
    }
    keyMaterialMap.put(KeyTookit.DOUBLE_WRAPPING_FIELD, Boolean.toString(doubleWrapping));
    keyMaterialMap.put(KeyTookit.MASTER_KEY_ID_FIELD, masterKeyID);

    if (doubleWrapping) {
      keyMaterialMap.put(KeyTookit.KEK_ID_FIELD, keyEncryptionKey.getEncodedID());
      keyMaterialMap.put(KeyTookit.WRAPPED_KEK_FIELD, keyEncryptionKey.getWrappedWithCRK());
    }
    keyMaterialMap.put(KeyTookit.WRAPPED_DEK_FIELD, encodedWrappedDEK);
    String keyMaterial;
    try {
      keyMaterial = objectMapper.writeValueAsString(keyMaterialMap);
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Failed to parse key material", e);
    }

    // Create key metadata
    byte[] keyMetadata = null;
    if (null != targetKeyMaterialStore) {
      if (null == keyIdInFile) {
        if (isFooterKey) {
          keyIdInFile = KeyTookit.FOOTER_KEY_ID_IN_FILE;
        } else {
          keyIdInFile = KeyTookit.KEY_ID_IN_FILE_PREFIX + keyCounter;
          keyCounter++;
        }
      }
      targetKeyMaterialStore.addKeyMaterial(keyIdInFile, keyMaterial);

      Map<String, String> keyMetadataMap = new HashMap<String, String>(2);
      keyMetadataMap.put(KeyTookit.KEY_REFERENCE_FIELD, keyIdInFile);

      String keyMetadataString;
      try {
        keyMetadataString = objectMapper.writeValueAsString(keyMetadataMap);
      } catch (Exception e) {
        throw new ParquetCryptoRuntimeException("Failed to serialize key material", e);
      }

      keyMetadata = keyMetadataString.getBytes(StandardCharsets.UTF_8);
    }  else {
      keyMetadata  = keyMaterial.getBytes(StandardCharsets.UTF_8);
    }

    return keyMetadata;
  }

  private KeyEncryptionKey createKeyEncryptionKey(String masterKeyID) {
    byte[] kekBytes = new byte[KEK_LENGTH]; 
    random.nextBytes(kekBytes);

    byte[] kekID = new byte[KEK_ID_LENGTH];
    random.nextBytes(kekID);
    String encodedKEK_ID = Base64.getEncoder().encodeToString(kekID);

    // Encrypt KEK with Master key
    String encodedWrappedKEK = null;
    encodedWrappedKEK = kmsClient.wrapKey(kekBytes, masterKeyID);

    return new KeyEncryptionKey(kekBytes, encodedKEK_ID, kekID, encodedWrappedKEK);
  }
}