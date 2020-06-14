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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.keytools.KeyToolkit.KeyEncryptionKey;
import org.codehaus.jackson.map.ObjectMapper;

public class FileKeyWrapper {

  public static final int KEK_LENGTH = 16;
  public static final int KEK_ID_LENGTH = 16;

  // For every token: a map of MEK_ID to (KEK ID and KEK)
  private static final ConcurrentMap<String, ExpiringCacheEntry<ConcurrentMap<String, KeyEncryptionKey>>> KEKMapPerToken =
      new ConcurrentHashMap<>(KeyToolkit.INITIAL_PER_TOKEN_CACHE_SIZE);
  private static volatile long lastKekCacheCleanupTimestamp = System.currentTimeMillis() + 60l * 1000; // grace period of 1 minute;

  //A map of MEK_ID to (KEK ID and KEK) - for the current token
  private final ConcurrentMap<String, KeyEncryptionKey> KEKPerMasterKeyID;

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

    cacheEntryLifetime = 1000l * hadoopConfiguration.getLong(KeyToolkit.TOKEN_LIFETIME_PROPERTY_NAME, 
        KeyToolkit.DEFAULT_CACHE_ENTRY_LIFETIME); 

    kmsInstanceID = hadoopConfiguration.getTrimmed(KeyToolkit.KMS_INSTANCE_ID_PROPERTY_NAME, 
        KmsClient.DEFAULT_KMS_INSTANCE_ID);

    doubleWrapping =  hadoopConfiguration.getBoolean(KeyToolkit.DOUBLE_WRAPPING_PROPERTY_NAME, true);
    accessToken = hadoopConfiguration.getTrimmed(KeyToolkit.KEY_ACCESS_TOKEN_PROPERTY_NAME, KmsClient.DEFAULT_ACCESS_TOKEN);

    kmsClient = KeyToolkit.getKmsClient(kmsInstanceID, configuration, accessToken, cacheEntryLifetime);

    kmsInstanceURL = hadoopConfiguration.getTrimmed(KeyToolkit.KMS_INSTANCE_URL_PROPERTY_NAME, 
        RemoteKmsClient.DEFAULT_KMS_INSTANCE_URL);

    this.keyMaterialStore = keyMaterialStore;

    random = new SecureRandom();
    keyCounter = 0;

    // Check caches upon each file writing (clean once in cacheEntryLifetime)
    KeyToolkit.checkKmsCacheForExpiredTokens(cacheEntryLifetime);
    if (doubleWrapping) {
      checkKekCacheForExpiredTokens();

      ExpiringCacheEntry<ConcurrentMap<String, KeyEncryptionKey>> KEKCacheEntry = KEKMapPerToken.get(accessToken);
      if ((null == KEKCacheEntry) || KEKCacheEntry.isExpired()) {
        synchronized (KEKMapPerToken) {
          KEKCacheEntry = KEKMapPerToken.get(accessToken);
          if ((null == KEKCacheEntry) || KEKCacheEntry.isExpired()) {
            KEKCacheEntry = new ExpiringCacheEntry<>(new ConcurrentHashMap<String, KeyEncryptionKey>(), cacheEntryLifetime);
            KEKMapPerToken.put(accessToken, KEKCacheEntry);
          }
        }
      }
      KEKPerMasterKeyID = KEKCacheEntry.getCachedItem();
    } else {
      KEKPerMasterKeyID = null;
    }
  }

  public byte[] getEncryptionKeyMetadata(byte[] dataKey, String masterKeyID, boolean isFooterKey) {
    return getEncryptionKeyMetadata(dataKey, masterKeyID, isFooterKey, null);
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

  private void checkKekCacheForExpiredTokens() {
    long now = System.currentTimeMillis();

    if (now > (lastKekCacheCleanupTimestamp + cacheEntryLifetime)) {
      synchronized (KEKMapPerToken) {
        if (now > (lastKekCacheCleanupTimestamp + cacheEntryLifetime)) {
          KeyToolkit.removeExpiredEntriesFromCache(KEKMapPerToken);
          lastKekCacheCleanupTimestamp = now;
        }
      }
    }
  }

  byte[] getEncryptionKeyMetadata(byte[] dataKey, String masterKeyID, boolean isFooterKey, String keyIdInFile) {
    if (null == kmsClient) {
      throw new ParquetCryptoRuntimeException("No KMS client available. See previous errors.");
    }

    KeyEncryptionKey keyEncryptionKey = null;
    String encodedWrappedDEK = null;
    if (!doubleWrapping) {
      encodedWrappedDEK = kmsClient.wrapKey(dataKey, masterKeyID);
    } else {
      // Find in cache, or generate KEK for Master Key ID
      keyEncryptionKey = KEKPerMasterKeyID.computeIfAbsent(masterKeyID,
          (k) -> createKeyEncryptionKey(masterKeyID));

      // Encrypt DEK with KEK
      byte[] AAD = keyEncryptionKey.getID();
      encodedWrappedDEK = KeyToolkit.wrapKeyLocally(dataKey, keyEncryptionKey.getBytes(), AAD);
    }

    // Pack all into key material JSON
    Map<String, String> keyMaterialMap = new HashMap<String, String>(10);
    keyMaterialMap.put(KeyToolkit.KEY_MATERIAL_TYPE_FIELD, KeyToolkit.KEY_MATERIAL_TYPE);
    if (isFooterKey) {
      keyMaterialMap.put(KeyToolkit.KMS_INSTANCE_ID_FIELD, kmsInstanceID);
      keyMaterialMap.put(KeyToolkit.KMS_INSTANCE_URL_FIELD, kmsInstanceURL);
    }
    if (null == keyMaterialStore) {
      keyMaterialMap.put(KeyToolkit.KEY_MATERIAL_INTERNAL_STORAGE_FIELD, "true"); // TODO use/check
    }
    keyMaterialMap.put(KeyToolkit.DOUBLE_WRAPPING_FIELD, Boolean.toString(doubleWrapping));
    keyMaterialMap.put(KeyToolkit.MASTER_KEY_ID_FIELD, masterKeyID);

    if (doubleWrapping) {
      keyMaterialMap.put(KeyToolkit.KEK_ID_FIELD, keyEncryptionKey.getEncodedID());
      keyMaterialMap.put(KeyToolkit.WRAPPED_KEK_FIELD, keyEncryptionKey.getWrappedWithCRK());
    }
    keyMaterialMap.put(KeyToolkit.WRAPPED_DEK_FIELD, encodedWrappedDEK);
    String keyMaterial;
    try {
      keyMaterial = objectMapper.writeValueAsString(keyMaterialMap);
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Failed to parse key material", e);
    }

    // Create key metadata
    byte[] keyMetadata = null;
    if (null != keyMaterialStore) {
      if (null == keyIdInFile) {
        if (isFooterKey) {
          keyIdInFile = KeyToolkit.FOOTER_KEY_ID_IN_FILE;
        } else {
          keyIdInFile = KeyToolkit.KEY_ID_IN_FILE_PREFIX + keyCounter;
          keyCounter++;
        }
      }
      keyMaterialStore.addKeyMaterial(keyIdInFile, keyMaterial);

      Map<String, String> keyMetadataMap = new HashMap<String, String>(2);
      keyMetadataMap.put(KeyToolkit.KEY_REFERENCE_FIELD, keyIdInFile);

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
