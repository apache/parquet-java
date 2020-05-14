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
import org.apache.parquet.crypto.keytools.KeyToolUtilities.KeyEncryptionKey;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Management of file encryption keys, and their metadata.
 * For scalability, implementing code is recommended to run locally / not to make remote calls -
 * except for calls to KMS server, via the pre-defined KmsClient interface.
 *
 * Implementing class instance should be created per each Parquet file. The methods don't need to
 * be thread-safe.
 */
public class FileKeyWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(FileKeyWrapper.class);

  public static final String KMS_CLIENT_CLASS_PROPERTY_NAME = "encryption.kms.client.class";
  public static final String KMS_INSTANCE_ID_PROPERTY_NAME = "encryption.kms.instance.id";
  public static final String DOUBLE_WRAPPING_PROPERTY_NAME = "encryption.double.wrapping";
  public static final String KEY_ACCESS_TOKEN_PROPERTY_NAME = "encryption.key.access.token";
  public static final String TOKEN_LIFETIME_PROPERTY_NAME = "encryption.key.access.token.lifetime";

  public static final String KEY_MATERIAL_TYPE_FIELD = "keyMaterialType";
  public static final String KEY_MATERIAL_TYPE = "PKMT1";
  public static final String KEY_MATERIAL_INTERNAL_STORAGE_FIELD = "internalStorage";
  public static final String KEY_REFERENCE_FIELD = "keyReference";
  public static final String DOUBLE_WRAPPING_FIELD = "doubleWrapping";

  public static final String KMS_INSTANCE_ID_FIELD = "kmsInstanceID";
  public static final String KMS_INSTANCE_URL_FIELD = "kmsInstanceURL";

  public static final String MASTER_KEY_ID_FIELD = "masterKeyID";
  public static final String WRAPPED_DEK_FIELD = "wrappedDEK";
  public static final String KEK_ID_FIELD = "keyEncryptionKeyID";
  public static final String WRAPPED_KEK_FIELD = "wrappedKEK";

  public static final int KEK_LENGTH = 16;
  public static final int KEK_ID_LENGTH = 16;

  public static final String DEFAULT_KMS_INSTANCE_ID = "DEFAULT";
  public static final String DEFAULT_KMS_INSTANCE_URL = "DEFAULT";
  public static final String DEFAULT_ACCESS_TOKEN = "DEFAULT";

  public static final String FOOTER_KEY_ID_IN_FILE = "kf";
  public static final String KEY_ID_IN_FILE_PREFIX = "k";

  public static final long DEFAULT_CACHE_ENTRY_LIFETIME = 10 * 60; // 10 minutes
  public static final int INITIAL_PER_TOKEN_CACHE_SIZE = 5;
  // For every token: a map of KMSInstanceId to kmsClient
  private static final Map<String, ExpiringCacheEntry<Map<String, KmsClient>>> kmsClientCachePerToken =
      new HashMap<>(INITIAL_PER_TOKEN_CACHE_SIZE);
  // For every token: a map of MEK_ID to (KEK ID and KEK)
  private static final Map<String, ExpiringCacheEntry<HashMap<String,KeyEncryptionKey>>> KEKMapPerToken =
      new HashMap<>(INITIAL_PER_TOKEN_CACHE_SIZE);
  private static volatile long lastKmsCacheCleanupTimestamp = System.currentTimeMillis() + 60l * 1000; // grace period of 1 minute
  private static volatile long lastKekCacheCleanupTimestamp = lastKmsCacheCleanupTimestamp;

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

    cacheEntryLifetime = 1000l * hadoopConfiguration.getLong(TOKEN_LIFETIME_PROPERTY_NAME, 
        DEFAULT_CACHE_ENTRY_LIFETIME); 

    kmsInstanceID = hadoopConfiguration.getTrimmed(KMS_INSTANCE_ID_PROPERTY_NAME, 
        DEFAULT_KMS_INSTANCE_ID);

    doubleWrapping =  hadoopConfiguration.getBoolean(DOUBLE_WRAPPING_PROPERTY_NAME, true);
    accessToken = hadoopConfiguration.getTrimmed(KEY_ACCESS_TOKEN_PROPERTY_NAME, DEFAULT_ACCESS_TOKEN);

    kmsClient = getKmsClient(kmsInstanceID, configuration, accessToken, cacheEntryLifetime);

    kmsInstanceURL = hadoopConfiguration.getTrimmed(RemoteKmsClient.KMS_INSTANCE_URL_PROPERTY_NAME, 
        DEFAULT_KMS_INSTANCE_URL);

    this.keyMaterialStore = keyMaterialStore;

    random = new SecureRandom();
    keyCounter = 0;
    
    // Check caches upon each file writing (clean once in cacheEntryLifetime)
    checkCacheEntriesForExpiredTokens();

    ExpiringCacheEntry<HashMap<String, KeyEncryptionKey>> KEKCacheEntry = KEKMapPerToken.get(accessToken);
    if ((null == KEKCacheEntry) || KEKCacheEntry.isExpired()) {
      synchronized (KEKMapPerToken) {
        KEKCacheEntry = KEKMapPerToken.get(accessToken);
        if ((null == KEKCacheEntry) || KEKCacheEntry.isExpired()) {
          LOG.debug("CACHE --- create write-KEK cache for token: " + formatTokenForLog(accessToken));
          KEKCacheEntry = new ExpiringCacheEntry<>(new HashMap<String, KeyEncryptionKey>(), cacheEntryLifetime);
          KEKMapPerToken.put(accessToken, KEKCacheEntry);
        }
      }
    }
    KEKPerMasterKeyID = KEKCacheEntry.getCachedItem();
  }

  public byte[] getEncryptionKeyMetadata(byte[] dataKey, String masterKeyID, boolean isFooterKey) {
    return getEncryptionKeyMetadata(dataKey, masterKeyID, isFooterKey, keyMaterialStore, null);
  }

  static void removeCacheEntriesForToken(String accessToken) {
    synchronized (kmsClientCachePerToken) {
      kmsClientCachePerToken.remove(accessToken);
    }
    synchronized(KEKMapPerToken) {
      KEKMapPerToken.remove(accessToken);
    }
  }

  static void removeCacheEntriesForAllTokens() {
    synchronized (kmsClientCachePerToken) {
      kmsClientCachePerToken.clear();
    }
    synchronized (KEKMapPerToken) {
      KEKMapPerToken.clear();
    }
  }

  private void checkCacheEntriesForExpiredTokens() {
    long now = System.currentTimeMillis();
    if (now > (lastKekCacheCleanupTimestamp + this.cacheEntryLifetime)) {
      synchronized (KEKMapPerToken) {
        if (now > (lastKekCacheCleanupTimestamp + this.cacheEntryLifetime)) {
          KeyToolUtilities.removeExpiredEntriesFromCache(KEKMapPerToken);
          lastKekCacheCleanupTimestamp = now;
        }
      }
    }

    if (now > (lastKmsCacheCleanupTimestamp + this.cacheEntryLifetime)) {
      synchronized (kmsClientCachePerToken) {
        if (now > (lastKmsCacheCleanupTimestamp + this.cacheEntryLifetime)) {
          KeyToolUtilities.removeExpiredEntriesFromCache(kmsClientCachePerToken);
          lastKmsCacheCleanupTimestamp = now;
        }
      }
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
      keyEncryptionKey = KEKPerMasterKeyID.get(masterKeyID);
      if (null == keyEncryptionKey) {
        synchronized (KEKPerMasterKeyID) {
          keyEncryptionKey = KEKPerMasterKeyID.get(masterKeyID);
          if (null == keyEncryptionKey) {
            keyEncryptionKey = createKeyEncryptionKey(masterKeyID);
            KEKPerMasterKeyID.put(masterKeyID, keyEncryptionKey);
            LOG.debug("CACHE --- get KEK " + masterKeyID + " and add to write-KEK cache for token: " + formatTokenForLog(accessToken));
          }
        }
      }

      // Encrypt DEK with KEK
      byte[] AAD = keyEncryptionKey.getID();
      encodedWrappedDEK = KeyToolUtilities.wrapKeyLocally(dataKey, keyEncryptionKey.getBytes(), AAD);
    }

    // Pack all into key material JSON
    Map<String, String> keyMaterialMap = new HashMap<String, String>(10);
    if (isFooterKey) {
      keyMaterialMap.put(KEY_MATERIAL_TYPE_FIELD, KEY_MATERIAL_TYPE);
      keyMaterialMap.put(KMS_INSTANCE_ID_FIELD, kmsInstanceID);
      keyMaterialMap.put(KMS_INSTANCE_URL_FIELD, kmsInstanceURL);
    }
    if (null == targetKeyMaterialStore) {
      keyMaterialMap.put(KEY_MATERIAL_INTERNAL_STORAGE_FIELD, "true"); // TODO use/check
    }
    keyMaterialMap.put(DOUBLE_WRAPPING_FIELD, Boolean.toString(doubleWrapping));
    keyMaterialMap.put(MASTER_KEY_ID_FIELD, masterKeyID);

    if (doubleWrapping) {
      keyMaterialMap.put(KEK_ID_FIELD, keyEncryptionKey.getEncodedID());
      keyMaterialMap.put(WRAPPED_KEK_FIELD, keyEncryptionKey.getWrappedWithCRK());
    }
    keyMaterialMap.put(WRAPPED_DEK_FIELD, encodedWrappedDEK);
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
          keyIdInFile = FOOTER_KEY_ID_IN_FILE;
        } else {
          keyIdInFile = KEY_ID_IN_FILE_PREFIX + keyCounter;
          keyCounter++;
        }
      }
      targetKeyMaterialStore.addKeyMaterial(keyIdInFile, keyMaterial);

      Map<String, String> keyMetadataMap = new HashMap<String, String>(2);
      keyMetadataMap.put(KEY_REFERENCE_FIELD, keyIdInFile);

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

  /**
   * Get KMSClient from cache, or create and initialize it
   * @param configuration
   * @param kmsInstanceID
   * @return
   */
  static KmsClient getKmsClient(String kmsInstanceID, Configuration configuration, String accessToken, long cacheEntryLifetime) {
    // Try cache first
    ExpiringCacheEntry<Map<String, KmsClient>> kmsClientCachePerTokenEntry = kmsClientCachePerToken.get(accessToken);
    if ((null == kmsClientCachePerTokenEntry) || kmsClientCachePerTokenEntry.isExpired()) {
      synchronized (kmsClientCachePerToken) {
        kmsClientCachePerTokenEntry = kmsClientCachePerToken.get(accessToken);
        if ((null == kmsClientCachePerTokenEntry) || kmsClientCachePerTokenEntry.isExpired()) {
          Map<String, KmsClient> kmsClientPerToken = new HashMap<>();

          kmsClientCachePerTokenEntry = new ExpiringCacheEntry<>(kmsClientPerToken, cacheEntryLifetime);
          kmsClientCachePerToken.put(accessToken, kmsClientCachePerTokenEntry);
        }
      }
    }
    final Map<String, KmsClient> kmsClientPerKmsInstanceCache = kmsClientCachePerTokenEntry.getCachedItem();
    KmsClient kmsClient = kmsClientPerKmsInstanceCache.get(kmsInstanceID);

    // Not in cache. Create.
    if (null == kmsClient) {
      synchronized (kmsClientPerKmsInstanceCache) {
        kmsClient = kmsClientPerKmsInstanceCache.get(kmsInstanceID);
        if (null == kmsClient) {
          kmsClient = createAndInitKmsClient(kmsInstanceID, configuration, accessToken);
          kmsClientPerKmsInstanceCache.put(kmsInstanceID, kmsClient);
        }
      }
    }

    return kmsClient;
  }

  private static KmsClient createAndInitKmsClient(String kmsInstanceID, Configuration configuration, String accessToken) {
    Class<?> kmsClientClass = null;
    KmsClient kmsClient = null;

    try {
      kmsClientClass = ConfigurationUtil.getClassFromConfig(configuration,
          KMS_CLIENT_CLASS_PROPERTY_NAME, KmsClient.class);

      if (null == kmsClientClass) {
        throw new ParquetCryptoRuntimeException("Unspecified " + KMS_CLIENT_CLASS_PROPERTY_NAME);
      }
      kmsClient = (KmsClient)kmsClientClass.newInstance();
    } catch (InstantiationException | IllegalAccessException | BadConfigurationException e) {
      throw new ParquetCryptoRuntimeException("Could not instantiate KmsClient class: "
          + kmsClientClass, e);
    }

    kmsClient.initialize(configuration, kmsInstanceID);

    return kmsClient;
  }

  private static String formatTokenForLog(String accessToken) {
    int maxTokenDisplayLength = 5;
    if (accessToken.length() <= maxTokenDisplayLength) {
      return accessToken;
    }
    return accessToken.substring(accessToken.length() - maxTokenDisplayLength);
  }
}