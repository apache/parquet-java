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
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.keytools.KeyToolUtilities.KeyEncryptionKey;
import org.apache.parquet.crypto.keytools.KeyToolUtilities.KeyWithMasterID;
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
public class EnvelopeKeyManager {
  private static final Logger LOG = LoggerFactory.getLogger(EnvelopeKeyManager.class);
  
  public static final String KEY_METADATA_STORAGE_FIELD = "storageLocation";
  public static final String keyMetadataInternalStorage = "internal";
  public static final String KEY_REFERENCE_FIELD = "keyReference";
  
  public static final String DEFAULT_KMS_INSTANCE_ID = "DEFAULT";
  public static final String DEFAULT_KMS_INSTANCE_URL = "DEFAULT";
  public static final String DEFAULT_ACCESS_TOKEN = "DEFAULT";
  
  public static final String KMS_INSTANCE_ID_FIELD = "kmsInstanceID";
  public static final String KMS_INSTANCE_URL_FIELD = "kmsInstanceURL";

  public static final String WRAPPING_METHOD_PROPERTY_NAME = "encryption.key.wrapping.method";
  public static final String CACHE_ENTRY_LIFETIME_PROPERTY_NAME = "encryption.cache.entry.lifetime";
  public static final String KMS_CLIENT_CLASS_PROPERTY_NAME = "encryption.kms.client.class";
  public static final String KEY_ACCESS_TOKEN_PROPERTY_NAME = "encryption.key.access.token";

  public static final String WRAPPING_METHOD_FIELD = "method";
  public static final String single_wrapping_method = "single";
  public static final String double_wrapping_method = "double";
  public static final String WRAPPING_METHOD_VERSION_FIELD = "version";
  public static final String wrapping_method_version = "0.1";
  public static final String MASTER_KEY_ID_FIELD = "masterKeyID";
  public static final String WRAPPED_DEK_FIELD = "wrappedDEK";

  public static final String KEK_ID_FIELD = "keyEncryptionKeyID";
  public static final String WRAPPED_KEK_FIELD = "wrappedKEK";
  
  public static final long DEFAULT_CACHE_ENTRY_LIFETIME = 10 * 60 * 1000; // 10 minutes
  public static final int INITIAL_PER_TOKEN_CACHE_SIZE = 5;
  // For every access token a map of KMSInstanceId to kmsClient
  private static final ConcurrentMap<String, ExpiringCacheEntry<ConcurrentMap<String, KmsClient>>> kmsClientCachePerToken =
    new ConcurrentHashMap<>(INITIAL_PER_TOKEN_CACHE_SIZE);
  private static final Object cacheLock = new Object();
  private static volatile Long lastCacheCleanupTimestamp = System.currentTimeMillis() + 60l * 1000; // grace period of 1 minute

  private static final ConcurrentMap<String, ExpiringCacheEntry<ConcurrentHashMap<String,KeyEncryptionKey>>> writeKEKMapPerToken =
    new ConcurrentHashMap<>(INITIAL_PER_TOKEN_CACHE_SIZE);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final ConcurrentMap<String,KeyEncryptionKey> writeSessionKEKMap;
  private final long cacheEntryLifetime;

  private final KmsClient kmsClient;
  private final String kmsInstanceID;
  private final String kmsInstanceURLForParquetWrite;
  private final FileKeyMaterialStore keyMaterialStore;
  private final Configuration hadoopConfiguration;
  private final SecureRandom random;
  private final String wrappingMethod;
  private final boolean doubleWrapping;

  private short keyCounter;
  private String accessToken;

  public EnvelopeKeyManager(Configuration configuration, FileKeyMaterialStore keyMaterialStore) {
    this.hadoopConfiguration = configuration;

    this.cacheEntryLifetime = hadoopConfiguration.getLong(CACHE_ENTRY_LIFETIME_PROPERTY_NAME, DEFAULT_CACHE_ENTRY_LIFETIME);
    invalidateCachesForExpiredTokens();

    String kmsInstanceID = hadoopConfiguration.getTrimmed(RemoteKmsClient.KMS_INSTANCE_ID_PROPERTY_NAME);
    if (StringUtils.isEmpty(kmsInstanceID)) {
      kmsInstanceID = DEFAULT_KMS_INSTANCE_ID;
    }
    this.kmsInstanceID = kmsInstanceID;
    this.kmsClient = getKmsClient(configuration, kmsInstanceID);

    String kmsInstanceURLForParquetWrite = hadoopConfiguration.getTrimmed(RemoteKmsClient.KMS_INSTANCE_URL_PROPERTY_NAME);
    if (StringUtils.isEmpty(kmsInstanceURLForParquetWrite)) {
      kmsInstanceURLForParquetWrite = DEFAULT_KMS_INSTANCE_URL;
    }
    this.kmsInstanceURLForParquetWrite = kmsInstanceURLForParquetWrite;

    this.keyMaterialStore = keyMaterialStore;
    random = new SecureRandom();
    keyCounter = 0;

    String wrappingMethod =  hadoopConfiguration.getTrimmed(WRAPPING_METHOD_PROPERTY_NAME);
    if (!StringUtils.isEmpty(wrappingMethod) && wrappingMethod.equals(single_wrapping_method)) { // TODO
      doubleWrapping = false;
      this.wrappingMethod = wrappingMethod;
    } else {
      doubleWrapping = true; // default
      this.wrappingMethod = double_wrapping_method;
  }

    this.accessToken = getAccessTokenOrDefault(configuration);

    ExpiringCacheEntry<ConcurrentHashMap<String, KeyEncryptionKey>> writeKEKCacheEntry = writeKEKMapPerToken.get(accessToken);
    if ((null == writeKEKCacheEntry) || writeKEKCacheEntry.isExpired()) {
      synchronized (cacheLock) {
        writeKEKCacheEntry = writeKEKMapPerToken.get(accessToken);
        if ((null == writeKEKCacheEntry) || writeKEKCacheEntry.isExpired()) {
          LOG.debug("CACHE --- create write-KEK cache for token: " + formatTokenForLog(accessToken));
          writeKEKCacheEntry = new ExpiringCacheEntry<>(new ConcurrentHashMap<String, KeyEncryptionKey>(), this.cacheEntryLifetime);
          writeKEKMapPerToken.put(accessToken, writeKEKCacheEntry);
        }
      }
    }
    this.writeSessionKEKMap = writeKEKCacheEntry.getCachedItem();
  }

  public byte[] getEncryptionKeyMetadata(byte[] dataKey, String masterKeyID, boolean isFooterKey) {
    return getEncryptionKeyMetadata(dataKey, masterKeyID, isFooterKey, keyMaterialStore, null);
  }

  public void rotateMasterKeys(FileKeyMaterialStore tempKeyMaterialStore) {
    if (null == keyMaterialStore) {
      throw new ParquetCryptoRuntimeException("Null key material store");
    }
    if (null == tempKeyMaterialStore) {
      throw new ParquetCryptoRuntimeException("Null temp key material store");
    }

    EnvelopeKeyRetriever unwrapper = new EnvelopeKeyRetriever(kmsClient, hadoopConfiguration, keyMaterialStore);

    Set<String> fileKeyIdSet = keyMaterialStore.getFileKeyIDSet();

    for (String keyIdInFile : fileKeyIdSet) {
      boolean footerKey = keyIdInFile.equals("kf"); // TODO
      String keyMaterial = keyMaterialStore.getKeyMaterial(keyIdInFile);
      KeyWithMasterID key = unwrapper.getDEKandMasterID(keyMaterial);
      getEncryptionKeyMetadata(key.getDataKey(), key.getMasterID(), footerKey, tempKeyMaterialStore, keyIdInFile);
    }

    tempKeyMaterialStore.saveFileKeyMaterial();

    keyMaterialStore.removeFileKeyMaterial();

    tempKeyMaterialStore.moveFileKeyMaterial(keyMaterialStore);

    invalidateCachesForAllTokens();
  }

  /**
   * Flush any caches that are tied to the specified accessToken
   * @param accessToken
   */
  public static void invalidateCachesForToken(String accessToken) {
    synchronized (cacheLock) {
      kmsClientCachePerToken.remove(accessToken);
      writeKEKMapPerToken.remove(accessToken);
    }
  }

  private void invalidateCachesForExpiredTokens() {
    long now = System.currentTimeMillis();
    if (now > lastCacheCleanupTimestamp + this.cacheEntryLifetime) {
      synchronized (cacheLock) {
        if (now > lastCacheCleanupTimestamp + this.cacheEntryLifetime) {
          removeExpiredEntriesFromCache(kmsClientCachePerToken);
          removeExpiredEntriesFromCache(writeKEKMapPerToken);
          lastCacheCleanupTimestamp = now;
        }
      }
    }
    }

  static <E> void removeExpiredEntriesFromCache(ConcurrentMap<String, ExpiringCacheEntry<E>> cache) {
    Set<Map.Entry<String, ExpiringCacheEntry<E>>> cacheEntries = cache.entrySet();
    List<String> expiredKeys = new ArrayList<>(cacheEntries.size());
    for (Map.Entry<String, ExpiringCacheEntry<E>> cacheEntry : cacheEntries) {
      if (cacheEntry.getValue().isExpired()) {
        expiredKeys.add(cacheEntry.getKey());
      }
    }
    LOG.debug("CACHE --- Removing " + expiredKeys.size() + " expired entries from cache");
    cache.keySet().removeAll(expiredKeys);
  }

  static void invalidateCachesForAllTokens() {
    synchronized (cacheLock) {
      kmsClientCachePerToken.clear();
      writeKEKMapPerToken.clear();
    }
  }

  public static String getAccessTokenOrDefault(Configuration configuration) {
    return configuration.getTrimmed(KEY_ACCESS_TOKEN_PROPERTY_NAME, DEFAULT_ACCESS_TOKEN); // default for KMS without token
  }

  private byte[] getEncryptionKeyMetadata(byte[] dataKey, String masterKeyID, boolean isFooterKey, 
      FileKeyMaterialStore targetKeyMaterialStore, String keyIdInFile) {
    if (null == kmsClient) {
      throw new ParquetCryptoRuntimeException("No KMS client available. See previous errors.");
    }

    KeyEncryptionKey keyEncryptionKey = null;
    String encodedWrappedDEK = null;
    if (!doubleWrapping) {
      try {
      encodedWrappedDEK = kmsClient.wrapDataKey(dataKey, masterKeyID);
      } catch (IOException e) {
        throw new ParquetCryptoRuntimeException(e);
      }
    } else {
     // Find or generate KEK for Master Key ID
      keyEncryptionKey = writeSessionKEKMap.computeIfAbsent(masterKeyID, (k) -> {
          LOG.debug("CACHE --- get KEK " + masterKeyID + " and add to write-KEK cache for token: " + formatTokenForLog(accessToken));
          return getKeyEncryptionKey(masterKeyID);
      });
      
      // Encrypt DEK with KEK
      byte[] AAD = keyEncryptionKey.getID();
      encodedWrappedDEK = KeyToolUtilities.wrapKeyLocally(dataKey, keyEncryptionKey.getBytes(), AAD);
    }

    // Pack all into key material JSON
    Map<String, String> keyMaterialMap = new HashMap<String, String>(10);
    if (null == targetKeyMaterialStore) {
      keyMaterialMap.put(KEY_METADATA_STORAGE_FIELD, keyMetadataInternalStorage); // TODO use/check
    }
    keyMaterialMap.put(WRAPPING_METHOD_FIELD, wrappingMethod);
    keyMaterialMap.put(WRAPPING_METHOD_VERSION_FIELD, wrapping_method_version);
    keyMaterialMap.put(MASTER_KEY_ID_FIELD, masterKeyID);
    if (isFooterKey) { // Add KMS metadata
      keyMaterialMap.put(KMS_INSTANCE_ID_FIELD, kmsInstanceID);
      keyMaterialMap.put(KMS_INSTANCE_URL_FIELD, kmsInstanceURLForParquetWrite);
    }
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
          keyIdInFile = "kf"; // TODO
        } else {
          keyIdInFile = "k" + keyCounter; // TODO
          keyCounter++;
        }
      }
      targetKeyMaterialStore.addKeyMaterial(keyIdInFile, keyMaterial);
      keyMetadata = createKeyReferenceMetadata(keyIdInFile, targetKeyMaterialStore.getStorageLocation())
          .getBytes(StandardCharsets.UTF_8);
    }  else {
      keyMetadata  = keyMaterial.getBytes(StandardCharsets.UTF_8);
    }

    return keyMetadata;
  }

  private KeyEncryptionKey getKeyEncryptionKey(String masterKeyID) {
    byte[] kekBytes = new byte[16]; //TODO length. configure via properties
    random.nextBytes(kekBytes);

    byte[] kekID = new byte[16];  //TODO length. configure via properties
    random.nextBytes(kekID);
    String encodedKEK_ID = Base64.getEncoder().encodeToString(kekID);

    // Encrypt KEK with Master key
    String encodedWrappedKEK = null;
    try {
      encodedWrappedKEK = kmsClient.wrapDataKey(kekBytes, masterKeyID);
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException(e);
    }

    return new KeyEncryptionKey(kekBytes, encodedKEK_ID, kekID, encodedWrappedKEK);
  }

  /**
   * Create and initialize a KMSClient. Called when KMS instance ID should be known.
   * @param configuration
   * @param kmsInstanceID
   * @return
   * @throws IOException
   */
  static KmsClient getKmsClient(Configuration configuration, String kmsInstanceID) 
      throws ParquetCryptoRuntimeException {

    String currentAccessToken = getAccessTokenOrDefault(configuration);
    ExpiringCacheEntry<ConcurrentMap<String, KmsClient>> kmsClientCachePerTokenEntry =
      kmsClientCachePerToken.get(currentAccessToken);

    if ((null == kmsClientCachePerTokenEntry) || kmsClientCachePerTokenEntry.isExpired()) {
      synchronized (cacheLock) {
        kmsClientCachePerTokenEntry = kmsClientCachePerToken.get(currentAccessToken);
        if ((null == kmsClientCachePerTokenEntry) || kmsClientCachePerTokenEntry.isExpired()) {
          ConcurrentHashMap<String, KmsClient> kmsClientPerToken = new ConcurrentHashMap<>();
          long cacheEntryLifetime = configuration.getLong(CACHE_ENTRY_LIFETIME_PROPERTY_NAME, DEFAULT_CACHE_ENTRY_LIFETIME);
          kmsClientCachePerTokenEntry = new ExpiringCacheEntry<>(kmsClientPerToken, cacheEntryLifetime);
          kmsClientCachePerToken.put(currentAccessToken, kmsClientCachePerTokenEntry);
        }
      }
      }
    final ConcurrentMap<String, KmsClient> kmsClientPerKmsInstanceCache = kmsClientCachePerTokenEntry.getCachedItem();
    KmsClient kmsClient =
      kmsClientPerKmsInstanceCache.computeIfAbsent(kmsInstanceID,
        (k) -> createKmsClient(configuration, kmsInstanceID, currentAccessToken));

    return kmsClient;
      }

  private static KmsClient createKmsClient(Configuration configuration, String kmsInstanceID, String accessToken) {
    KmsClient kmsClient;
      try {
      kmsClient = instantiateKmsClient(configuration);
        kmsClient.initialize(configuration, kmsInstanceID);
    } catch (IOException e) {
        LOG.info("Cannot create KMS client. If encryption.kms.instance.id not defined, will be expecting to use " +
            "default KMS instance ID, if relevant, or key metadata from parquet file.");
        LOG.debug("Cannot create KMS client.", e);
        return null;
      }
    if (kmsClient == null) return null;
    LOG.debug("CACHE --- KmsClent instantiated and configured.");
    return kmsClient;
  }

  private static KmsClient instantiateKmsClient(Configuration configuration) {
    Class<?> kmsClientClass = null;
    KmsClient kmsClient = null;

    try {
      kmsClientClass = ConfigurationUtil.getClassFromConfig(configuration,
          KMS_CLIENT_CLASS_PROPERTY_NAME, KmsClient.class);

      if (null == kmsClientClass) {
        throw new ParquetCryptoRuntimeException("Unspecified encryption.kms.client.class"); //TODO
      }
        kmsClient = (KmsClient)kmsClientClass.newInstance();
    } catch (InstantiationException | IllegalAccessException | BadConfigurationException e) {
      throw new ParquetCryptoRuntimeException("Could not instantiate KmsClient class: "
            + kmsClientClass, e);
      }

    return kmsClient;
  }

  private static String createKeyReferenceMetadata(String keyReference, String targetStorageLocation) {
    Map<String, String> keyMetadataMap = new HashMap<String, String>(2);
    keyMetadataMap.put(KEY_METADATA_STORAGE_FIELD, targetStorageLocation);
    keyMetadataMap.put(KEY_REFERENCE_FIELD, keyReference);

    String keyMetadata;
    try {
      keyMetadata = objectMapper.writeValueAsString(keyMetadataMap);
    } catch (Exception e) {
      throw new ParquetCryptoRuntimeException("Failed to serialize key material", e);
    }

    return keyMetadata;
  }

  private static String formatTokenForLog(String accessToken) {
    int maxTokenDisplayLength = 5;
    if (accessToken.length() <= maxTokenDisplayLength) {
      return accessToken;
    }
    return accessToken.substring(accessToken.length() - maxTokenDisplayLength);
  }
}
