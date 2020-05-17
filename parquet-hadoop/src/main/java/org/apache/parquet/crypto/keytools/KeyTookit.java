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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.AesGcmDecryptor;
import org.apache.parquet.crypto.AesGcmEncryptor;
import org.apache.parquet.crypto.AesMode;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.apache.parquet.crypto.ModuleCipherFactory;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.hadoop.util.HiddenFileFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class KeyTookit {

  public static final String KMS_CLIENT_CLASS_PROPERTY_NAME = "encryption.kms.client.class";
  public static final String KMS_INSTANCE_ID_PROPERTY_NAME = "encryption.kms.instance.id";
  public static final String DOUBLE_WRAPPING_PROPERTY_NAME = "encryption.double.wrapping";
  public static final String KEY_ACCESS_TOKEN_PROPERTY_NAME = "encryption.key.access.token";
  public static final String TOKEN_LIFETIME_PROPERTY_NAME = "encryption.key.access.token.lifetime";
  public static final String KMS_INSTANCE_URL_PROPERTY_NAME = "encryption.kms.instance.url";
  public static final String WRAP_LOCALLY_PROPERTY_NAME = "encryption.wrap.locally";
  public static final String KEY_MATERIAL_INTERNAL_PROPERTY_NAME = "encryption.key.material.internal.storage";

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

  public static final String DEFAULT_KMS_INSTANCE_ID = "DEFAULT";
  public static final String DEFAULT_KMS_INSTANCE_URL = "DEFAULT";
  public static final String DEFAULT_ACCESS_TOKEN = "DEFAULT";

  public static final String FOOTER_KEY_ID_IN_FILE = "kf";
  public static final String KEY_ID_IN_FILE_PREFIX = "k";

  public static final long DEFAULT_CACHE_ENTRY_LIFETIME = 10 * 60; // 10 minutes
  public static final int INITIAL_PER_TOKEN_CACHE_SIZE = 5;
  public static final String TEMP_FILE_PREFIX = "_TMP";


  // For every token: a map of KMSInstanceId to kmsClient
  private static final Map<String, ExpiringCacheEntry<Map<String, KmsClient>>> kmsClientCachePerToken =
      new HashMap<>(INITIAL_PER_TOKEN_CACHE_SIZE);
  private static volatile long lastKmsCacheCleanupTimestamp = System.currentTimeMillis() + 60l * 1000; // grace period of 1 minute

  static class KeyWithMasterID {

    private final byte[] keyBytes;
    private final String masterID ;

    KeyWithMasterID(byte[] keyBytes, String masterID) {
      this.keyBytes = keyBytes;
      this.masterID = masterID;
    }

    byte[] getDataKey() {
      return keyBytes;
    }

    String getMasterID() {
      return masterID;
    }
  }

  static class KeyEncryptionKey {
    private final byte[] kekBytes;
    private final byte[] kekID;
    private final String encodedKEK_ID;
    private final String encodedWrappedKEK;

    KeyEncryptionKey(byte[] kekBytes, String encodedKEK_ID, byte[] kekID, String encodedWrappedKEK) {
      this.kekBytes = kekBytes;
      this.kekID = kekID;
      this.encodedKEK_ID = encodedKEK_ID;
      this.encodedWrappedKEK = encodedWrappedKEK;
    }

    byte[] getBytes() {
      return kekBytes;
    }

    byte[] getID() {
      return kekID;
    }

    String getEncodedID() {
      return encodedKEK_ID;
    }

    String getWrappedWithCRK() {
      return encodedWrappedKEK;
    }
  }

  public static void rotateMasterKeys(String folderPath, Configuration hadoopConfig)
      throws IOException, ParquetCryptoRuntimeException, KeyAccessDeniedException {

    Path parentPath = new Path(folderPath);

    FileSystem hadoopFileSystem = parentPath.getFileSystem(hadoopConfig);

    FileStatus[] keyMaterialFiles = hadoopFileSystem.listStatus(parentPath, HiddenFileFilter.INSTANCE);

    for (FileStatus fs : keyMaterialFiles) {
      Path parquetFile = fs.getPath();

      FileKeyMaterialStore sourceKeyMaterialStore = new HadoopFSKeyMaterialStore(hadoopFileSystem, parquetFile);

      FileKeyWrapper fileKeyWrapper = new FileKeyWrapper(hadoopConfig, sourceKeyMaterialStore);

      FileKeyMaterialStore tempKeyMaterialStore = new HadoopFSKeyMaterialStore(hadoopFileSystem, parquetFile, TEMP_FILE_PREFIX);

      FileKeyUnwrapper fileKeyUnwrapper = new FileKeyUnwrapper(hadoopConfig, sourceKeyMaterialStore);

      Set<String> fileKeyIdSet = sourceKeyMaterialStore.getKeyIDSet();

      for (String keyIdInFile : fileKeyIdSet) {
        boolean footerKey = keyIdInFile.equals(FOOTER_KEY_ID_IN_FILE);
        String keyMaterial = sourceKeyMaterialStore.getKeyMaterial(keyIdInFile);
        KeyWithMasterID key = fileKeyUnwrapper.getDEKandMasterID(keyMaterial);
        fileKeyWrapper.getEncryptionKeyMetadata(key.getDataKey(), key.getMasterID(), footerKey, tempKeyMaterialStore, keyIdInFile);
      }

      tempKeyMaterialStore.saveMaterial();

      sourceKeyMaterialStore.removeMaterial();

      tempKeyMaterialStore.moveMaterial(sourceKeyMaterialStore);

      // Clear all per-token caches
      synchronized (kmsClientCachePerToken) {
        kmsClientCachePerToken.clear();
      }
      FileKeyWrapper.removeCacheEntriesForAllTokens();
      FileKeyUnwrapper.removeCacheEntriesForAllTokens();

    }
  }

  public static String wrapKeyLocally(byte[] key, byte[] wrappingKey, byte[] AAD) {
    AesGcmEncryptor keyEncryptor;

    keyEncryptor = (AesGcmEncryptor) ModuleCipherFactory.getEncryptor(AesMode.GCM, wrappingKey);

    byte[] wrappedKey = keyEncryptor.encrypt(false, key, AAD);

    return Base64.getEncoder().encodeToString(wrappedKey);
  }

  public static byte[] unwrapKeyLocally(String encodedWrappedKey, byte[] wrappingKey, byte[] AAD) {
    byte[] wrappedKEy = Base64.getDecoder().decode(encodedWrappedKey);
    AesGcmDecryptor keyDecryptor;

    keyDecryptor = (AesGcmDecryptor) ModuleCipherFactory.getDecryptor(AesMode.GCM, wrappingKey);

    return keyDecryptor.decrypt(wrappedKEy, 0, wrappedKEy.length, AAD);
  }

  /**
   * Flush any caches that are tied to the specified accessToken
   * @param accessToken
   */
  public static void removeCacheEntriesForToken(String accessToken) {
    synchronized (kmsClientCachePerToken) {
      kmsClientCachePerToken.remove(accessToken);
    }

    FileKeyWrapper.removeCacheEntriesForToken(accessToken);

    FileKeyUnwrapper.removeCacheEntriesForToken(accessToken);
  }


  static <E> void checkCacheEntriesForExpiredTokens(Map<String, ExpiringCacheEntry<E>> cache, long lastCacheCleanupTimestamp, 
      long cacheEntryLifetime) {
    long now = System.currentTimeMillis();

    if (now > (lastCacheCleanupTimestamp + cacheEntryLifetime)) {
      synchronized (cache) {
        if (now > (lastCacheCleanupTimestamp + cacheEntryLifetime)) {
          removeExpiredEntriesFromCache(cache);
          lastCacheCleanupTimestamp = now;
        }
      }
    }
  }

  static void checkKmsCacheForExpiredTokens(long cacheEntryLifetime) {
    checkCacheEntriesForExpiredTokens(kmsClientCachePerToken, lastKmsCacheCleanupTimestamp, cacheEntryLifetime);
  }

  static KmsClient getKmsClient(String kmsInstanceID, Configuration configuration, String accessToken, long cacheEntryLifetime) {
    // Try cache first
    ExpiringCacheEntry<Map<String, KmsClient>> kmsClientCachePerTokenEntry;
    synchronized (kmsClientCachePerToken) {
      kmsClientCachePerTokenEntry = kmsClientCachePerToken.get(accessToken);
      if ((null == kmsClientCachePerTokenEntry) || kmsClientCachePerTokenEntry.isExpired()) {
        Map<String, KmsClient> kmsClientPerToken = new HashMap<>();

        kmsClientCachePerTokenEntry = new ExpiringCacheEntry<>(kmsClientPerToken, cacheEntryLifetime);
        kmsClientCachePerToken.put(accessToken, kmsClientCachePerTokenEntry);
      }

    }
    
    final Map<String, KmsClient> kmsClientPerKmsInstanceCache = kmsClientCachePerTokenEntry.getCachedItem();
    KmsClient kmsClient; 
    synchronized (kmsClientPerKmsInstanceCache) {
      kmsClient = kmsClientPerKmsInstanceCache.get(kmsInstanceID);
      if (null == kmsClient) {
        // Not in cache. Create, init and put in cache
        kmsClient = createAndInitKmsClient(kmsInstanceID, configuration, accessToken);
        kmsClientPerKmsInstanceCache.put(kmsInstanceID, kmsClient);
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

  static String formatTokenForLog(String accessToken) {
    int maxTokenDisplayLength = 5;
    if (accessToken.length() <= maxTokenDisplayLength) {
      return accessToken;
    }
    return accessToken.substring(accessToken.length() - maxTokenDisplayLength);
  }

  static <E> void removeExpiredEntriesFromCache(Map<String, ExpiringCacheEntry<E>> cache) {
    Set<Map.Entry<String, ExpiringCacheEntry<E>>> cacheEntries = cache.entrySet();
    List<String> expiredKeys = new ArrayList<>(cacheEntries.size());
    for (Map.Entry<String, ExpiringCacheEntry<E>> cacheEntry : cacheEntries) {
      if (cacheEntry.getValue().isExpired()) {
        expiredKeys.add(cacheEntry.getKey());
      }
    }
    cache.keySet().removeAll(expiredKeys);
  }
}