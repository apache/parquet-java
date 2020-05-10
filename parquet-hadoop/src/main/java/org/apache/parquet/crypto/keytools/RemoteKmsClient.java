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

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An abstract class for implementation of a remote-KMS client.
 * Both KMS instance ID and KMS URL need to be defined in order to access such a KMS.
 * The concrete implementation should implement getKeyFromServer() and/or
 * wrapDataKeyInServer() with unwrapDataKeyInServer() methods.
 */
public abstract class RemoteKmsClient implements KmsClient {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteKmsClient.class);

  public static final String KMS_INSTANCE_ID_PROPERTY_NAME = "encryption.kms.instance.id";
  public static final String KMS_INSTANCE_URL_PROPERTY_NAME = "encryption.kms.instance.url";
  public static final String KMS_INSTANCE_URL_LIST_PROPERTY_NAME = "encryption.kms.instance.url.list";
  public static final String WRAP_LOCALLY_PROPERTY_NAME = "encryption.wrap.locally";
  public static final String CACHE_ENTRY_LIFETIME_PROPERTY_NAME = "encryption.cache.entry.lifetime";
  
  private static final long DEFAULT_CACHE_ENTRY_LIFETIME = 10 * 60 * 1000; // 10 minutes

  protected String kmsInstanceID;
  protected String kmsURL;
  // Example value that matches the pattern:    vault-instance-1: http://127.0.0.1:8200
  protected Pattern kmsUrlListItemPattern = Pattern.compile("^(\\S+)\\s*:\\s*(\\w*://\\S+)$");
  protected Boolean isWrapLocally;

  // MasterKey cache: key material per master key ID, concurrent since it can be shared by multiple threads with the same
  // KMS instance id and access token
  private final int INITIAL_KEY_CACHE_SIZE = 10;
  private final ConcurrentMap<String, ExpiringCacheEntry<byte[]>> masterKeyCache =
    new ConcurrentHashMap<>(INITIAL_KEY_CACHE_SIZE);
  private final Object cacheLock = new Object();
  private long cacheEntryLifetime;
  private volatile Long lastCacheCleanupTimestamp = System.currentTimeMillis() + 60l * 1000; // grace period of 1 minute

  /**
   *  Initialize the KMS Client with KMS instance ID and URL.
   *
   *  When reading a parquet file:
   *  - the KMS instance ID can be either specified in configuration or read from parquet file metadata,
   *  or default if there is a default value for this KMS type.
   *  - The KMS URL has to be specified in configuration either specifically or as a mapping of KMS instance ID to KMS URL,
   *  e.g. { "kmsInstanceID1": "kmsURL1", "kmsInstanceID2" : "kmsURL2" }, but not both.
   *  The list supports scenario of reading files using kms instances different from those used for writing,
   *  with keys exported/imported between the instances.
   *
   *  When writing a parquet file:
   *  - the KMS instance ID has to be specified in configuration or default if there is a default value for this KMS type.
   *  - The KMS URL has to be specified in configuration specifically or default
   *
   * @param configuration Hadoop configuration
   * @param kmsInstanceID instance ID of the KMS managed by this KmsClient. When reading a parquet file, the KMS
   *                      instance ID can be either specified in configuration or read from parquet file metadata.
   *                      When writing a parquet file, the KMS instance ID has to be specified in configuration.
   *                      KMSClient implementation could have a default value for this.
   * @throws IOException
   */
  @Override
  public void initialize(Configuration configuration, String kmsInstanceID) throws IOException, KeyAccessDeniedException, UnsupportedOperationException {
    this.kmsInstanceID = kmsInstanceID;
    setKmsURL(configuration);
    this.isWrapLocally = configuration.getBoolean(WRAP_LOCALLY_PROPERTY_NAME, false);
    this.cacheEntryLifetime = configuration.getLong(CACHE_ENTRY_LIFETIME_PROPERTY_NAME, DEFAULT_CACHE_ENTRY_LIFETIME);
    initializeInternal(configuration);
  }

  @Override
  public String wrapDataKey(byte[] dataKey, String masterKeyIdentifier) throws IOException, KeyAccessDeniedException, UnsupportedOperationException {
    if (isWrapLocally) {
      byte[] masterKey = getKeyFromCacheOrServer(masterKeyIdentifier);
      byte[] AAD = masterKeyIdentifier.getBytes(StandardCharsets.UTF_8);
      return KeyToolUtilities.wrapKeyLocally(dataKey, masterKey, AAD);
    } else {
      return wrapDataKeyInServer(dataKey, masterKeyIdentifier);
    }
  }

  @Override
  public byte[] unwrapDataKey(String wrappedKey, String masterKeyIdentifier) throws IOException, KeyAccessDeniedException, UnsupportedOperationException {
      if (isWrapLocally) {
        byte[] masterKey = getKeyFromCacheOrServer(masterKeyIdentifier);
        byte[] AAD = masterKeyIdentifier.getBytes(StandardCharsets.UTF_8);
        return KeyToolUtilities.unwrapKeyLocally(wrappedKey, masterKey, AAD);
      } else {
        return unwrapDataKeyInServer(wrappedKey, masterKeyIdentifier);
      }
  }

  protected abstract void initializeInternal(Configuration configuration) throws IOException, KeyAccessDeniedException, UnsupportedOperationException;

  private void setKmsURL(Configuration configuration) throws IOException {
    final String kmsUrlProperty = configuration.getTrimmed(KMS_INSTANCE_URL_PROPERTY_NAME);
    final String[] kmsUrlList = configuration.getTrimmedStrings(KMS_INSTANCE_URL_LIST_PROPERTY_NAME);
    if (StringUtils.isEmpty(kmsUrlProperty) && ArrayUtils.isEmpty(kmsUrlList) || "DEFAULT".equals(kmsUrlProperty)) {
      throw new IOException("KMS URL is not set.");
    }
    if (!StringUtils.isEmpty(kmsUrlProperty) && !ArrayUtils.isEmpty(kmsUrlList)) {
      throw new IOException("KMS URL is ambiguous: " +
          "it should either be set in encryption.kms.instance.url or in encryption.kms.instance.url.list"); // TODO use constants
    }
    if (!StringUtils.isEmpty(kmsUrlProperty)) {
      kmsURL = kmsUrlProperty;
    } else {
      if (StringUtils.isEmpty(kmsInstanceID) ) {
        throw new IOException("Missing kms instance id value. Cannot find a matching KMS URL mapping.");
      }
      Map<String, String> kmsUrlMap = new HashMap<String, String>(kmsUrlList.length);
      int nKeys = kmsUrlList.length;
      for (int i=0; i < nKeys; i++) {
        Matcher m = kmsUrlListItemPattern.matcher(kmsUrlList[i]);
        if (!m.matches() || (m.groupCount() != 2)) {
          throw new IOException(String.format("String %s doesn't match pattern %s for KMS URL mapping",
              kmsUrlList[i], kmsUrlListItemPattern.toString()));
        }
        String instanceID = m.group(1);
        String kmsURL = m.group(2);
        //TODO check parts
        kmsUrlMap.put(instanceID, kmsURL);
      }      kmsURL = kmsUrlMap.get(kmsInstanceID);
      if (StringUtils.isEmpty(kmsURL) ) {
        throw new IOException(String.format("Missing KMS URL for kms instance ID [%s] in KMS URL mapping",
            kmsInstanceID));
      }
    }
  }

  /**
   * Get a standard key from server. First check if the key is in local key cache and the cache entry is not expired.
   * If it is - return the key from the cache entry. Otherwise - getKeyFromServerRemoteCall.
   * @param keyIdentifier: a string that uniquely identifies the key in KMS:
   * ranging from a simple key ID, to e.g. a JSON with key ID, KMS instance etc.
   * @return
   * @throws UnsupportedOperationException
   * @throws KeyAccessDeniedException
   * @throws IOException
   */

  private byte[] getKeyFromCacheOrServer(String keyIdentifier) throws IOException {
    invalidateExpiredKeyCacheEntries();
    ExpiringCacheEntry<byte[]> keyCacheEntry = null;
    ExpiringCacheEntry<byte[]> expiringCacheEntry = masterKeyCache.get(keyIdentifier);
    if ((null == expiringCacheEntry) || expiringCacheEntry.isExpired()) {
      synchronized (cacheLock) {
        expiringCacheEntry = masterKeyCache.get(keyIdentifier);
        if ((null == expiringCacheEntry) || expiringCacheEntry.isExpired()) {
          byte[] key = getKeyFromServer(keyIdentifier);
          keyCacheEntry = new ExpiringCacheEntry<>(key, cacheEntryLifetime);
          masterKeyCache.put(keyIdentifier, keyCacheEntry);
      }
    }
  }
    return keyCacheEntry.getCachedItem();
  }

  private void invalidateExpiredKeyCacheEntries() {
    long now = System.currentTimeMillis();
    if (now > lastCacheCleanupTimestamp + this.cacheEntryLifetime) {
      synchronized (cacheLock) {
        if (now > lastCacheCleanupTimestamp + this.cacheEntryLifetime) {
          Set<Map.Entry<String, ExpiringCacheEntry<byte[]>>> cacheEntries = masterKeyCache.entrySet();
          List<String> expiredKeys = new ArrayList<>(cacheEntries.size());
          for (Map.Entry<String, ExpiringCacheEntry<byte[]>> cacheEntry : cacheEntries) {
            if (cacheEntry.getValue().isExpired()) {
              expiredKeys.add(cacheEntry.getKey());
            }
          }
          LOG.debug("CACHE --- Removing " + expiredKeys.size() + " expired key entries from cache");
          masterKeyCache.keySet().removeAll(expiredKeys);
          lastCacheCleanupTimestamp = now;
        }
    }
      }
    }

    /**
   * Get a standard key from server - call the remote server, without using the key cache.
   * This method should be implemented by the concrete RemoteKmsClient implementation,
   * or otherwise  throw UnsupportedOperationException.
     */
  protected abstract byte[] getKeyFromServer(String keyIdentifier) throws IOException, KeyAccessDeniedException, UnsupportedOperationException;

  protected abstract String wrapDataKeyInServer(byte[] dataKey, String masterKeyIdentifier) throws IOException, KeyAccessDeniedException, UnsupportedOperationException;

  protected abstract byte[] unwrapDataKeyInServer(String wrappedKey, String masterKeyIdentifier) throws IOException, KeyAccessDeniedException, UnsupportedOperationException;

}
