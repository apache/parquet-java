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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

/**
 * An abstract class for implementation of a remote-KMS client.
 * Both KMS instance ID and KMS URL need to be defined in order to access such a KMS.
 * The concrete implementation should implement getKeyFromServer() and/or
 * wrapKeyInServer() with unwrapKeyInServer() methods.
 */
public abstract class RemoteKmsClient implements KmsClient {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteKmsClient.class);

  public static final String KMS_INSTANCE_URL_PROPERTY_NAME = "encryption.kms.instance.url";
  //public static final String KMS_INSTANCE_URL_LIST_PROPERTY_NAME = "encryption.kms.instance.url.list";
  public static final String WRAP_LOCALLY_PROPERTY_NAME = "encryption.wrap.locally";

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
  public void initialize(Configuration configuration, String kmsInstanceID) {
    this.kmsInstanceID = kmsInstanceID;
    this.kmsURL = configuration.getTrimmed(KMS_INSTANCE_URL_PROPERTY_NAME);
    this.isWrapLocally = configuration.getBoolean(WRAP_LOCALLY_PROPERTY_NAME, false);
    this.cacheEntryLifetime = 1000 * configuration.getLong(EnvelopeKeyManager.TOKEN_LIFETIME_PROPERTY_NAME, 
        DEFAULT_CACHE_ENTRY_LIFETIME);
    initializeInternal(configuration);
  }

  @Override
  public String wrapKey(byte[] dataKey, String masterKeyIdentifier) throws KeyAccessDeniedException {
    if (isWrapLocally) {
      byte[] masterKey = getKeyFromCacheOrServer(masterKeyIdentifier);
      byte[] AAD = masterKeyIdentifier.getBytes(StandardCharsets.UTF_8);
      return KeyToolUtilities.wrapKeyLocally(dataKey, masterKey, AAD);
    } else {
      return wrapKeyInServer(dataKey, masterKeyIdentifier);
    }
  }

  @Override
  public byte[] unwrapKey(String wrappedKey, String masterKeyIdentifier) throws KeyAccessDeniedException, UnsupportedOperationException {
    if (isWrapLocally) {
      byte[] masterKey = getKeyFromCacheOrServer(masterKeyIdentifier);
      byte[] AAD = masterKeyIdentifier.getBytes(StandardCharsets.UTF_8);
      return KeyToolUtilities.unwrapKeyLocally(wrappedKey, masterKey, AAD);
    } else {
      return unwrapKeyInServer(wrappedKey, masterKeyIdentifier);
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

  private byte[] getKeyFromCacheOrServer(String keyIdentifier) {
    invalidateExpiredKeyCacheEntries();
    ExpiringCacheEntry<byte[]> keyCacheEntry = masterKeyCache.get(keyIdentifier);
    if ((null == keyCacheEntry) || keyCacheEntry.isExpired()) {
      synchronized (cacheLock) {
        keyCacheEntry = masterKeyCache.get(keyIdentifier);
        if ((null == keyCacheEntry) || keyCacheEntry.isExpired()) {
          byte[] key = getMasterKeyFromServer(keyIdentifier);
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
   * Get master key from server - call the remote server, without using the key cache.
   * This method should be implemented by the concrete RemoteKmsClient implementation,
   * or otherwise  throw UnsupportedOperationException.
   */
  protected abstract byte[] getMasterKeyFromServer(String masterKeyIdentifier) 
      throws KeyAccessDeniedException, UnsupportedOperationException;

  protected abstract String wrapKeyInServer(byte[] dataKey, String masterKeyIdentifier) 
      throws KeyAccessDeniedException, UnsupportedOperationException;

  protected abstract byte[] unwrapKeyInServer(String wrappedKey, String masterKeyIdentifier) 
      throws KeyAccessDeniedException, UnsupportedOperationException;

  protected abstract void initializeInternal(Configuration configuration) 
      throws KeyAccessDeniedException;
}