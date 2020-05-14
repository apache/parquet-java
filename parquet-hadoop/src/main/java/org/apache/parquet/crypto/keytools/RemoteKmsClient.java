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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * An abstract class for implementation of a remote-KMS client.
 * Both KMS instance ID and KMS URL need to be defined in order to access such a KMS.
 * The concrete implementation should implement getKeyFromServer() and/or
 * wrapKeyInServer() with unwrapKeyInServer() methods.
 */
public abstract class RemoteKmsClient implements KmsClient {
  public static final String KMS_INSTANCE_URL_PROPERTY_NAME = "encryption.kms.instance.url";
  public static final String WRAP_LOCALLY_PROPERTY_NAME = "encryption.wrap.locally";

  protected String kmsInstanceID;
  protected String kmsURL;
  protected Boolean isWrapLocally;

  private final int INITIAL_KEY_CACHE_SIZE = 10;
  // MasterKey cache: master keys per key ID (per KMS Client). For local wrapping only.
  private final Map<String, ExpiringCacheEntry<byte[]>> masterKeyCache = new HashMap<>(INITIAL_KEY_CACHE_SIZE);
  private long cacheEntryLifetime;
  private volatile long lastCacheCleanupTimestamp = System.currentTimeMillis() + 60l * 1000; // grace period of 1 minute

  /**
   *  Initialize the KMS Client with KMS instance ID and URL.
   *
   *  When reading a parquet file:
   *  - the KMS instance ID can be either specified in configuration or read from parquet file key material,
   *  or be a default if there is a default value for this KMS type.
   *
   *  When writing a parquet file:
   *  - the KMS instance ID has to be specified in configuration, or be a default if there is a default value for this KMS type.
   *  - The KMS URL has to be specified in configuration specifically, or be a default if there is a default value for this KMS type.
   *
   * @param configuration Hadoop configuration
   * @param kmsInstanceID instance ID of the KMS managed by this KmsClient
   * @throws IOException
   */
  @Override
  public void initialize(Configuration configuration, String kmsInstanceID) {
    this.kmsInstanceID = kmsInstanceID;
    this.kmsURL = configuration.getTrimmed(KMS_INSTANCE_URL_PROPERTY_NAME);
    this.isWrapLocally = configuration.getBoolean(WRAP_LOCALLY_PROPERTY_NAME, false);
    this.cacheEntryLifetime = 1000 * configuration.getLong(FileKeyWrapper.TOKEN_LIFETIME_PROPERTY_NAME, 
        FileKeyWrapper.DEFAULT_CACHE_ENTRY_LIFETIME);
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
   * Local wrapping only.
   * Get a master key from server. First check if the key is in local key cache and the cache entry is not expired.
   * If it is - return the key from the cache entry. Otherwise - getKeyFromServerRemoteCall.
   * @param keyIdentifier: a string that uniquely identifies the key in KMS
   * @return master encryption key
   */

  private byte[] getKeyFromCacheOrServer(String keyIdentifier) {
    // Check caches upon each key retrieval (clean once in cacheEntryLifetime)
    checkExpiredMasterdKeyCacheEntries();
    ExpiringCacheEntry<byte[]> keyCacheEntry = masterKeyCache.get(keyIdentifier);
    if ((null == keyCacheEntry) || keyCacheEntry.isExpired()) {
      synchronized (masterKeyCache) {
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

  private void checkExpiredMasterdKeyCacheEntries() {
    long now = System.currentTimeMillis();
    if (now < (lastCacheCleanupTimestamp + cacheEntryLifetime)) {
      return;
    }

    synchronized (masterKeyCache) {
      if (now < (lastCacheCleanupTimestamp + cacheEntryLifetime)) {
        return;
      }
      KeyToolUtilities.removeExpiredEntriesFromCache(masterKeyCache);
      lastCacheCleanupTimestamp = now;
    }
  }

  /**
   * Get master key from server - call the remote KMS server, without using the key cache.
   * This method should be implemented by the concrete RemoteKmsClient implementation,
   * or otherwise  throw UnsupportedOperationException.
   */
  protected abstract byte[] getMasterKeyFromServer(String masterKeyIdentifier) 
      throws KeyAccessDeniedException, UnsupportedOperationException;

  /**
   * Wrap key bytes with master key in remote KMS server.
   * This method should be implemented by the concrete RemoteKmsClient implementation,
   * or otherwise  throw UnsupportedOperationException.
   */
  protected abstract String wrapKeyInServer(byte[] keyBytes, String masterKeyIdentifier) 
      throws KeyAccessDeniedException, UnsupportedOperationException;

  /**
   * Unwrap key bytes with master key in remote KMS server.
   * This method should be implemented by the concrete RemoteKmsClient implementation,
   * or otherwise  throw UnsupportedOperationException.
   */
  protected abstract byte[] unwrapKeyInServer(String wrappedKey, String masterKeyIdentifier) 
      throws KeyAccessDeniedException, UnsupportedOperationException;

  protected abstract void initializeInternal(Configuration configuration) 
      throws KeyAccessDeniedException;
}