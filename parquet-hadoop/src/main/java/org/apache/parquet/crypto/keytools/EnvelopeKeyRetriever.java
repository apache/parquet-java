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
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.keytools.KeyToolUtilities.KeyWithMasterID;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvelopeKeyRetriever implements DecryptionKeyRetriever {
  private static final Logger LOG = LoggerFactory.getLogger(EnvelopeKeyRetriever.class);

  // For every token a map of KEK_ID to KEK bytes
  private static final ConcurrentMap<String, ExpiringCacheEntry<ConcurrentMap<String,byte[]>>> readKEKMapPerToken
  = new ConcurrentHashMap<>();
  private static final Object readKEKCacheLock = new Object();
  private static volatile Long lastCacheCleanupTimestamp = System.currentTimeMillis() + 60l * 1000; // grace period of 1 minute
  private final String accessToken;

  // A map of KEK_ID to KEK bytes for the current token
  private ConcurrentMap<String,byte[]> readSessionKEKMap;
  private final long cacheEntryLifetime;
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private KmsClient kmsClient;
  private final FileKeyMaterialStore keyMaterialStore;
  private final Configuration hadoopConfiguration;

  public EnvelopeKeyRetriever(KmsClient kmsClient, Configuration hadoopConfiguration,
      FileKeyMaterialStore keyStore) {
    this.kmsClient = kmsClient;
    this.hadoopConfiguration = hadoopConfiguration;
    this.keyMaterialStore = keyStore;

    this.accessToken = EnvelopeKeyManager.getAccessTokenOrDefault(hadoopConfiguration);

    this.cacheEntryLifetime = 1000 * hadoopConfiguration.getLong(EnvelopeKeyManager.TOKEN_LIFETIME_PROPERTY_NAME,
        EnvelopeKeyManager.DEFAULT_CACHE_ENTRY_LIFETIME);
    invalidateCachesForExpiredTokens();
    ExpiringCacheEntry<ConcurrentMap<String, byte[]>> readKEKCacheEntry = readKEKMapPerToken.get(accessToken);
    if ((null == readKEKCacheEntry) || (readKEKCacheEntry.isExpired())) {
      synchronized (readKEKCacheLock) {
        if ((null == readKEKCacheEntry) || (readKEKCacheEntry.isExpired())) {
          LOG.debug("CACHE --- create read-KEK cache for token: " + accessToken.substring(accessToken.length() - 5));
          readKEKCacheEntry = new ExpiringCacheEntry<>(new ConcurrentHashMap<String,byte[]>(), cacheEntryLifetime);
          readKEKMapPerToken.put(accessToken, readKEKCacheEntry);
        }
      }
    }
    this.readSessionKEKMap = readKEKCacheEntry.getCachedItem();
  }

  @Override
  public byte[] getKey(byte[] keyMetaData) {
    String keyMaterial;
    if (null != keyMaterialStore) {
      String keyReferenceMetadata = new String(keyMetaData, StandardCharsets.UTF_8);
      String keyIDinFile = getKeyReference(keyReferenceMetadata);
      keyMaterial = keyMaterialStore.getKeyMaterial(keyIDinFile);
      if (null == keyMaterial) {
        throw new ParquetCryptoRuntimeException("Null key material for keyIDinFile: " + keyIDinFile);
      }
    }  else {
      keyMaterial = new String(keyMetaData, StandardCharsets.UTF_8);
    }

    return getDEKandMasterID(keyMaterial).getDataKey();
  }

  KeyWithMasterID getDEKandMasterID(String keyMaterial)  {
    Map<String, String> keyMaterialJson = null;
    try {
      keyMaterialJson = objectMapper.readValue(new StringReader(keyMaterial),
          new TypeReference<Map<String, String>>() {});
    }  catch (Exception e) {
      throw new ParquetCryptoRuntimeException("Failed to parse key material " + keyMaterial, e);
    }

    String keyMaterialType = keyMaterialJson.get(EnvelopeKeyManager.KEY_MATERIAL_TYPE_FIELD);
    if (null != keyMaterialType && !EnvelopeKeyManager.keyMaterialType.equals(keyMaterialType)) {
      throw new ParquetCryptoRuntimeException("Wrong key materila type: " + keyMaterialType + 
          " vs " + EnvelopeKeyManager.KEY_MATERIAL_TYPE_FIELD);
    }

    if (null == kmsClient) {
      kmsClient = getKmsClientFromKeyMaterial(hadoopConfiguration, keyMaterialJson);
    }

    boolean doubleWrapping = Boolean.valueOf(keyMaterialJson.get(EnvelopeKeyManager.DOUBLE_WRAPPING_FIELD));

    String masterKeyID = keyMaterialJson.get(EnvelopeKeyManager.MASTER_KEY_ID_FIELD);
    String encodedWrappedDatakey = keyMaterialJson.get(EnvelopeKeyManager.WRAPPED_DEK_FIELD);

    byte[] dataKey;
    if (!doubleWrapping) {
      dataKey = kmsClient.unwrapKey(encodedWrappedDatakey, masterKeyID);
    } else {
      // Get KEK
      String encodedKEK_ID = keyMaterialJson.get(EnvelopeKeyManager.KEK_ID_FIELD);
      final Map<String, String> keyMaterialJsonFinal = keyMaterialJson;
      byte[] kekBytes = readSessionKEKMap.computeIfAbsent(encodedKEK_ID, (k) -> {
        LOG.debug("CACHE --- get kek " + masterKeyID + " and add to read-KEK cache");
        return unwrapKek(keyMaterialJsonFinal, masterKeyID);
      });

      // Decrypt the data key
      byte[]  AAD = Base64.getDecoder().decode(encodedKEK_ID);
      dataKey =  KeyToolUtilities.unwrapKeyLocally(encodedWrappedDatakey, kekBytes, AAD);
    }

    return new KeyWithMasterID(dataKey, masterKeyID);
  }

  private byte[] unwrapKek(Map<String, String> keyMaterialJson, String masterKeyID) {
    byte[] kekBytes;
    String encodedWrappedKEK = keyMaterialJson.get(EnvelopeKeyManager.WRAPPED_KEK_FIELD);
    kekBytes = kmsClient.unwrapKey(encodedWrappedKEK, masterKeyID);

    if (null == kekBytes) {
      throw new ParquetCryptoRuntimeException("Null KEK, after unwrapping in KMS with master key " + masterKeyID);
    }
    return kekBytes;
  }

  /**
   * Create and initialize KmsClient based on properties saved in key metadata
   * @param hadoopConfiguration
   * @param keyMaterialJson key metadata
   * @return new KMSClient
   * @throws IOException
   */
  private static KmsClient getKmsClientFromKeyMaterial(Configuration hadoopConfiguration,
      Map<String, String> keyMaterialJson) {
    String kmsInstanceID = keyMaterialJson.get(EnvelopeKeyManager.KMS_INSTANCE_ID_FIELD);
    final String kmsInstanceURL = keyMaterialJson.get(EnvelopeKeyManager.KMS_INSTANCE_URL_FIELD);
    updateKmsInstanceURLInHadoopConfiguration(hadoopConfiguration, kmsInstanceURL);
    KmsClient kmsClient = EnvelopeKeyManager.getKmsClient(hadoopConfiguration, kmsInstanceID);
    if (null == kmsClient) {
      throw new ParquetCryptoRuntimeException("KMSClient was not successfully created for reading encrypted data.");
    }
    return kmsClient;
  }

  /**
   * If KMS instance URL is specified in encryption.kms.instance.url, then it overrides
   * the KMS instance URL that is read from file metadata.
   * @param hadoopConfiguration
   * @param kmsInstanceURL
   */
  private static void updateKmsInstanceURLInHadoopConfiguration(Configuration hadoopConfiguration, String kmsInstanceURL) {
    if (!StringUtils.isEmpty(kmsInstanceURL)) {
      final String kmsUrlProperty = hadoopConfiguration.getTrimmed(RemoteKmsClient.KMS_INSTANCE_URL_PROPERTY_NAME);
      if (StringUtils.isEmpty(kmsUrlProperty)) {
        LOG.debug("Updating KMS instance URL to: " + kmsInstanceURL);
        hadoopConfiguration.set(RemoteKmsClient.KMS_INSTANCE_URL_PROPERTY_NAME, kmsInstanceURL);
      }
    }
  }

  private static String getKeyReference(String keyReferenceMetadata) {
    Map<String, String> keyMetadataJson = null;
    try {
      keyMetadataJson = objectMapper.readValue(new StringReader(keyReferenceMetadata),
          new TypeReference<Map<String, String>>() {});
    } catch (Exception e) {
      throw new ParquetCryptoRuntimeException("Failed to parse key metadata " + keyReferenceMetadata, e);
    }

    return keyMetadataJson.get(EnvelopeKeyManager.KEY_REFERENCE_FIELD);
  }

  /**
   * Flush any caches that are tied to the specified accessToken
   * @param accessToken
   */
  public static void invalidateCachesForToken(String accessToken) {
    synchronized (readKEKCacheLock) {
      readKEKMapPerToken.remove(accessToken);
    }
  }

  private void invalidateCachesForExpiredTokens() {
    long now = System.currentTimeMillis();
    if (now > lastCacheCleanupTimestamp + this.cacheEntryLifetime) {
      synchronized (readKEKCacheLock) {
        if (now > lastCacheCleanupTimestamp + this.cacheEntryLifetime) {
          EnvelopeKeyManager.removeExpiredEntriesFromCache(readKEKMapPerToken);
          lastCacheCleanupTimestamp = now;
        }
      }
    }
  }

  static void invalidateCachesForAllTokens() {
    synchronized (readKEKCacheLock) {
      readKEKMapPerToken.clear();
    }
  }
}