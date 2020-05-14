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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.keytools.KeyToolUtilities.KeyWithMasterID;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


public class FileKeyUnwrapper implements DecryptionKeyRetriever {
  // For every token: a map of KEK_ID to KEK bytes
  private static final Map<String, ExpiringCacheEntry<Map<String,byte[]>>> KEKMapPerToken = new HashMap<>();

  private volatile long lastCacheCleanupTimestamp = System.currentTimeMillis() + 60l * 1000; // grace period of 1 minute

  private final long cacheEntryLifetime;
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private KmsClient kmsClient = null;
  private final FileKeyMaterialStore keyMaterialStore;
  private final Configuration hadoopConfiguration;

  private ExpiringCacheEntry<Map<String, byte[]>> KEKCacheEntry;

  FileKeyUnwrapper(Configuration hadoopConfiguration, FileKeyMaterialStore keyStore) {
    this.hadoopConfiguration = hadoopConfiguration;
    this.keyMaterialStore = keyStore;

    cacheEntryLifetime = 1000l * hadoopConfiguration.getLong(FileKeyWrapper.TOKEN_LIFETIME_PROPERTY_NAME,
        FileKeyWrapper.DEFAULT_CACHE_ENTRY_LIFETIME);

    // Check cache upon each file reading (clean once in cacheEntryLifetime)
    checkKekCacheEntriesForExpiredTokens();
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
    }  catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Failed to parse key material " + keyMaterial, e);
    }

    String keyMaterialType = keyMaterialJson.get(FileKeyWrapper.KEY_MATERIAL_TYPE_FIELD);
    if (null != keyMaterialType && !FileKeyWrapper.KEY_MATERIAL_TYPE.equals(keyMaterialType)) {
      throw new ParquetCryptoRuntimeException("Wrong key materila type: " + keyMaterialType + 
          " vs " + FileKeyWrapper.KEY_MATERIAL_TYPE_FIELD);
    }

    if (null == kmsClient) {
      kmsClient = getKmsClient(keyMaterialJson);
    }

    boolean doubleWrapping = Boolean.valueOf(keyMaterialJson.get(FileKeyWrapper.DOUBLE_WRAPPING_FIELD));

    String masterKeyID = keyMaterialJson.get(FileKeyWrapper.MASTER_KEY_ID_FIELD);
    String encodedWrappedDatakey = keyMaterialJson.get(FileKeyWrapper.WRAPPED_DEK_FIELD);

    byte[] dataKey;
    if (!doubleWrapping) {
      dataKey = kmsClient.unwrapKey(encodedWrappedDatakey, masterKeyID);
    } else {
      // Get KEK
      String encodedKEK_ID = keyMaterialJson.get(FileKeyWrapper.KEK_ID_FIELD);
      final Map<String, String> keyMaterialJsonFinal = keyMaterialJson;

      Map<String,byte[]> KEKPerKekID = getKekPerIdMap();

      byte[] kekBytes = KEKPerKekID.get(encodedKEK_ID);
      if (null == kekBytes) {
        synchronized (KEKPerKekID) {
          kekBytes = KEKPerKekID.get(encodedKEK_ID);
          if (null == kekBytes) {
            kekBytes = unwrapKek(keyMaterialJsonFinal, masterKeyID);
            KEKPerKekID.put(encodedKEK_ID, kekBytes);
          }
        }
      }

      // Decrypt the data key
      byte[]  AAD = Base64.getDecoder().decode(encodedKEK_ID);
      dataKey =  KeyToolUtilities.unwrapKeyLocally(encodedWrappedDatakey, kekBytes, AAD);
    }

    return new KeyWithMasterID(dataKey, masterKeyID);
  }

  private Map<String,byte[]> getKekPerIdMap() {
    if (null != KEKCacheEntry && !KEKCacheEntry.isExpired()) {
      return KEKCacheEntry.getCachedItem();
    }

    String accessToken = hadoopConfiguration.getTrimmed(FileKeyWrapper.KEY_ACCESS_TOKEN_PROPERTY_NAME, 
        FileKeyWrapper.DEFAULT_ACCESS_TOKEN);

    KEKCacheEntry = KEKMapPerToken.get(accessToken);
    if ((null == KEKCacheEntry) || (KEKCacheEntry.isExpired())) {
      synchronized (KEKMapPerToken) {
        KEKCacheEntry = KEKMapPerToken.get(accessToken);
        if ((null == KEKCacheEntry) || (KEKCacheEntry.isExpired())) {
          KEKCacheEntry = new ExpiringCacheEntry<>(new HashMap<String,byte[]>(), cacheEntryLifetime);
          KEKMapPerToken.put(accessToken, KEKCacheEntry);
        }
      }
    }
    return KEKCacheEntry.getCachedItem();
  }

  private byte[] unwrapKek(Map<String, String> keyMaterialJson, String masterKeyID) {
    byte[] kekBytes;
    String encodedWrappedKEK = keyMaterialJson.get(FileKeyWrapper.WRAPPED_KEK_FIELD);
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
  private KmsClient getKmsClient(Map<String, String> keyMaterialJson) {
    String kmsInstanceID = hadoopConfiguration.getTrimmed(FileKeyWrapper.KMS_INSTANCE_ID_PROPERTY_NAME);
    if (StringUtils.isEmpty(kmsInstanceID)) {
      kmsInstanceID = keyMaterialJson.get(FileKeyWrapper.KMS_INSTANCE_ID_FIELD);
      if (null == kmsInstanceID) {
        throw new ParquetCryptoRuntimeException("KMS instance ID is missing both in properties and file key material");
      }
      hadoopConfiguration.set(FileKeyWrapper.KMS_INSTANCE_ID_PROPERTY_NAME, kmsInstanceID);
    }

    String kmsInstanceURL = hadoopConfiguration.getTrimmed(RemoteKmsClient.KMS_INSTANCE_URL_PROPERTY_NAME);
    if (StringUtils.isEmpty(kmsInstanceURL)) {
      kmsInstanceURL = keyMaterialJson.get(FileKeyWrapper.KMS_INSTANCE_URL_FIELD);
      if (null == kmsInstanceURL) {
        throw new ParquetCryptoRuntimeException("KMS instance URL is missing both in properties and file key material");
      }
      hadoopConfiguration.set(RemoteKmsClient.KMS_INSTANCE_URL_PROPERTY_NAME, kmsInstanceURL);
    }

    String accessToken = hadoopConfiguration.getTrimmed(FileKeyWrapper.KEY_ACCESS_TOKEN_PROPERTY_NAME, 
        FileKeyWrapper.DEFAULT_ACCESS_TOKEN);

    KmsClient kmsClient = FileKeyWrapper.getKmsClient(kmsInstanceID, hadoopConfiguration, accessToken, cacheEntryLifetime);
    if (null == kmsClient) {
      throw new ParquetCryptoRuntimeException("KMSClient was not successfully created for reading encrypted data.");
    }
    return kmsClient;
  }

  private static String getKeyReference(String keyReferenceMetadata) {
    Map<String, String> keyMetadataJson = null;
    try {
      keyMetadataJson = objectMapper.readValue(new StringReader(keyReferenceMetadata),
          new TypeReference<Map<String, String>>() {});
    } catch (Exception e) {
      throw new ParquetCryptoRuntimeException("Failed to parse key metadata " + keyReferenceMetadata, e);
    }

    return keyMetadataJson.get(FileKeyWrapper.KEY_REFERENCE_FIELD);
  }

  /**
   * Flush any caches that are tied to the specified accessToken
   * @param accessToken
   */
  static void removeCacheEntriesForToken(String accessToken) {
    synchronized (KEKMapPerToken) { 
      KEKMapPerToken.remove(accessToken);
    }
  }

  private void checkKekCacheEntriesForExpiredTokens() {
    long now = System.currentTimeMillis();
    if (now < (lastCacheCleanupTimestamp + this.cacheEntryLifetime)) {
      return;
    }
    synchronized (KEKMapPerToken) {
      if (now < (lastCacheCleanupTimestamp + this.cacheEntryLifetime)) {
        return;
      }
      KeyToolUtilities.removeExpiredEntriesFromCache(KEKMapPerToken);
      lastCacheCleanupTimestamp = now;
    }
  }

  static void removeCacheEntriesForAllTokens() {
    synchronized (KEKMapPerToken) {
      KEKMapPerToken.clear();
    }
  }
}