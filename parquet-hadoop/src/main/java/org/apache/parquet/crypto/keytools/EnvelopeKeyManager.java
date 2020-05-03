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
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.keytools.KeyToolUtilities.KeyEncryptionKey;
import org.apache.parquet.crypto.keytools.KeyToolUtilities.KeyWithMasterID;
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
  
  public static final String KMS_INSTANCE_ID_FIELD = "kmsInstanceID";
  public static final String KMS_INSTANCE_URL_FIELD = "kmsInstanceURL";

  public static final String WRAPPING_METHOD_PROPERTY_NAME = "encryption.key.wrapping.method";
  public static final String CACHE_ENTRY_LIFETIME_PROPERTY_NAME = "encryption.cache.entry.lifetime";
  public static final String KMS_CLIENT_CLASS_PROPERTY_NAME = "encryption.kms.client.class";

  public static final String WRAPPING_METHOD_FIELD = "method";
  public static final String single_wrapping_method = "single";
  public static final String double_wrapping_method = "double";
  public static final String WRAPPING_METHOD_VERSION_FIELD = "version";
  public static final String wrapping_method_version = "0.1";
  public static final String MASTER_KEY_ID_FIELD = "masterKeyID";
  public static final String WRAPPED_DEK_FIELD = "wrappedDEK";

  public static final String KEK_ID_FIELD = "keyEncryptionKeyID";
  public static final String WRAPPED_KEK_FIELD = "wrappedKEK";
  
  private static final int INITIAL_KMS_CLIENT_CACHE_SIZE = 5;
  private static final Map<String, KmsClientCacheEntry> kmsClientPerKmsInstanceCache =
      new HashMap<String, KmsClientCacheEntry>(INITIAL_KMS_CLIENT_CACHE_SIZE);
  private static final long KMS_CLIENT_CACHE_ENTRY_DEFAULT_LIFETIME = 10 * 60 * 1000; // 10 minutes


  private static final Map<String,KeyEncryptionKey> writeSessionKEKMap = new HashMap<String, KeyEncryptionKey>();

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final KmsClient kmsClient;
  private final String kmsInstanceID;
  private final String kmsInstanceURLForParquetWrite;
  private final FileKeyMaterialStore keyMaterialStore;
  private final Configuration hadoopConfiguration;
  private final SecureRandom random;
  private final String wrappingMethod;
  private final boolean doubleWrapping;

  private short keyCounter;

  EnvelopeKeyManager(Configuration configuration, FileKeyMaterialStore keyMaterialStore) {
    this.hadoopConfiguration = configuration;

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
  }

  /**
   * Immutable class for KmsClient cache entries
   */
  private static class KmsClientCacheEntry {
    private final KmsClient kmsClient;
    private final String accessToken;
    private final long expirationTimestamp;

    private KmsClientCacheEntry(KmsClient kmsClient, String accessToken, long expirationTimestamp) {
      this.kmsClient = kmsClient;
      this.accessToken = accessToken;
      this.expirationTimestamp = expirationTimestamp;
    }
    public KmsClient getKmsClient() {
      return kmsClient;
    }

    /**
     * Cache entry is valid - it is not expired and matches the current access token
     * @param currentAccessToken
     * @return
     */
    public boolean isValid(String currentAccessToken) {
      final long now = System.currentTimeMillis();
      return (now < expirationTimestamp) && Objects.equals(this.accessToken, currentAccessToken);
    }
  }



  private byte[] getEncryptionKeyMetadata(byte[] dataKey, String masterKeyID, boolean isFooterKey, 
      FileKeyMaterialStore targetKeyMaterialStore, String keyIdInFile) {
    if (null == kmsClient) {
      throw new ParquetCryptoRuntimeException("No KMS client available. See previous errors.");
    }

    KeyEncryptionKey keyEncryptionKey = null;
    String encodedWrappedDEK = null;
    if (!doubleWrapping) {
      encodedWrappedDEK = kmsClient.wrapDataKey(dataKey, masterKeyID);
    } else {
     // Find or generate KEK for Master Key ID
      keyEncryptionKey = writeSessionKEKMap.get(masterKeyID);
      if (null == keyEncryptionKey) {
        synchronized(writeSessionKEKMap) {
          keyEncryptionKey = writeSessionKEKMap.get(masterKeyID);
          if (null == keyEncryptionKey) {
            byte[] kekBytes = new byte[16]; //TODO length. configure via properties
            random.nextBytes(kekBytes);

            byte[] kekID = new byte[16];  //TODO length. configure via properties
            random.nextBytes(kekID);
            String encodedKEK_ID = Base64.getEncoder().encodeToString(kekID);

            // Encrypt KEK with Master key
            String encodedWrappedKEK = kmsClient.wrapDataKey(kekBytes, masterKeyID); // TODO nested sync. Break

            keyEncryptionKey = new KeyEncryptionKey(kekBytes, encodedKEK_ID, kekID, encodedWrappedKEK);
            writeSessionKEKMap.put(masterKeyID, keyEncryptionKey);
          }
        } // sync writeSessionKEKMap
      }
      
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

  /**
   * Create and initialize a KMSClient. Called when KMS instance ID should be known.
   * @param configuration
   * @param kmsInstanceID
   * @return
   * @throws IOException
   */
  static KmsClient getKmsClient(Configuration configuration, String kmsInstanceID) 
      throws ParquetCryptoRuntimeException {
    String currentAccessToken = configuration.getTrimmed(RemoteKmsClient.KEY_ACCESS_TOKEN_PROPERTY_NAME, ""); // default for KMS without token
    KmsClient kmsClient = null;
    synchronized (kmsClientPerKmsInstanceCache) {
      // Get from KmsClients cache if entry not expired
      KmsClientCacheEntry kmsClientCacheEntry = kmsClientPerKmsInstanceCache.get(kmsInstanceID);
      if (null != kmsClientCacheEntry) {
        if (kmsClientCacheEntry.isValid(currentAccessToken)) {
          kmsClient = kmsClientCacheEntry.getKmsClient();
          return kmsClient;
        }
      }

      final Class<?> kmsClientClass = ConfigurationUtil.getClassFromConfig(configuration,
          KMS_CLIENT_CLASS_PROPERTY_NAME, KmsClient.class);

      if (null == kmsClientClass) {
        throw new ParquetCryptoRuntimeException("Unspecified encryption.kms.client.class"); //TODO
      }

      try {
        kmsClient = (KmsClient)kmsClientClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ParquetCryptoRuntimeException("could not instantiate KmsClient class: "
            + kmsClientClass, e);
      }

      try {
        kmsClient.initialize(configuration, kmsInstanceID);
      } catch (ParquetCryptoRuntimeException e) {
        LOG.info("Cannot create KMS client. If encryption.kms.instance.id not defined, will be expecting to use " +
            "default KMS instance ID, if relevant, or key metadata from parquet file.");
        LOG.debug("Cannot create KMS client.", e);
        return null;
      }
      long kmsClientCacheEntryLifetime = configuration.getLong(CACHE_ENTRY_LIFETIME_PROPERTY_NAME, KMS_CLIENT_CACHE_ENTRY_DEFAULT_LIFETIME);
      final long expirationTimestamp = System.currentTimeMillis() + kmsClientCacheEntryLifetime;
      final KmsClientCacheEntry newClientCacheEntry = new KmsClientCacheEntry(kmsClient, currentAccessToken, expirationTimestamp);
      kmsClientPerKmsInstanceCache.put(kmsInstanceID, newClientCacheEntry);
    } // sync on KMS cache

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
}
