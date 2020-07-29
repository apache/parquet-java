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

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.keytools.KeyToolkit.KeyEncryptionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.parquet.crypto.keytools.KeyToolkit.KMS_CLIENT_CACHE_PER_TOKEN;
import static org.apache.parquet.crypto.keytools.KeyToolkit.KEK_WRITE_CACHE_PER_TOKEN;

public class FileKeyWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(FileKeyWrapper.class);

  public static final int KEK_LENGTH = 16;
  public static final int KEK_ID_LENGTH = 16;

  // A map of MEK_ID -> KeyEncryptionKey, for the current token
  private final ConcurrentMap<String, KeyEncryptionKey> KEKPerMasterKeyID;

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

  FileKeyWrapper(Configuration configuration, FileKeyMaterialStore keyMaterialStore) {
    this.hadoopConfiguration = configuration;
    this.keyMaterialStore = keyMaterialStore;
    
    random = new SecureRandom();
    keyCounter = 0;

    cacheEntryLifetime = 1000L * hadoopConfiguration.getLong(KeyToolkit.CACHE_LIFETIME_PROPERTY_NAME, 
        KeyToolkit.CACHE_LIFETIME_DEFAULT_SECONDS); 

    kmsInstanceID = hadoopConfiguration.getTrimmed(KeyToolkit.KMS_INSTANCE_ID_PROPERTY_NAME, 
        KmsClient.KMS_INSTANCE_ID_DEFAULT);

    doubleWrapping =  hadoopConfiguration.getBoolean(KeyToolkit.DOUBLE_WRAPPING_PROPERTY_NAME, KeyToolkit.DOUBLE_WRAPPING_DEFAULT);
    accessToken = hadoopConfiguration.getTrimmed(KeyToolkit.KEY_ACCESS_TOKEN_PROPERTY_NAME, KmsClient.KEY_ACCESS_TOKEN_DEFAULT);

    kmsInstanceURL = hadoopConfiguration.getTrimmed(KeyToolkit.KMS_INSTANCE_URL_PROPERTY_NAME, 
        KmsClient.KMS_INSTANCE_URL_DEFAULT);

    // Check caches upon each file writing (clean once in cacheEntryLifetime)
    KMS_CLIENT_CACHE_PER_TOKEN.checkCacheForExpiredTokens(cacheEntryLifetime);
    kmsClient = KeyToolkit.getKmsClient(kmsInstanceID, kmsInstanceURL, configuration, accessToken, cacheEntryLifetime);

    if (doubleWrapping) {
      KEK_WRITE_CACHE_PER_TOKEN.checkCacheForExpiredTokens(cacheEntryLifetime);
      KEKPerMasterKeyID = KEK_WRITE_CACHE_PER_TOKEN.getOrCreateInternalCache(accessToken, cacheEntryLifetime);
    } else {
      KEKPerMasterKeyID = null;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating file key wrapper. KmsClient: {}; KmsInstanceId: {}; KmsInstanceURL: {}; doubleWrapping: {}; "
          + "keyMaterialStore: {}; token snippet: {}", kmsClient, kmsInstanceID, kmsInstanceURL, doubleWrapping, 
          keyMaterialStore, KeyToolkit.formatTokenForLog(accessToken));
    }
  }

  byte[] getEncryptionKeyMetadata(byte[] dataKey, String masterKeyID, boolean isFooterKey) {
    return getEncryptionKeyMetadata(dataKey, masterKeyID, isFooterKey, null);
  }

  byte[] getEncryptionKeyMetadata(byte[] dataKey, String masterKeyID, boolean isFooterKey, String keyIdInFile) {
    if (null == kmsClient) {
      throw new ParquetCryptoRuntimeException("No KMS client available. See previous errors.");
    }

    String encodedKekID = null;
    String encodedWrappedKEK = null;
    String encodedWrappedDEK = null;
    if (!doubleWrapping) {
      encodedWrappedDEK = kmsClient.wrapKey(dataKey, masterKeyID);
    } else {
      // Find in cache, or generate KEK for Master Key ID
      KeyEncryptionKey keyEncryptionKey = KEKPerMasterKeyID.computeIfAbsent(masterKeyID,
          (k) -> createKeyEncryptionKey(masterKeyID));

      // Encrypt DEK with KEK
      byte[] AAD = keyEncryptionKey.getID();
      encodedWrappedDEK = KeyToolkit.encryptKeyLocally(dataKey, keyEncryptionKey.getBytes(), AAD);
      encodedKekID = keyEncryptionKey.getEncodedID();
      encodedWrappedKEK = keyEncryptionKey.getEncodedWrappedKEK();
    }
    
    boolean storeKeyMaterialInternally = (null == keyMaterialStore);

    String serializedKeyMaterial = KeyMaterial.createSerialized(isFooterKey, kmsInstanceID, kmsInstanceURL, masterKeyID, 
        doubleWrapping, encodedKekID, encodedWrappedKEK, encodedWrappedDEK, storeKeyMaterialInternally);

    // Internal key material storage: key metadata and key material are the same
    if (storeKeyMaterialInternally) {
      return serializedKeyMaterial.getBytes(StandardCharsets.UTF_8);
    } 

    // External key material storage: key metadata is a reference to a key in the material store
    if (null == keyIdInFile) {
      if (isFooterKey) {
        keyIdInFile = KeyMaterial.FOOTER_KEY_ID_IN_FILE;
      } else {
        keyIdInFile = KeyMaterial.COLUMN_KEY_ID_IN_FILE_PREFIX + keyCounter;
        keyCounter++;
      }
    }
    keyMaterialStore.addKeyMaterial(keyIdInFile, serializedKeyMaterial);
    
    String serializedKeyMetadata = KeyMetadata.createSerializedForExternalMaterial(keyIdInFile);

    return serializedKeyMetadata.getBytes(StandardCharsets.UTF_8);
  }

  private KeyEncryptionKey createKeyEncryptionKey(String masterKeyID) {
    byte[] kekBytes = new byte[KEK_LENGTH]; 
    random.nextBytes(kekBytes);

    byte[] kekID = new byte[KEK_ID_LENGTH];
    random.nextBytes(kekID);

    // Encrypt KEK with Master key
    String encodedWrappedKEK = null;
    encodedWrappedKEK = kmsClient.wrapKey(kekBytes, masterKeyID);

    return new KeyEncryptionKey(kekBytes, kekID, encodedWrappedKEK);
  }
}