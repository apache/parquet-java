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
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.AesDecryptor;
import org.apache.parquet.crypto.AesEncryptor;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.apache.parquet.crypto.keytools.FileKeyManager;
import org.apache.parquet.crypto.keytools.KeyMaterialStore;
import org.apache.parquet.crypto.keytools.KeyWithMetadata;
import org.apache.parquet.crypto.keytools.KmsClient;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


public class DoubleWrappedKeyManager extends FileKeyManager {

  private static class KeyEncryptionKey {
    private final byte[] kekBytes;
    private final byte[] kekID;
    private final String encodedKEK_ID;
    private final String encodedWrappedKEK;
    
    private KeyEncryptionKey(byte[] kekBytes, String encodedKEK_ID, byte[] kekID, String encodedWrappedKEK) {
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

  private static final String wrappingMethod = "org.apache.parquet.crypto.keytools.DoubleWrappedKeyManager";
  private static final String wrappingMethodVersion = "0.1";

  private static final String WRAPPING_METHOD_FIELD = "method";
  private static final String WRAPPING_METHOD_VERSION_FIELD = "version";
  private static final String CRK_ID_FIELD = "customerRootKeyID";
  private static final String KMS_INSTANCE_ID_FIELD = "kmsInstanceID";
  private static final String KEK_ID_FIELD = "keyEncryptionKeyID";
  private static final String WRAPPED_KEK_FIELD = "wrappedKEK";
  private static final String WRAPPED_DEK_FIELD = "wrappedDEK";

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final Map<String,KeyEncryptionKey> writeSessionKEKMap = new HashMap<String, KeyEncryptionKey>();
  private static final Map<String,byte[]> readSessionKEKMap = new HashMap<String, byte[]>();


  private Configuration hadoopConfiguration;
  private KmsClient kmsClient;
  private boolean wrapLocally;
  private KeyMaterialStore keyMaterialStore;
  private String fileID;

  private  SecureRandom random;
  private short keyCounter;

  public static class DoubleWrappedKeyRetriever implements DecryptionKeyRetriever {
    private KmsClient kmsClient;
    private String kmsInstanceID;
    private final boolean unwrapLocally;
    private final KeyMaterialStore keyMaterialStore;
    private final String fileID;
    private final Configuration hadoopConfiguration;

    private DoubleWrappedKeyRetriever(KmsClient kmsClient, String kmsInstanceID, Configuration hadoopConfiguration, boolean unwrapLocally,
                                      KeyMaterialStore keyStore, String fileID) {
      this.kmsClient = kmsClient;
      this.kmsInstanceID = kmsInstanceID;
      this.hadoopConfiguration = hadoopConfiguration;
      this.keyMaterialStore = keyStore;
      this.fileID = fileID;
      this.unwrapLocally = unwrapLocally;
    }

    @Override
    public byte[] getKey(byte[] keyMetaData) throws IOException, KeyAccessDeniedException {
      String keyMaterial;
      if (null != keyMaterialStore) {
        String keyIDinFile = new String(keyMetaData, StandardCharsets.UTF_8);
        keyMaterial = keyMaterialStore.getKeyMaterial(fileID, keyIDinFile);
      }
      else {
        keyMaterial = new String(keyMetaData, StandardCharsets.UTF_8);
      }

      Map<String, String> keyMaterialJson = null;
      try {
        keyMaterialJson = objectMapper.readValue(new StringReader(keyMaterial),
                new TypeReference<Map<String, String>>() {});
      } catch (JsonProcessingException e) {
        throw new IOException("Failed to parse key material " + keyMaterial, e);
      }

      String wrapMethod = keyMaterialJson.get(WRAPPING_METHOD_FIELD);
      if (!wrappingMethod.equals(wrapMethod)) {
        throw new IOException("Wrong wrapping method " + wrapMethod);
      }

      //String wrapMethodVersion = (String) jsonObject.get(WRAPPING_METHOD_VERSION_FIELD);
      //TODO compare to wrappingMethodVersion

      // Get KEK
      String encodedKEK_ID = keyMaterialJson.get(KEK_ID_FIELD);
      byte[] kekBytes = readSessionKEKMap.get(encodedKEK_ID);
      byte[] AAD = null;
      if (null == kekBytes) {
        synchronized (readSessionKEKMap) {
          kekBytes = readSessionKEKMap.get(encodedKEK_ID);
          if (null == kekBytes) {
            String masterKeyID = keyMaterialJson.get(CRK_ID_FIELD);
            if (null == kmsClient) {
              kmsInstanceID = keyMaterialJson.get(KMS_INSTANCE_ID_FIELD);
              kmsClient = getKmsClient(hadoopConfiguration, kmsInstanceID);
            }
            String encodedWrappedKEK = keyMaterialJson.get(WRAPPED_KEK_FIELD);
            
            if (unwrapLocally) {
              byte[] wrappedKEK = Base64.getDecoder().decode(encodedWrappedKEK);
              String encodedMasterKey = null;
              try {
                encodedMasterKey = kmsClient.getKeyFromServer(masterKeyID); // TODO nested sync. Break.
              }
              catch (UnsupportedOperationException e) {
                throw new IOException("KMS client doesnt support key fetching", e);
              }
              if (null == encodedMasterKey) {
                throw new IOException("Failed to get from KMS the master key " + masterKeyID);
              }
              byte[] masterKey = Base64.getDecoder().decode(encodedMasterKey);
              // TODO key wiping
              AesDecryptor kekDecryptor = new AesDecryptor(AesEncryptor.Mode.GCM, masterKey, null);
              AAD = Base64.getDecoder().decode(encodedKEK_ID);
              kekBytes = kekDecryptor.decrypt(wrappedKEK, 0, wrappedKEK.length, AAD);
            }
            else {
              String encodedKEK = null;
              try {
                encodedKEK = kmsClient.unwrapDataKeyInServer(encodedWrappedKEK, masterKeyID); // TODO nested sync. Break.
              }
              catch (UnsupportedOperationException e) {
                throw new IOException("KMS client doesnt support key wrapping", e);
              }
              if (null == encodedKEK) {
                throw new IOException("Failed to unwrap in KMS with master key " + masterKeyID);
              }
              kekBytes = Base64.getDecoder().decode(encodedKEK);
            }
            
            readSessionKEKMap.put(encodedKEK_ID, kekBytes);
          }
        } // sync readSessionKEKMap
      }

      // Decrypt the data key
      String encodedEncryptedDEK = keyMaterialJson.get(WRAPPED_DEK_FIELD);
      byte[] encryptedDEK = Base64.getDecoder().decode(encodedEncryptedDEK);
      AesDecryptor dekDecryptor = new AesDecryptor(AesEncryptor.Mode.GCM, kekBytes, null);
      if (null == AAD) AAD = Base64.getDecoder().decode(encodedKEK_ID);
      return  dekDecryptor.decrypt(encryptedDEK, 0, encryptedDEK.length, AAD);
    }
  }

  @Override
  public void initialize(Configuration configuration, KmsClient kmsClient, KeyMaterialStore keyMaterialStore,
                         String fileID) throws IOException {
    String localWrap = configuration.getTrimmed("encryption.wrap.locally");
    if (null == localWrap || localWrap.equalsIgnoreCase("true")) {
      wrapLocally = true; // true by default
    }
    else if (localWrap.equalsIgnoreCase("false")) {
      wrapLocally = false;
    }
    else {
      throw new IOException("Bad encryption.wrap.locally value: " + localWrap);
    }
    this.kmsClient = kmsClient;
    this.hadoopConfiguration = configuration;
    this.keyMaterialStore = keyMaterialStore;
    this.fileID = fileID;
    random = new SecureRandom();
    keyCounter = 0;
  }

  @Override
  public KeyWithMetadata getFooterEncryptionKey(String footerMasterKeyID) throws IOException {
    return generateDataKey(footerMasterKeyID, true);
  }

  @Override
  public KeyWithMetadata getColumnEncryptionKey(ColumnPath column, String columnMasterKeyID) throws IOException {
    return generateDataKey(columnMasterKeyID, false);
  }

  @Override
  public DecryptionKeyRetriever getDecryptionKeyRetriever() {
    return new DoubleWrappedKeyRetriever(kmsClient, kmsInstanceID, hadoopConfiguration, wrapLocally, keyMaterialStore, fileID);
  }

  @Override
  public void close() {
    // TODO Wipe keys
  }

  private KeyWithMetadata generateDataKey(String masterKeyID, boolean addKMSMetadata) throws IOException {
    if (null == kmsClient) {
      throw new IOException("No KMS client available.");
    }

    // Generate DEK
    byte[] dataEncryptionKey = new byte[16]; //TODO length. configure via properties
    random.nextBytes(dataEncryptionKey);

    // Find or generate KEK for CRK (master Key)
    KeyEncryptionKey keyEncryptionKey = writeSessionKEKMap.get(masterKeyID);
    if (null == keyEncryptionKey) {
      synchronized(writeSessionKEKMap) {
        keyEncryptionKey = writeSessionKEKMap.get(masterKeyID);
        if (null == keyEncryptionKey) {
          byte[] kekBytes = new byte[16]; //TODO length. configure via properties
          random.nextBytes(kekBytes);
          
          byte[] kekID = new byte[16];  //TODO length. configure via properties
          random.nextBytes(kekID);
          String encodedKEK_ID = Base64.getEncoder().encodeToString(kekID);
          
          // Encrypt KEK with CRK (master key)
          String encodedWrappedKEK = null;
          if (wrapLocally) {
            String encodedMasterKey;
            try {
              // TODO cache here or in kmsClient?
              encodedMasterKey = kmsClient.getKeyFromServer(masterKeyID); // TODO nested sync. Break
            } 
            catch (KeyAccessDeniedException e) {
              throw new IOException("Unauthorized to fetch key: " + masterKeyID, e);
            } 
            catch (UnsupportedOperationException e) {
              throw new IOException("KMS client doesnt support key fetching", e);
            }
            byte[] masterKey = Base64.getDecoder().decode(encodedMasterKey);
            // TODO key wiping
            AesEncryptor keyEncryptor = new AesEncryptor(AesEncryptor.Mode.GCM, masterKey, null);
            byte[] AAD = kekID;
            byte[] wrappedKEK = keyEncryptor.encrypt(false, kekBytes, AAD);
            encodedWrappedKEK = Base64.getEncoder().encodeToString(wrappedKEK);
          }
          else {
            String encodedKEK = Base64.getEncoder().encodeToString(kekBytes);
            try {
              encodedWrappedKEK = kmsClient.wrapDataKeyInServer(encodedKEK, masterKeyID); // TODO nested sync. Break
            } 
            catch (KeyAccessDeniedException e) {
              throw new IOException("Unauthorized to wrap with master key: " + masterKeyID, e);
            } 
            catch (UnsupportedOperationException e) {
              throw new IOException("KMS client doesnt support key wrapping", e);
            }
          }
          
          keyEncryptionKey = new KeyEncryptionKey(kekBytes, encodedKEK_ID, kekID, encodedWrappedKEK);
          writeSessionKEKMap.put(masterKeyID, keyEncryptionKey);
        }
      } // sync writeSessionKEKMap
    }

    // Encrypt DEK with KEK
    // TODO key wiping
    AesEncryptor dekEncryptor = new AesEncryptor(AesEncryptor.Mode.GCM, keyEncryptionKey.getBytes(), null);
    byte[] AAD = keyEncryptionKey.getID();
    byte[] encryptedDEK = dekEncryptor.encrypt(false, dataEncryptionKey, AAD);
    String encodedEncryptedDEK = Base64.getEncoder().encodeToString(encryptedDEK);

    // Pack all into key material JSON
    Map<String, String> keyMaterialMap = new HashMap<String, String>(6);
    keyMaterialMap.put(WRAPPING_METHOD_FIELD, wrappingMethod);
    keyMaterialMap.put(WRAPPING_METHOD_VERSION_FIELD, wrappingMethodVersion);
    keyMaterialMap.put(CRK_ID_FIELD, masterKeyID);
    if (addKMSMetadata) {
      keyMaterialMap.put(KMS_INSTANCE_ID_FIELD, kmsInstanceID);
    }
    keyMaterialMap.put(KEK_ID_FIELD, keyEncryptionKey.getEncodedID());
    keyMaterialMap.put(WRAPPED_KEK_FIELD, keyEncryptionKey.getWrappedWithCRK());
    keyMaterialMap.put(WRAPPED_DEK_FIELD, encodedEncryptedDEK);
    String keyMaterial = objectMapper.writeValueAsString(keyMaterialMap);

    // Create key metadata
    byte[] keyMetadata = null;
    if (null != keyMaterialStore) {
      String keyName = "k" + keyCounter;
      keyMaterialStore.storeKeyMaterial(keyMaterial, fileID, keyName);
      keyMetadata = keyName.getBytes(StandardCharsets.UTF_8);
      keyCounter++;
    }
    else {
      keyMetadata  = keyMaterial.getBytes(StandardCharsets.UTF_8);
    }

    return new KeyWithMetadata(dataEncryptionKey, keyMetadata);
  }

}
