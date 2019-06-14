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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.AesDecryptor;
import org.apache.parquet.crypto.AesEncryptor;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class WrappedKeyManager implements FileKeyManager {
  
  private static final String wrappingMethod = "org.apache.parquet.crypto.keytools.WrappedKeyManager";
  private static final String wrappingMethodVersion = "0.1";
  
  private static final String WRAPPING_METHOD_FIELD = "method";
  private static final String WRAPPING_METHOD_VERSION_FIELD = "version";
  private static final String MASTER_KEY_ID_FIELD = "masterKeyID";
  private static final String WRAPPED_KEY_FIELD = "wrappedKey";

  private KmsClient kmsClient;
  private boolean wrapLocally;
  private  KeyMaterialStore keyMaterialStore;
  private  String fileID;

  private  SecureRandom random;
  private short keyCounter;

  public static class WrappedKeyRetriever implements DecryptionKeyRetriever {
    private final KmsClient kmsClient;
    private final boolean unwrapLocally;
    private final KeyMaterialStore keyStore;
    private final String fileID;

    private WrappedKeyRetriever(KmsClient kmsClient, boolean unwrapLocally, KeyMaterialStore keyStore, String fileID) {
      this.kmsClient = kmsClient;
      this.keyStore = keyStore;
      this.fileID = fileID;
      this.unwrapLocally = unwrapLocally;
    }

    public byte[] getKey(byte[] keyMetaData) throws IOException, KeyAccessDeniedException {
      String keyMaterial;
      if (null != keyStore) {
        String keyIDinFile = new String(keyMetaData, StandardCharsets.UTF_8);
        keyMaterial = keyStore.getKeyMaterial(fileID, keyIDinFile);
      }
      else {
        keyMaterial = new String(keyMetaData, StandardCharsets.UTF_8);
      }
      
      JSONParser parser = new JSONParser();
      JSONObject keyMaterialJson = null;
      try {
        keyMaterialJson = (JSONObject) parser.parse(keyMaterial);
      } catch (ParseException e) {
        throw new IOException("Failed to parse key material " + keyMaterial, e);
      }
      
      String wrapMethod = (String) keyMaterialJson.get(WRAPPING_METHOD_FIELD);
      if (!wrapMethod.equals(wrappingMethod)) {
        throw new IOException("Wrong wrapping method " + wrapMethod);
      }
      
      //String wrapMethodVersion = (String) jsonObject.get(WRAPPING_METHOD_VERSION_FIELD);
      //TODO compare to wrappingMethodVersion
          
      String encodedWrappedDatakey = (String) keyMaterialJson.get(WRAPPED_KEY_FIELD);
      String masterKeyID = (String) keyMaterialJson.get(MASTER_KEY_ID_FIELD);
      
      byte[] dataKey = null;
      if (unwrapLocally) {
        byte[] wrappedDataKey = Base64.getDecoder().decode(encodedWrappedDatakey);
        String encodedMasterKey = null;
        try {
          encodedMasterKey = kmsClient.getKeyFromServer(masterKeyID);
        }
        catch (UnsupportedOperationException e) {
          throw new IOException("KMS client doesnt support key fetching", e);
        }
        if (null == encodedMasterKey) {
          throw new IOException("Failed to get from KMS the master key " + masterKeyID);
        }
        byte[] masterKey = Base64.getDecoder().decode(encodedMasterKey);
        // TODO key wiping
        AesDecryptor keyDecryptor = new AesDecryptor(AesEncryptor.Mode.GCM, masterKey, null);
        dataKey = keyDecryptor.decrypt(wrappedDataKey, 0, wrappedDataKey.length, null);
      }
      else {
        String encodedDataKey = null;
        try {
          encodedDataKey = kmsClient.unwrapDataKeyInServer(encodedWrappedDatakey, masterKeyID);
        }
        catch (UnsupportedOperationException e) {
          throw new IOException("KMS client doesnt support key wrapping", e);
        }
        if (null == encodedDataKey) {
          throw new IOException("Failed to unwrap in KMS with master key " + masterKeyID);
        }
        dataKey = Base64.getDecoder().decode(encodedDataKey);
      }
      return dataKey;
    }
  }
  
  @Override
  public void initialize(Configuration configuration, KmsClient kmsClient, KeyMaterialStore keyMaterialStore, String fileID) throws IOException {
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
    if (!wrapLocally && !kmsClient.supportsServerSideWrapping()) {
      throw new UnsupportedOperationException("KMS client doesn't support server-side wrapping");
    }
    this.kmsClient = kmsClient;
    this.keyMaterialStore = keyMaterialStore;
    this.fileID = fileID;
    random = new SecureRandom();
    keyCounter = 0;
  }

  @Override
  public KeyWithMetadata getFooterEncryptionKey(String footerMasterKeyID) throws IOException {
    return generateDataKey(footerMasterKeyID);
  }

  @Override
  public KeyWithMetadata getColumnEncryptionKey(ColumnPath column, String columnMasterKeyID) throws IOException {
    return generateDataKey(columnMasterKeyID);
  }
  
  @Override
  public DecryptionKeyRetriever getDecryptionKeyRetriever() {
    return new WrappedKeyRetriever(kmsClient, wrapLocally, keyMaterialStore, fileID);
  }

  @Override
  public void close() {
    // TODO Wipe keys
  }

  /**
   * Generates random data encryption key, and creates its metadata.
   * The metadata is comprised of the wrapped data key (encrypted with master key), and the identity of the master key.
   * @param masterKeyID
   * @return
   * @throws IOException
   */
  private KeyWithMetadata generateDataKey(String masterKeyID) throws IOException {
    byte[] dataKey = new byte[16]; //TODO length. configure via properties
    random.nextBytes(dataKey);
    String encodedWrappedDataKey = null;
    if (wrapLocally) {
      String encodedMasterKey;
      try {
        encodedMasterKey = kmsClient.getKeyFromServer(masterKeyID);
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
      byte[] wrappedDataKey = keyEncryptor.encrypt(false, dataKey, null);
      encodedWrappedDataKey = Base64.getEncoder().encodeToString(wrappedDataKey);
    }
    else {
      String encodedDataKey = Base64.getEncoder().encodeToString(dataKey);
      try {
        encodedWrappedDataKey = kmsClient.wrapDataKeyInServer(encodedDataKey, masterKeyID);
      } 
      catch (KeyAccessDeniedException e) {
        throw new IOException("Unauthorized to wrap with master key: " + masterKeyID, e);
      } 
      catch (UnsupportedOperationException e) {
        throw new IOException("KMS client doesnt support key wrapping", e);
      }
    }
    
    Map<String, String> keyMaterialMap = new HashMap<String, String>(4);
    keyMaterialMap.put(WRAPPING_METHOD_FIELD, wrappingMethod);
    keyMaterialMap.put(WRAPPING_METHOD_VERSION_FIELD, wrappingMethodVersion);
    keyMaterialMap.put(MASTER_KEY_ID_FIELD, masterKeyID);
    keyMaterialMap.put(WRAPPED_KEY_FIELD, encodedWrappedDataKey);
    String keyMaterial = JSONValue.toJSONString(keyMaterialMap);
        
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
    KeyWithMetadata key = new KeyWithMetadata(dataKey, keyMetadata);
    return key;
  }
}
