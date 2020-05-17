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
package org.apache.parquet.crypto.keytools.samples;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.keytools.KmsClient;
import org.apache.parquet.crypto.keytools.KeyTookit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InMemoryKMS implements KmsClient {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryKMS.class);

  public static final String KEY_LIST_PROPERTY_NAME = "encryption.key.list";
  public static final String NEW_KEY_LIST_PROPERTY_NAME = "encryption.new.key.list"; // optional, for key rotation

  private Map<String,byte[]> masterKeyMap;
  private Map<String,byte[]> newMasterKeyMap;

  @Override
  public void initialize(Configuration configuration, String kmsInstanceID) {

    // Parse master  keys
    String[] masterKeys = configuration.getTrimmedStrings(KEY_LIST_PROPERTY_NAME);
    if (null == masterKeys || masterKeys.length == 0) {
      throw new ParquetCryptoRuntimeException("No encryption key list");
    }
    masterKeyMap = parseKeyList(masterKeys);

    // Parse new master keys (if available, for key rotation)
    String[] newMasterKeys = configuration.getTrimmedStrings(NEW_KEY_LIST_PROPERTY_NAME);
    if (null == newMasterKeys || newMasterKeys.length == 0) {
      newMasterKeyMap = masterKeyMap;
    } else {
      newMasterKeyMap = parseKeyList(newMasterKeys);
    }
  }

  private static Map<String, byte[]> parseKeyList(String[] masterKeys) {
    Map<String,byte[]> keyMap = new HashMap<>();

    int nKeys = masterKeys.length;
    for (int i=0; i < nKeys; i++) {
      String[] parts = masterKeys[i].split(":");
      String keyName = parts[0].trim();
      if (parts.length != 2) {
        throw new IllegalArgumentException("Key '" + keyName + "' is not formatted correctly");
      }
      String key = parts[1].trim();
      try {
        byte[] keyBytes = Base64.getDecoder().decode(key);
        keyMap.put(keyName, keyBytes);
      } catch (IllegalArgumentException e) {
        LOG.warn("Could not decode key '" + keyName + "'!");
        throw e;
      }
    }
    return keyMap;
  }

  @Override
  public String wrapKey(byte[] dataKey, String masterKeyIdentifier) {
    byte[] masterKey = newMasterKeyMap.get(masterKeyIdentifier);
    if (null == masterKey) {
      throw new ParquetCryptoRuntimeException("Key not found: " + masterKeyIdentifier);
    }
    byte[] AAD = masterKeyIdentifier.getBytes(StandardCharsets.UTF_8);
    return KeyTookit.wrapKeyLocally(dataKey, masterKey, AAD);
  }

  @Override
  public byte[] unwrapKey(String wrappedDataKey, String masterKeyIdentifier) {
    byte[] masterKey = masterKeyMap.get(masterKeyIdentifier);
    if (null == masterKey) {
      throw new ParquetCryptoRuntimeException("Key not found: " + masterKeyIdentifier);
    }
    byte[] AAD = masterKeyIdentifier.getBytes(StandardCharsets.UTF_8);
    return KeyTookit.unwrapKeyLocally(wrappedDataKey, masterKey, AAD);
  }
}