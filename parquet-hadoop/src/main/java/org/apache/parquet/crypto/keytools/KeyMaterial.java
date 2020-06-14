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
import java.util.HashMap;
import java.util.Map;

import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class KeyMaterial {
  static final String KEY_MATERIAL_TYPE_FIELD = "keyMaterialType";
  static final String KEY_MATERIAL_TYPE = "PKMT1";
  static final String KEY_MATERIAL_INTERNAL_STORAGE_FIELD = "internalStorage";

  static final String FOOTER_KEY_ID_IN_FILE = "footerKey";
  static final String COLUMN_KEY_ID_IN_FILE_PREFIX = "columnKey";
  
  private static final String IS_FOOTER_KEY_FIELD = "isFooterKey";
  private static final String DOUBLE_WRAPPING_FIELD = "doubleWrapping";
  private static final String KMS_INSTANCE_ID_FIELD = "kmsInstanceID";
  private static final String KMS_INSTANCE_URL_FIELD = "kmsInstanceURL";
  private static final String MASTER_KEY_ID_FIELD = "masterKeyID";
  private static final String WRAPPED_DEK_FIELD = "wrappedDEK";
  private static final String KEK_ID_FIELD = "keyEncryptionKeyID";
  private static final String WRAPPED_KEK_FIELD = "wrappedKEK";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final boolean isFooterKey;
  private final String kmsInstanceID;
  private final String kmsInstanceURL;
  private final String masterKeyID;
  private final boolean isDoubleWrapped;
  private final String kekID;
  private final String encodedWrappedKEK;
  private final String encodedWrappedDEK;

  private KeyMaterial(boolean isFooterKey, String kmsInstanceID, String kmsInstanceURL, String masterKeyID, 
      boolean isDoubleWrapped, String kekID, String encodedWrappedKEK, String encodedWrappedDEK) {
    this.isFooterKey = isFooterKey;
    this.kmsInstanceID = kmsInstanceID;
    this.kmsInstanceURL = kmsInstanceURL;
    this.masterKeyID = masterKeyID;
    this.isDoubleWrapped = isDoubleWrapped;
    this.kekID = kekID;
    this.encodedWrappedKEK = encodedWrappedKEK;
    this.encodedWrappedDEK = encodedWrappedDEK;
  }

  static KeyMaterial parse(Map<String, String> keyMaterialJson) {
    boolean isFooterKey = Boolean.valueOf(keyMaterialJson.get(IS_FOOTER_KEY_FIELD));
    String kmsInstanceID = null;
    String kmsInstanceURL = null;
    if (isFooterKey) {
      kmsInstanceID = keyMaterialJson.get(KMS_INSTANCE_ID_FIELD);
      kmsInstanceURL = keyMaterialJson.get(KMS_INSTANCE_URL_FIELD);
    }
    boolean isDoubleWrapped = Boolean.valueOf(keyMaterialJson.get(DOUBLE_WRAPPING_FIELD));
    String masterKeyID = keyMaterialJson.get(MASTER_KEY_ID_FIELD);
    String  encodedWrappedDEK = keyMaterialJson.get(WRAPPED_DEK_FIELD);
    String kekID = null;
    String encodedWrappedKEK = null;
    if (isDoubleWrapped) {
      kekID = keyMaterialJson.get(KEK_ID_FIELD);
      encodedWrappedKEK = keyMaterialJson.get(WRAPPED_KEK_FIELD);
    }

    return new KeyMaterial(isFooterKey, kmsInstanceID, kmsInstanceURL, masterKeyID, isDoubleWrapped, kekID, encodedWrappedKEK, encodedWrappedDEK);
  }

  static KeyMaterial parse(String keyMaterialString) {
    Map<String, String> keyMaterialJson = null;
    try {
      keyMaterialJson = OBJECT_MAPPER.readValue(new StringReader(keyMaterialString),
          new TypeReference<Map<String, String>>() {});
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Failed to parse key metadata " + keyMaterialString, e);
    }
    String keyMaterialType = keyMaterialJson.get(KEY_MATERIAL_TYPE_FIELD);
    if (!KEY_MATERIAL_TYPE.equals(keyMaterialType)) {
      throw new ParquetCryptoRuntimeException("Wrong key material type: " + keyMaterialType + 
          " vs " + KEY_MATERIAL_TYPE);
    }
    return parse(keyMaterialJson);
  }

  static String createSerialized(boolean isFooterKey, String kmsInstanceID, String kmsInstanceURL, String masterKeyID, 
      boolean isDoubleWrapped, String kekID, String encodedWrappedKEK, String encodedWrappedDEK, boolean isInternalStorage) {
    Map<String, String> keyMaterialMap = new HashMap<String, String>(10);
    keyMaterialMap.put(KEY_MATERIAL_TYPE_FIELD, KEY_MATERIAL_TYPE);
    if (isInternalStorage) {
      keyMaterialMap.put(KEY_MATERIAL_INTERNAL_STORAGE_FIELD, "true");
    }
    keyMaterialMap.put(IS_FOOTER_KEY_FIELD, Boolean.toString(isFooterKey));
    if (isFooterKey) {
      keyMaterialMap.put(KMS_INSTANCE_ID_FIELD, kmsInstanceID);
      keyMaterialMap.put(KMS_INSTANCE_URL_FIELD, kmsInstanceURL);
    }
    keyMaterialMap.put(MASTER_KEY_ID_FIELD, masterKeyID);
    keyMaterialMap.put(WRAPPED_DEK_FIELD, encodedWrappedDEK);
    keyMaterialMap.put(DOUBLE_WRAPPING_FIELD, Boolean.toString(isDoubleWrapped));
    if (isDoubleWrapped) {
      keyMaterialMap.put(KEK_ID_FIELD, kekID);
      keyMaterialMap.put(WRAPPED_KEK_FIELD, encodedWrappedKEK);
    }

    try {
      return OBJECT_MAPPER.writeValueAsString(keyMaterialMap);
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Failed to serialize key material", e);
    }
  }

  boolean isFooterKey() {
    return isFooterKey;
  }

  boolean isDoubleWrapped() {
    return isDoubleWrapped;
  }

  String getMasterKeyID() {
    return masterKeyID;
  }

  String getWrappedDEK() {
    return encodedWrappedDEK;
  }

  String getKekID() {
    return kekID;
  }

  String getWrappedKEK() {
    return encodedWrappedKEK;
  }

  String getKmsInstanceID() {
    return kmsInstanceID;
  }

  String getKmsInstanceURL() {
    return kmsInstanceURL;
  }
}