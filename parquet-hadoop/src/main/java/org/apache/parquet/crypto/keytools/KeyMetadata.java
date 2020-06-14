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
import java.util.HashMap;
import java.util.Map;

import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class KeyMetadata {
  private static final String KEY_REFERENCE_FIELD = "keyReference";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final boolean isInternalStorage;
  private final String keyReference;
  private final KeyMaterial keyMaterial;

  private KeyMetadata(boolean isInternalStorage, String keyReference, KeyMaterial keyMaterial) {
    this.isInternalStorage = isInternalStorage;
    this.keyReference = keyReference;
    this.keyMaterial = keyMaterial;
  }

  static KeyMetadata parse(byte[] keyMetadataBytes) {
    String keyMetaDataString = new String(keyMetadataBytes, StandardCharsets.UTF_8);
    Map<String, String> keyMetadataJson = null;
    try {
      keyMetadataJson = OBJECT_MAPPER.readValue(new StringReader(keyMetaDataString),
          new TypeReference<Map<String, String>>() {});
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Failed to parse key metadata " + keyMetaDataString, e);
    }

    String keyMaterialType = keyMetadataJson.get(KeyMaterial.KEY_MATERIAL_TYPE_FIELD);
    if (!KeyMaterial.KEY_MATERIAL_TYPE.equals(keyMaterialType)) {
      throw new ParquetCryptoRuntimeException("Wrong key material type: " + keyMaterialType + 
          " vs " + KeyMaterial.KEY_MATERIAL_TYPE);
    }

    boolean isInternalStorage = Boolean.parseBoolean(keyMetadataJson.get(KeyMaterial.KEY_MATERIAL_INTERNAL_STORAGE_FIELD));
    String keyReference;
    KeyMaterial keyMaterial;

    if (isInternalStorage) {
      keyReference = null;
      keyMaterial = KeyMaterial.parse(keyMetadataJson);
    } else {
      keyReference = keyMetadataJson.get(KEY_REFERENCE_FIELD);
      keyMaterial = null;
    }
    return new KeyMetadata(isInternalStorage, keyReference, keyMaterial);
  }

  static String createSerializedForExternalMaterial(String keyReference) {
    Map<String, String> keyMetadataMap = new HashMap<String, String>(3);
    keyMetadataMap.put(KeyMaterial.KEY_MATERIAL_TYPE_FIELD, KeyMaterial.KEY_MATERIAL_TYPE);
    keyMetadataMap.put(KeyMaterial.KEY_MATERIAL_INTERNAL_STORAGE_FIELD, "false");
    keyMetadataMap.put(KEY_REFERENCE_FIELD, keyReference);

    try {
      return OBJECT_MAPPER.writeValueAsString(keyMetadataMap);
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Failed to serialize key metadata", e);
    }
  }

  boolean keyMaterialStoredInternally() {
    return isInternalStorage;
  }

  KeyMaterial getKeyMaterial() {
    return keyMaterial;
  }

  String getKeyReference() {
    return keyReference;
  }
}