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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;

public class HadoopFSKeyMaterialStore implements FileKeyMaterialStore {
  public static final String KEY_MATERIAL_FILE_PREFIX = "_KEY_MATERIAL_FOR_";
  public static final String TEMP_FILE_PREFIX = "_TMP";
  public static final String KEY_MATERIAL_FILE_SUFFFIX = ".json";
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private FileSystem hadoopFileSystem;
  private Map<String, String> keyMaterialMap;
  private Path keyMaterialFile;

  HadoopFSKeyMaterialStore(FileSystem hadoopFileSystem) {
    this.hadoopFileSystem = hadoopFileSystem;
  }

  @Override
  public void initialize(Path parquetFilePath, Configuration hadoopConfig, boolean tempStore) {
    String fullPrefix = (tempStore ? TEMP_FILE_PREFIX : "");
    fullPrefix += KEY_MATERIAL_FILE_PREFIX;
    keyMaterialFile = new Path(
        parquetFilePath.getParent(), fullPrefix + parquetFilePath.getName() + KEY_MATERIAL_FILE_SUFFFIX);
  }

  @Override
  public void addKeyMaterial(String keyIDInFile, String keyMaterial) throws ParquetCryptoRuntimeException {
    if (null == keyMaterialMap) {
      keyMaterialMap = new HashMap<>();
    }
    keyMaterialMap.put(keyIDInFile, keyMaterial);
  }

  @Override
  public String getKeyMaterial(String keyIDInFile) throws ParquetCryptoRuntimeException {
    if (null == keyMaterialMap) {
      loadKeyMaterialMap();
    }
    return keyMaterialMap.get(keyIDInFile);
  }

  private void loadKeyMaterialMap() {
    try (FSDataInputStream keyMaterialStream = hadoopFileSystem.open(keyMaterialFile)) {
      JsonNode keyMaterialJson = objectMapper.readTree(keyMaterialStream);
      keyMaterialMap =
          objectMapper.readValue(keyMaterialJson.traverse(), new TypeReference<Map<String, String>>() {});
    } catch (FileNotFoundException e) {
      throw new ParquetCryptoRuntimeException("External key material not found at " + keyMaterialFile, e);
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Failed to get key material from " + keyMaterialFile, e);
    }
  }

  @Override
  public void saveMaterial() throws ParquetCryptoRuntimeException {
    try (FSDataOutputStream keyMaterialStream = hadoopFileSystem.create(keyMaterialFile)) {
      objectMapper.writeValue((OutputStream) keyMaterialStream, keyMaterialMap);
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Failed to save key material in " + keyMaterialFile, e);
    }
  }

  @Override
  public Set<String> getKeyIDSet() throws ParquetCryptoRuntimeException {
    if (null == keyMaterialMap) {
      loadKeyMaterialMap();
    }

    return keyMaterialMap.keySet();
  }

  @Override
  public void removeMaterial() throws ParquetCryptoRuntimeException {
    try {
      hadoopFileSystem.delete(keyMaterialFile, false);
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Failed to delete key material file " + keyMaterialFile, e);
    }
  }

  @Override
  public void moveMaterialTo(FileKeyMaterialStore keyMaterialStore) throws ParquetCryptoRuntimeException {
    // Currently supports only moving to a HadoopFSKeyMaterialStore
    HadoopFSKeyMaterialStore targetStore;
    try {
      targetStore = (HadoopFSKeyMaterialStore) keyMaterialStore;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          "Currently supports only moving to HadoopFSKeyMaterialStore, not to " + keyMaterialStore.getClass(),
          e);
    }
    Path targetKeyMaterialFile = targetStore.getStorageFilePath();
    try {
      hadoopFileSystem.rename(keyMaterialFile, targetKeyMaterialFile);
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Failed to rename file " + keyMaterialFile, e);
    }
  }

  private Path getStorageFilePath() {
    return keyMaterialFile;
  }
}
