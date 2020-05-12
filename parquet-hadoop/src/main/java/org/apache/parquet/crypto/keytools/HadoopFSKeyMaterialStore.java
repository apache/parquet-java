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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ShouldNeverHappenException;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;

public class HadoopFSKeyMaterialStore implements FileKeyMaterialStore {

  public final static String KEY_MATERIAL_FILE_PREXIX = "_KEY_MATERIAL_FOR_";

  private final FileSystem hadoopFileSystem;
  private HashMap<String, String> keyMaterialMap;
  private Path keyMaterialFile;

  HadoopFSKeyMaterialStore(FileSystem hadoopFileSystem, Path parquetFilePath) {
    this(hadoopFileSystem, parquetFilePath, null);
  }

  HadoopFSKeyMaterialStore(FileSystem hadoopFileSystem, Path parquetFilePath, String prefix) {
    this.hadoopFileSystem = hadoopFileSystem;
    String fullPrefix = "";
    if (null != prefix) {
      fullPrefix = prefix;
    }
    fullPrefix += KEY_MATERIAL_FILE_PREXIX;
    keyMaterialFile = new Path(parquetFilePath.getParent(), fullPrefix + parquetFilePath.getName());
  }

  @Override
  public void initialize(Path parquetFilePath) {
  }

  @Override
  public void addKeyMaterial(String keyIDInFile, String keyMaterial) throws ParquetCryptoRuntimeException {
    if (null == keyMaterialMap) {
      keyMaterialMap = new HashMap<String, String>();
    }
    keyMaterialMap.put(keyIDInFile, keyMaterial);
  }


  @Override
  public String getKeyMaterial(String keyIDInFile)  throws ParquetCryptoRuntimeException {
    if (null == keyMaterialMap) {
      loadKeyMaterialMap();
    }

    return keyMaterialMap.get(keyIDInFile);
  }

  @SuppressWarnings("unchecked")
  private void loadKeyMaterialMap() {
    try {
      FSDataInputStream keyMaterialStream = hadoopFileSystem.open(keyMaterialFile);

      ObjectInputStream objectStream = new ObjectInputStream(keyMaterialStream);
      try {
        keyMaterialMap = (HashMap<String,String>) objectStream.readObject(); // TODO run instanceof, to get rid of SupressWarning
      } catch (ClassNotFoundException e) {
        throw new ShouldNeverHappenException(e);
      }
      objectStream.close();
      keyMaterialStream.close();
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Failed to get key material from " + keyMaterialFile, e);
    }
  }

  @Override
  public void saveFileKeyMaterial() throws ParquetCryptoRuntimeException {
    // TODO needed? Path qualifiedPath = parquetFilePath.makeQualified(hadoopFileSystem);
    try {
      FSDataOutputStream keyMaterialStream = hadoopFileSystem.create(keyMaterialFile);
      ObjectOutputStream objectStream = new ObjectOutputStream(keyMaterialStream);
      objectStream.writeObject(keyMaterialMap);
      objectStream.close();
      keyMaterialStream.close();
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Failed to save key material in " + keyMaterialFile, e);
    }
  }

  @Override
  public Set<String> getFileKeyIDSet() throws ParquetCryptoRuntimeException {
    if (null == keyMaterialMap) {
      loadKeyMaterialMap();
    }

    return keyMaterialMap.keySet();
  }

  @Override
  public void removeFileKeyMaterial() throws ParquetCryptoRuntimeException {
    try {
      hadoopFileSystem.delete(keyMaterialFile, false);
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException(e); // TODO file names
    }
  }

  @Override
  public void moveFileKeyMaterial(FileKeyMaterialStore keyMaterialStore) throws ParquetCryptoRuntimeException {
    HadoopFSKeyMaterialStore targetStore = (HadoopFSKeyMaterialStore) keyMaterialStore;
    Path targetKeyMaterialFile = targetStore.getStorageFilePath();
    try {
      hadoopFileSystem.rename(keyMaterialFile, targetKeyMaterialFile);
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException(e); // TODO file names
    }
  }

  private Path getStorageFilePath() {
    return keyMaterialFile;
  }
}