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

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public interface FileKeyMaterialStore {

  /**
   * Initializes key material store for a parquet file.
   * @param parquetFilePath Parquet file path
   * @param hadoopConfig Hadoop configuration
   * @param tempStore set true if this is a temporary store, used in key rotation
   */
  public void initialize(Path parquetFilePath, Configuration hadoopConfig, boolean tempStore);

  /**
   * Add key material for one encryption key.
   * @param keyIDInFile ID of the key in Parquet file
   * @param keyMaterial key material
   */
  public void addKeyMaterial(String keyIDInFile, String keyMaterial);
  
  /**
   * After key material was added for all keys in the given Parquet file, 
   * save material in persistent store.
   */
  public void saveMaterial();

  /**
   * Get key material
   * @param keyIDInFile ID of a key in Parquet file
   * @return key material
   */
  public String getKeyMaterial(String keyIDInFile);

  /**
   * @return Set of all key IDs in this store (for the given Parquet file) 
   */
  public Set<String> getKeyIDSet();

  /**
   * Remove key material from persistent store. Used in key rotation.
   */
  public void removeMaterial();

  /**
   * Move key material to another store. Used in key rotation.
   * @param targetKeyMaterialStore target store
   */
  public void moveMaterialTo(FileKeyMaterialStore targetKeyMaterialStore);
}