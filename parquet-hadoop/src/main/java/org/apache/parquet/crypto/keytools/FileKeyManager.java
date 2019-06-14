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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.hadoop.metadata.ColumnPath;

/**
 * Management of file encryption keys, and their metadata. 
 * For scalability, implementing code is recommended to run locally / not to make remote calls -
 * except for calls to KMS server, via the pre-defined KmsClient interface. 
 *
 * Implementing class instance should be created per each Parquet file. The methods don't need to
 * be thread-safe.
 */
public interface FileKeyManager {
  
  /**
   * 
   * @param configuration
   * @param kmsClient
   * @param keyMaterialStore
   * @param fileID
   * @throws IOException
   */
  public void initialize(Configuration configuration, KmsClient kmsClient, KeyMaterialStore keyMaterialStore, String fileID) throws IOException;
  
  /**
   * Generates or fetched a column data encryption key, and creates its metadata. 
   * Eg can generate a random data key, and wrap it with a master key. ColumnKeyID is the master key then.
   * Or can fetch the data key from KMS. ColumnKeyID is the data key in this case.
   * @param column
   * @param columnKeyID 
   * @return
   * @throws IOException
   */
  public KeyWithMetadata getColumnEncryptionKey(ColumnPath column, String columnKeyID) throws IOException;
  
  /**
   * Footer key metadata can store additional information (such as KMS instance identity) - so it doesnt have
   * to be stored in each column key metadata.
   * @param footerKeyID
   * @return
   * @throws IOException
   */
  public KeyWithMetadata getFooterEncryptionKey(String footerKeyID) throws IOException;
  
  public DecryptionKeyRetriever getDecryptionKeyRetriever();
  
  /**
   * Wipes keys in memory.
   * Flushes key material to external store (if in use).
   */
  public void close();
}
