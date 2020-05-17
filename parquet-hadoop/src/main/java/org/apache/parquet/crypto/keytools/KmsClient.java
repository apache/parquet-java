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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.KeyAccessDeniedException;

public interface KmsClient {
  /**
   * Pass configuration with KMS-specific parameters.
   * @param configuration Hadoop configuration
   * @param kmsInstanceID ID of the KMS instance handled by this KmsClient. 
   *                      When writing a parquet file, the KMS instance ID has to be specified in configuration, 
   *                      and will be stored in parquet key material. 
   *                      When reading a parquet file, the KMS instance ID can be either specified in configuration 
   *                      or read from parquet key material.
   *                      ID can have a default value, for KMS systems that don't work with multiple instances.
   */
  public void initialize(Configuration configuration, String kmsInstanceID);

  /**
   * Wraps a key - encrypts it with the master key, encodes the result 
   * and potentially adds a KMS-specific metadata.
   * 
   * If your KMS client code throws runtime exceptions related to access/permission problems
   * (such as Hadoop AccessControlException), catch them and throw the KeyAccessDeniedException.
   * 
   * @param keyBytes: key bytes to be wrapped
   * @param masterKeyIdentifier: a string that uniquely identifies the master key in a KMS instance
   * @return
   * @throws KeyAccessDeniedException unauthorized to encrypt with the given master key
   */
  public String wrapKey(byte[] keyBytes, String masterKeyIdentifier)
      throws KeyAccessDeniedException;

  /**
   * Decrypts (unwraps) a key with the master key. 
   * 
   * If your KMS client code throws runtime exceptions related to access/permission problems
   * (such as Hadoop AccessControlException), catch them and throw the KeyAccessDeniedException.
   * 
   * @param wrappedKey String produced by wrapKey operation
   * @param masterKeyIdentifier: a string that uniquely identifies the master key in a KMS instance
   * @return
   * @throws KeyAccessDeniedException unauthorized to unwrap with the given master key
   */
  public byte[] unwrapKey(String wrappedKey, String masterKeyIdentifier)
      throws KeyAccessDeniedException;
}