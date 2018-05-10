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


package org.apache.parquet.crypto;


public class DecryptionSetup {
  
  private byte[] keyBytes;
  private byte[] aadBytes;
  private DecryptionKeyRetriever keyRetriever;

  /**
   * Configure a file decryptor with an explicit key. If applied on a file that contains key metadata - 
   * the metadata will be ignored, the file will be decrypted with the provided explicit key.
   * @param keyBytes
   */
  public DecryptionSetup(byte[] keyBytes) {
    this.keyBytes = keyBytes;
  }
  
  /**
   * Configure a file decryptor with a key retriever callback. If applied on a file that doesn't contain key metadata - 
   * an exception will be thrown.
   * @param keyRetriever
   */
  public DecryptionSetup(DecryptionKeyRetriever keyRetriever) {
    this.keyRetriever = keyRetriever;
  }

  /**
   * Set the AES-GCM additional authenticated data (AAD).
   * 
   * @param aad
   */
  public void setAAD(byte[] aad) {
    aadBytes = aad;
  }

  byte[] getKeyBytes() {
    return keyBytes;
  }

  DecryptionKeyRetriever getKeyRetriever() {
    return keyRetriever;
  }

  byte[] getAAD() {
    return aadBytes;
  }
}
