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
import java.util.Base64;

import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.crypto.KeyAccessDeniedException;


public class DirectKeyManager {

  private final KmsClient kmsClient;
  
  public static class DirectKeyRetriever implements DecryptionKeyRetriever {
    private final KmsClient kmsClient;
    
    private DirectKeyRetriever(KmsClient kmsClient) {
      this.kmsClient = kmsClient;
    }
    
    @Override
    public byte[] getKey(byte[] keyMetaData) throws IOException {
      String dataKeyID = new String(keyMetaData, StandardCharsets.UTF_8);
      String encodedDek;
      try {
        encodedDek = kmsClient.getKeyFromServer(dataKeyID);
      } 
      catch (KeyAccessDeniedException e) {
        return null;
      }
      byte[] dek = Base64.getDecoder().decode(encodedDek);
      return dek;
    }
  }

  public DirectKeyManager(KmsClient kmsClient) {
    this.kmsClient = kmsClient;
  }

  public ParquetKey getKey(String dataKeyID) throws IOException {
    String encodedDek;
    try {
      encodedDek = kmsClient.getKeyFromServer(dataKeyID);
    } 
    catch (KeyAccessDeniedException e) {
      return new ParquetKey((byte[])null, (byte[])null);
    }
    byte[] dek = Base64.getDecoder().decode(encodedDek);
    byte[] keyMetadata = dataKeyID.getBytes(StandardCharsets.UTF_8);
    return new ParquetKey(dek, keyMetadata);
  }

  public DecryptionKeyRetriever getKeyRetriever() {
    return new DirectKeyRetriever(kmsClient);
  }
}


