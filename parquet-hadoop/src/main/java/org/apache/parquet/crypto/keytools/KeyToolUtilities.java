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

import org.apache.parquet.crypto.AesGcmDecryptor;
import org.apache.parquet.crypto.AesGcmEncryptor;
import org.apache.parquet.crypto.AesMode;
import org.apache.parquet.crypto.ModuleCipherFactory;

import java.util.Base64;


public class KeyToolUtilities {

  static class KeyWithMasterID {

    private final byte[] keyBytes;
    private final String masterID ;

    KeyWithMasterID(byte[] keyBytes, String masterID) {
      this.keyBytes = keyBytes;
      this.masterID = masterID;
    }

    byte[] getDataKey() {
      return keyBytes;
    }

    String getMasterID() {
      return masterID;
    }
  }

  static class KeyEncryptionKey {
    private final byte[] kekBytes;
    private final byte[] kekID;
    private final String encodedKEK_ID;
    private final String encodedWrappedKEK;

    KeyEncryptionKey(byte[] kekBytes, String encodedKEK_ID, byte[] kekID, String encodedWrappedKEK) {
      this.kekBytes = kekBytes;
      this.kekID = kekID;
      this.encodedKEK_ID = encodedKEK_ID;
      this.encodedWrappedKEK = encodedWrappedKEK;
    }

    byte[] getBytes() {
      return kekBytes;
    }

    byte[] getID() {
      return kekID;
    }

    String getEncodedID() {
      return encodedKEK_ID;
    }

    String getWrappedWithCRK() {
      return encodedWrappedKEK;
    }
  }

  public static String wrapKeyLocally(byte[] key, byte[] wrappingKey, byte[] AAD) {
    AesGcmEncryptor keyEncryptor;

    keyEncryptor = (AesGcmEncryptor) ModuleCipherFactory.getEncryptor(AesMode.GCM, wrappingKey);

    byte[] wrappedKey = keyEncryptor.encrypt(false, key, AAD);

    return Base64.getEncoder().encodeToString(wrappedKey);
  }

  public static byte[] unwrapKeyLocally(String encodedWrappedKey, byte[] wrappingKey, byte[] AAD) {
    byte[] wrappedKEy = Base64.getDecoder().decode(encodedWrappedKey);
    AesGcmDecryptor keyDecryptor;

    keyDecryptor = (AesGcmDecryptor) ModuleCipherFactory.getDecryptor(AesMode.GCM, wrappingKey);

    return keyDecryptor.decrypt(wrappedKEy, 0, wrappedKEy.length, AAD);
  }
}