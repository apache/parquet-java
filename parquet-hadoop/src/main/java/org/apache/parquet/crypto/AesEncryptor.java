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


import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.parquet.format.BlockCipher;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

class AesEncryptor implements BlockCipher.Encryptor{

  enum Mode {
    GCM, CTR
  }

  static final int GCM_NONCE_LENGTH = 12;
  static final int CTR_NONCE_LENGTH = 16;
  static final int GCM_TAG_LENGTH = 16;
  static final int CHUNK_LEN = 4 * 1024;

  private final SecretKey aesKey;
  private final SecureRandom randomGenerator;
  private final byte[] AAD;
  private final int nonceLength;
  private final int tagLength;
  private final Cipher aesCipher;
  private final Mode aesMode;


  AesEncryptor(Mode mode, byte[] keyBytes, byte[] aad) throws IllegalArgumentException, IOException {
    if (null == keyBytes) {
      throw new IllegalArgumentException("Null key bytes");
    }
    aesKey = new SecretKeySpec(keyBytes, "AES");
    randomGenerator = new SecureRandom();
    AAD = aad;
    aesMode = mode;
    
    if (Mode.GCM == mode) {
      nonceLength = GCM_NONCE_LENGTH;
      tagLength = GCM_TAG_LENGTH;
      try {
        aesCipher = Cipher.getInstance("AES/GCM/NoPadding");
      } catch (GeneralSecurityException e) {
        throw new IOException("Failed to create GCM cipher", e);
      }
    }
    else {
      nonceLength = CTR_NONCE_LENGTH;
      tagLength = 0;
      try {
        aesCipher = Cipher.getInstance("AES/CTR/NoPadding");
      } catch (GeneralSecurityException e) {
        throw new IOException("Failed to create CTR cipher", e);
      }
    }
  }

  @Override
  public byte[] encrypt(byte[] plainText)  throws IOException {
    return encrypt(plainText, 0, plainText.length);
  }

  @Override
  public byte[] encrypt(byte[] plainText, int plainTextOffset, int plainTextLength)  throws IOException {
    byte[] nonce = new byte[nonceLength];
    randomGenerator.nextBytes(nonce);
    int cipherTextLength = plainTextLength + nonceLength + tagLength;
    byte[] cipherText = new byte[cipherTextLength];
    int inputLength = plainTextLength;
    int inputOffset = plainTextOffset;
    int outputOffset = nonceLength;
    try {
      if (Mode.GCM == aesMode) {
        GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, nonce);
        aesCipher.init(Cipher.ENCRYPT_MODE, aesKey, spec);
        if (null != AAD) aesCipher.updateAAD(AAD);
      }
      else {
        IvParameterSpec spec = new IvParameterSpec(nonce);
        aesCipher.init(Cipher.ENCRYPT_MODE, aesKey, spec);
      }
      // Breaking encryption into multiple updates, to trigger h/w acceleration in Java 9, 10
      while (inputLength > CHUNK_LEN) {
        int written = aesCipher.update(plainText, inputOffset, CHUNK_LEN, cipherText, outputOffset);
        inputOffset += CHUNK_LEN;
        outputOffset += written;
        inputLength -= CHUNK_LEN;
      }
      aesCipher.doFinal(plainText, inputOffset, inputLength, cipherText, outputOffset);
    }
    catch (GeneralSecurityException e) {
      throw new IOException("Failed to encrypt", e);
    }
    // Add the nonce (IV)
    System.arraycopy(nonce, 0, cipherText, 0, nonceLength);

    return cipherText;
  }
}

