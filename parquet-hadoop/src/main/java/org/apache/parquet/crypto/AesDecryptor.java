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

import static org.apache.parquet.crypto.AesEncryptor.GCM_NONCE_LENGTH;
import static org.apache.parquet.crypto.AesEncryptor.GCM_TAG_LENGTH;
import static org.apache.parquet.crypto.AesEncryptor.CTR_NONCE_LENGTH;
import static org.apache.parquet.crypto.AesEncryptor.CHUNK_LEN;

import org.apache.parquet.crypto.AesEncryptor.Mode;
import org.apache.parquet.format.BlockCipher;

import java.io.IOException;
import java.security.GeneralSecurityException;


class AesDecryptor implements BlockCipher.Decryptor{

  private final Mode aesMode;
  private final SecretKey aesKey;
  private final byte[] AAD;
  private final byte[] ivPrefix;
  private final int nonceLength;
  private final int tagLength;
  private final Cipher aesCipher;


  AesDecryptor(Mode mode, byte[] keyBytes, byte[] aad, byte[] ivPrefix) throws IllegalArgumentException, IOException {
    if (null == keyBytes) {
      throw new IllegalArgumentException("Null key bytes");
    }
    this.aesMode = mode;
    aesKey = new SecretKeySpec(keyBytes, "AES");
    AAD = aad;
    this.ivPrefix = ivPrefix;
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
    if (null != ivPrefix && ivPrefix.length > nonceLength) {
      throw new IllegalArgumentException("IV prefix length (" + ivPrefix.length + 
          ") should be shorter than nonce length (" + nonceLength + ")");
    }
  }

  @Override
  public byte[] decrypt(byte[] cipherText)  throws IOException {
    return decrypt(cipherText, 0, cipherText.length);
  }

  @Override
  public byte[] decrypt(byte[] cipherText, int cipherTextOffset, int cipherTextLength)  throws IOException {
    byte[] nonce = new byte[nonceLength];
    // Get the nonce
    int ivSuffixOffset = 0;
    int ivSuffixLength = nonceLength;
    if (null != ivPrefix) {
      // copy fixed part (iv_prefix)
      System.arraycopy(ivPrefix, 0, nonce, 0, ivPrefix.length);
      ivSuffixOffset = ivPrefix.length;
      ivSuffixLength -= ivPrefix.length; 
    }
    // copy iv (full or suffix) from ciphertext
    if (ivSuffixLength > 0) System.arraycopy(cipherText, cipherTextOffset, nonce, ivSuffixOffset, ivSuffixLength);
    int plainTextLength = cipherTextLength - tagLength - nonceLength;
    if (plainTextLength < 1) {
      throw new IOException("Wrong input length " + plainTextLength);
    }
    byte[] plainText = new byte[plainTextLength];
    int inputLength = cipherTextLength - nonceLength;
    int inputOffset = cipherTextOffset + nonceLength;
    int outputOffset = 0;
    try {
      if (Mode.GCM == aesMode) {
        GCMParameterSpec spec = new GCMParameterSpec(tagLength * 8, nonce);
        aesCipher.init(Cipher.DECRYPT_MODE, aesKey, spec);
        if (null != AAD) aesCipher.updateAAD(AAD);
      }
      else {
        IvParameterSpec spec = new IvParameterSpec(nonce);
        aesCipher.init(Cipher.DECRYPT_MODE, aesKey, spec);
        
        // TODO Doesn't help for GCM in Java 9/10. Check again in Java 11.
        // Breaking decryption into multiple updates, to trigger h/w acceleration
        while (inputLength > CHUNK_LEN) {
          int written = aesCipher.update(cipherText, inputOffset, CHUNK_LEN, plainText, outputOffset);
          inputOffset += CHUNK_LEN;
          outputOffset += written;
          inputLength -= CHUNK_LEN;
        } 
      }
      aesCipher.doFinal(cipherText, inputOffset, inputLength, plainText, outputOffset);
    }
    catch (GeneralSecurityException e) {
      throw new IOException("Failed to decrypt", e);
    }
    return plainText;
  }
}

