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
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.parquet.format.BlockCipher;

import java.io.IOException;
import java.security.GeneralSecurityException;

class AesCtrDecryptor implements BlockCipher.Decryptor{

  private final SecretKey key;

  private static final int CTR_NONCE_LENGTH = AesCtrEncryptor.CTR_NONCE_LENGTH;
  private static final int chunkLen = AesCtrEncryptor.chunkLen;
  private final byte[] ivPrefix;


  AesCtrDecryptor(byte[] keyBytes, byte[] ivPrefix) throws IOException {
    if (null == keyBytes) throw new IOException("Null key bytes");
    key = new SecretKeySpec(keyBytes, "AES");
    this.ivPrefix = ivPrefix;
    if (null != ivPrefix) {
      if (ivPrefix.length > CTR_NONCE_LENGTH) throw new IOException("IV prefix length: " + ivPrefix.length);
    }
  }

  @Override
  public byte[] decrypt(byte[] ciphertext)  throws IOException {
    return decrypt(ciphertext, 0, ciphertext.length);
  }

  @Override
  public byte[] decrypt(byte[] ciphertext, int offset, int cLen)  throws IOException {
    byte[] nonce = new byte[CTR_NONCE_LENGTH];
    // Get the nonce
    int noff = 0;
    int nlen = CTR_NONCE_LENGTH;
    if (null != ivPrefix) {
      System.arraycopy(ivPrefix, 0, nonce, 0, ivPrefix.length);
      noff += ivPrefix.length;
      nlen -= ivPrefix.length; 
    }
    if (nlen > 0) System.arraycopy(ciphertext, offset, nonce, noff, nlen);
    IvParameterSpec spec = new IvParameterSpec(nonce);
    byte[] plaintext;
    try {
      // Cipher is not thread safe (using 'synchronized decrypt' kills performance). Create new.
      Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
      cipher.init(Cipher.DECRYPT_MODE, key, spec);
      int plen = cLen - CTR_NONCE_LENGTH;
      if (plen < 1) {
        throw new IOException("Wrong input length " + plen);
      }
      plaintext = new byte[plen];
      int left = cLen - CTR_NONCE_LENGTH;
      int input_offset = offset + CTR_NONCE_LENGTH;
      int output_offset = 0;
      int written;
      // Breaking decryption into multiple updates, to trigger h/w acceleration
      while (left > chunkLen) {
        written = cipher.update(ciphertext, input_offset, chunkLen, plaintext, output_offset);
        input_offset += chunkLen;
        output_offset += written;
        left -= chunkLen;
      }
      cipher.doFinal(ciphertext, input_offset, left, plaintext, output_offset);
    }
    catch (GeneralSecurityException e) {
      throw new IOException("Failed to decrypt", e);
    }
    return plaintext;
  }
}

