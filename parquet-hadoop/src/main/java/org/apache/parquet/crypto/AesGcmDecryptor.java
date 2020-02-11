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
import javax.crypto.spec.GCMParameterSpec;

import org.apache.parquet.format.BlockCipher;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;

public class AesGcmDecryptor extends AesCipher implements BlockCipher.Decryptor{


  AesGcmDecryptor(byte[] keyBytes) throws IllegalArgumentException, IOException {
    super(AesMode.GCM, keyBytes);

    try {
      cipher = Cipher.getInstance(AesMode.GCM.getCipherName());
    } catch (GeneralSecurityException e) {
      throw new IOException("Failed to create GCM cipher", e);
    }
  }

  @Override
  public byte[] decrypt(byte[] lengthAndCiphertext, byte[] AAD)  throws IOException {
    int cipherTextOffset = SIZE_LENGTH;
    int cipherTextLength = lengthAndCiphertext.length - SIZE_LENGTH;

    return decrypt(lengthAndCiphertext, cipherTextOffset, cipherTextLength, AAD);
  }

  public byte[] decrypt(byte[] ciphertext, int cipherTextOffset, int cipherTextLength, byte[] AAD)  
      throws IOException {

    int plainTextLength = cipherTextLength - GCM_TAG_LENGTH - NONCE_LENGTH;
    if (plainTextLength < 1) {
      throw new IOException("Wrong input length " + plainTextLength);
    }

    // Get the nonce from ciphertext
    System.arraycopy(ciphertext, cipherTextOffset, localNonce, 0, NONCE_LENGTH);

    byte[] plainText = new byte[plainTextLength];
    int inputLength = cipherTextLength - NONCE_LENGTH;
    int inputOffset = cipherTextOffset + NONCE_LENGTH;
    int outputOffset = 0;
    try {
      GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, localNonce);
      cipher.init(Cipher.DECRYPT_MODE, aesKey, spec);
      if (null != AAD) cipher.updateAAD(AAD);

      cipher.doFinal(ciphertext, inputOffset, inputLength, plainText, outputOffset);
    }  catch (GeneralSecurityException e) {
      throw new IOException("Failed to decrypt", e);
    }

    return plainText;
  }

  @Override
  public byte[] decrypt(InputStream from, byte[] AAD) throws IOException {
    byte[] lengthBuffer = new byte[SIZE_LENGTH];
    int gotBytes = 0;

    // Read the length of encrypted Thrift structure
    while (gotBytes < SIZE_LENGTH) {
      int n = from.read(lengthBuffer, gotBytes, SIZE_LENGTH - gotBytes);
      if (n <= 0) {
        throw new IOException("Tried to read int (4 bytes), but only got " + gotBytes + " bytes.");
      }
      gotBytes += n;
    }

    final int ciphertextLength =
        ((lengthBuffer[3] & 0xff) << 24) |
        ((lengthBuffer[2] & 0xff) << 16) |
        ((lengthBuffer[1] & 0xff) << 8)  |
        ((lengthBuffer[0] & 0xff));

    if (ciphertextLength < 1) {
      throw new IOException("Wrong length of encrypted metadata: " + ciphertextLength);
    }

    byte[] ciphertextBuffer = new byte[ciphertextLength];
    gotBytes = 0;
    // Read the encrypted structure contents
    while (gotBytes < ciphertextLength) {
      int n = from.read(ciphertextBuffer, gotBytes, ciphertextLength - gotBytes);
      if (n <= 0) {
        throw new IOException("Tried to read " + ciphertextLength + " bytes, but only got " + gotBytes + " bytes.");
      }
      gotBytes += n;
    }

    // Decrypt the structure contents
    return decrypt(ciphertextBuffer, 0, ciphertextLength, AAD);
  }
}
