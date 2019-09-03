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
import javax.security.auth.DestroyFailedException;

import static org.apache.parquet.crypto.AesEncryptor.NONCE_LENGTH;
import static org.apache.parquet.crypto.AesEncryptor.GCM_TAG_LENGTH;
import static org.apache.parquet.crypto.AesEncryptor.GCM_TAG_LENGTH_BITS;
import static org.apache.parquet.crypto.AesEncryptor.CTR_IV_LENGTH;
import static org.apache.parquet.crypto.AesEncryptor.CHUNK_LENGTH; 
import static org.apache.parquet.crypto.AesEncryptor.SIZE_LENGTH;

import org.apache.parquet.ShouldNeverHappenException;
import org.apache.parquet.crypto.AesEncryptor.AesMode;
import org.apache.parquet.format.BlockCipher;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.LinkedList;

public class AesDecryptor implements BlockCipher.Decryptor{

  private final boolean gcmMode;
  private SecretKey aesKey;
  private final int tagLength;
  private Cipher aesCipher;
  private final byte[] ctrIV;
  private final byte[] nonce;
  private boolean wipedOut;


  public AesDecryptor(AesMode mode, byte[] keyBytes, LinkedList<AesDecryptor> allDecryptors) 
      throws IllegalArgumentException, IOException {
    if (null == keyBytes) {
      throw new IllegalArgumentException("Null key bytes");
    }
    aesKey = new EncryptionKey(keyBytes);

    if (AesMode.GCM == mode) {
      gcmMode = true;
      tagLength = GCM_TAG_LENGTH;
      try {
        aesCipher = Cipher.getInstance(AesMode.GCM.getCipherName());
      } catch (GeneralSecurityException e) {
        throw new IOException("Failed to create GCM cipher", e);
      }
      ctrIV = null;
    } else {
      gcmMode = false;
      tagLength = 0;
      try {
        aesCipher = Cipher.getInstance(AesMode.CTR.getCipherName());
      } catch (GeneralSecurityException e) {
        throw new IOException("Failed to create CTR cipher", e);
      }
      ctrIV = new byte[CTR_IV_LENGTH];
      ctrIV[CTR_IV_LENGTH - 1] = (byte) 1;
    }

    nonce = new byte[NONCE_LENGTH];
    if (null != allDecryptors) allDecryptors.add(this);
    wipedOut = false;
  }

  @Override
  public byte[] decrypt(byte[] lengthAndCiphertext, byte[] AAD)  throws IOException {
    int cipherTextOffset = SIZE_LENGTH;
    int cipherTextLength = lengthAndCiphertext.length - SIZE_LENGTH;
    return decrypt(lengthAndCiphertext, cipherTextOffset, cipherTextLength, AAD);
  }


  public byte[] decrypt(byte[] ciphertext, int cipherTextOffset, int cipherTextLength, byte[] AAD)  
      throws IOException {
    if (wipedOut) {
      throw new IOException("AES decryptor is wiped out");
    }

    int plainTextLength = cipherTextLength - tagLength - NONCE_LENGTH;
    if (plainTextLength < 1) {
      throw new IOException("Wrong input length " + plainTextLength);
    }

    // Get the nonce from ciphertext
    if (gcmMode) {
      System.arraycopy(ciphertext, cipherTextOffset, nonce, 0, NONCE_LENGTH);
    } else {
      System.arraycopy(ciphertext, cipherTextOffset, ctrIV, 0, NONCE_LENGTH);
    }

    byte[] plainText = new byte[plainTextLength];
    int inputLength = cipherTextLength - NONCE_LENGTH;
    int inputOffset = cipherTextOffset + NONCE_LENGTH;
    int outputOffset = 0;
    try {
      if (gcmMode) {
        GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, nonce);
        aesCipher.init(Cipher.DECRYPT_MODE, aesKey, spec);
        if (null != AAD) aesCipher.updateAAD(AAD);
      } else {
        IvParameterSpec spec = new IvParameterSpec(ctrIV);
        aesCipher.init(Cipher.DECRYPT_MODE, aesKey, spec);

        // Breaking decryption into multiple updates, to trigger h/w acceleration in Java 9+
        while (inputLength > CHUNK_LENGTH) {
          int written = aesCipher.update(ciphertext, inputOffset, CHUNK_LENGTH, plainText, outputOffset);
          inputOffset += CHUNK_LENGTH;
          outputOffset += written;
          inputLength -= CHUNK_LENGTH;
        } 
      }
      aesCipher.doFinal(ciphertext, inputOffset, inputLength, plainText, outputOffset);
    }
    catch (GeneralSecurityException e) {
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
        ((lengthBuffer[1] & 0xff) <<  8) |
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

  void wipeOut() {
    wipedOut = true;
    try {
      aesKey.destroy();
    } catch (DestroyFailedException e) {
      throw new ShouldNeverHappenException(e);
    }
    aesCipher = null; // dereference for GC
  }
}

