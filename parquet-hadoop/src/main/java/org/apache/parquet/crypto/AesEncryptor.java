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
import javax.crypto.spec.IvParameterSpec;
import javax.security.auth.DestroyFailedException;

import org.apache.parquet.ShouldNeverHappenException;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.format.BlockCipher;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.LinkedList;

public class AesEncryptor implements BlockCipher.Encryptor{

  public enum Mode {
    GCM, CTR
  }
  
  // Module types
  public static final byte Footer = 0;
  public static final byte ColumnMetaData = 1;
  public static final byte DataPage = 2;
  public static final byte DictionaryPage = 3;
  public static final byte DataPageHeader = 4;
  public static final byte DictionaryPageHeader = 5;
  public static final byte ColumnIndex = 6;
  public static final byte OffsetIndex = 7;

  public static final int NONCE_LENGTH = 12;
  public static final int GCM_TAG_LENGTH = 16;
  public static final int SIZE_LENGTH = 4;
  
  static final int CTR_IV_LENGTH = 16;
  static final int CHUNK_LENGTH = 4 * 1024;
  static final int AAD_FILE_UNIQUE_LENGTH = 8;

  private EncryptionKey aesKey;
  private final SecureRandom randomGenerator;
  private final int tagLength;
  private Cipher aesCipher;
  private final Mode aesMode;
  private final byte[] ctrIV;
  private final byte[] localNonce;

  /**
   * 
   * @param mode GCM or CTR
   * @param keyBytes encryption key
   * @param allEncryptors 
   * @throws IllegalArgumentException
   * @throws IOException
   */
  public AesEncryptor(Mode mode, byte[] keyBytes, LinkedList<AesEncryptor> allEncryptors) throws IllegalArgumentException, IOException {
    if (null == keyBytes) {
      throw new IllegalArgumentException("Null key bytes");
    }
    aesKey = new EncryptionKey(keyBytes);
    
    randomGenerator = new SecureRandom();
    aesMode = mode;
    
    if (Mode.GCM == mode) {
      tagLength = GCM_TAG_LENGTH;
      try {
        aesCipher = Cipher.getInstance("AES/GCM/NoPadding");
      } catch (GeneralSecurityException e) {
        throw new IOException("Failed to create GCM cipher", e);
      }
      ctrIV = null;
    }
    else {
      tagLength = 0;
      try {
        aesCipher = Cipher.getInstance("AES/CTR/NoPadding");
      } catch (GeneralSecurityException e) {
        throw new IOException("Failed to create CTR cipher", e);
      }
      ctrIV = new byte[CTR_IV_LENGTH];
      Arrays.fill(ctrIV, (byte) 0);
      // Setting last bit of initial CTR counter to 1
      ctrIV[CTR_IV_LENGTH - 1] = (byte) 1;
    }
    
    localNonce = new byte[NONCE_LENGTH];
    if (null != allEncryptors) allEncryptors.add(this);
  }

  @Override
  public byte[] encrypt(byte[] plainText, byte[] AAD)  throws IOException {
    return encrypt(true, plainText, AAD);
  }
  
  /**
   * 
   * @param writeLength whether to write buffer length (4-byte little endian)
   * @param plainText
   * @param AAD
   * @return
   * @throws IOException
   */
  public byte[] encrypt(boolean writeLength, byte[] plainText, byte[] AAD)  throws IOException {
    randomGenerator.nextBytes(localNonce);
    return encrypt(writeLength, plainText, localNonce, AAD);
  }
  
  /**
   * 
   * @param writeLength
   * @param plainText
   * @param nonce can be generated locally or supplied. the latter is needed for footer signature verification.
   * @param AAD
   * @return
   * @throws IOException
   */
  public byte[] encrypt(boolean writeLength, byte[] plainText, byte[] nonce, byte[] AAD)  throws IOException {
    if (nonce.length != NONCE_LENGTH) throw new IOException("Wrong nonce length " + nonce.length);
    int plainTextLength = plainText.length;
    int cipherTextLength = NONCE_LENGTH + plainTextLength + tagLength;
    int lengthBufferLength = (writeLength? SIZE_LENGTH: 0);
    byte[] cipherText = new byte[lengthBufferLength + cipherTextLength];
    int inputLength = plainTextLength;
    int inputOffset = 0;
    int outputOffset = lengthBufferLength + NONCE_LENGTH;
    try {
      if (Mode.GCM == aesMode) {
        GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, nonce);
        aesCipher.init(Cipher.ENCRYPT_MODE, aesKey, spec);
        if (null != AAD) aesCipher.updateAAD(AAD);
      }
      else {
        System.arraycopy(nonce, 0, ctrIV, 0, NONCE_LENGTH);
        IvParameterSpec spec = new IvParameterSpec(ctrIV);
        aesCipher.init(Cipher.ENCRYPT_MODE, aesKey, spec);
      }
      // Breaking encryption into multiple updates, to trigger h/w acceleration in Java 9-11
      while (inputLength > CHUNK_LENGTH) {
        int written = aesCipher.update(plainText, inputOffset, CHUNK_LENGTH, cipherText, outputOffset);
        inputOffset += CHUNK_LENGTH;
        outputOffset += written;
        inputLength -= CHUNK_LENGTH;
      }
      aesCipher.doFinal(plainText, inputOffset, inputLength, cipherText, outputOffset);
    }
    catch (GeneralSecurityException e) {
      throw new IOException("Failed to encrypt", e);
    }
    // Add ciphertext length
    if (writeLength) System.arraycopy(BytesUtils.intToBytes(cipherTextLength), 0, cipherText, 0, lengthBufferLength);
    // Add the nonce
    System.arraycopy(nonce, 0, cipherText, lengthBufferLength, NONCE_LENGTH);

    return cipherText;
  }
  
  public static byte[] createModuleAAD(byte[] fileAAD, byte moduleType, 
      short rowGroupOrdinal, short columnOrdinal, short pageOrdinal) {
    byte[] typeOrdinalBytes = new byte[1];
    typeOrdinalBytes[0] = moduleType;
    if (Footer == moduleType) {
      return concatByteArrays(fileAAD, typeOrdinalBytes);      
    }
    
    byte[] rowGroupOrdinalBytes = shortToBytesLE(rowGroupOrdinal);
    byte[] columnOrdinalBytes = shortToBytesLE(columnOrdinal);
    if (DataPage != moduleType && DataPageHeader != moduleType) {
      return concatByteArrays(fileAAD, typeOrdinalBytes, rowGroupOrdinalBytes, columnOrdinalBytes); 
    }
    
    byte[] pageOrdinalBytes = shortToBytesLE(pageOrdinal);
    return concatByteArrays(fileAAD, typeOrdinalBytes, rowGroupOrdinalBytes, columnOrdinalBytes, pageOrdinalBytes);
  }
  
  public static byte[] createFooterAAD(byte[] aadPrefixBytes) {
    return createModuleAAD(aadPrefixBytes, Footer, (short) -1, (short) -1, (short) -1);
  }
  
  /**
   * Update last two bytes with new page ordinal (instead of creating new page AAD from scratch)
   * @param pageAAD
   * @param newPageOrdinal
   */
  public static void quickUpdatePageAAD(byte[] pageAAD, short newPageOrdinal) {
    byte[] pageOrdinalBytes = shortToBytesLE(newPageOrdinal);
    int length = pageAAD.length;
    System.arraycopy(pageOrdinalBytes, 0, pageAAD, length-2, 2);
  }

  
  static byte[] concatByteArrays(byte[]... arrays) {
    int totalLength = 0;
    for (byte[] array : arrays) {
      totalLength += array.length;
    }
    byte[] output = new byte[totalLength];
    int offset = 0;
    for (byte[] array : arrays) {
      int arrayLength = array.length;
      System.arraycopy(array, 0, output, offset, arrayLength);
      offset += arrayLength;
    }
    return output;
  }
  
  private static byte[] shortToBytesLE(short input) {
    byte[] output  = new byte[2];
    output[1] = (byte)(0xff & (input >> 8));
    output[0] = (byte)(0xff & (input));
    return output;
  }

  public void wipeOut() {
    
    try {
      aesKey.destroy();
    } catch (DestroyFailedException e) {
      throw new ShouldNeverHappenException(e);
    }
    aesCipher = null; // dereference for GC
  }
}

