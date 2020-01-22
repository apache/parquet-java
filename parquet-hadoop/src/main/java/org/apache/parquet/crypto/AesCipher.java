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
import javax.security.auth.DestroyFailedException;

import org.apache.parquet.ShouldNeverHappenException;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;

import java.io.IOException;
import java.security.SecureRandom;


public class AesCipher {

  public static final int NONCE_LENGTH = 12;
  public static final int GCM_TAG_LENGTH = 16;

  static final int AAD_FILE_UNIQUE_LENGTH = 8;

  protected static final int CTR_IV_LENGTH = 16;
  protected static final int GCM_TAG_LENGTH_BITS = 8 * GCM_TAG_LENGTH;
  protected static final int CHUNK_LENGTH = 4 * 1024;
  protected static final int SIZE_LENGTH = ModuleCipherFactory.SIZE_LENGTH;

  protected EncryptionKey aesKey;
  protected final SecureRandom randomGenerator;
  protected Cipher cipher;
  protected final byte[] localNonce;
  protected boolean wipedOut;


  AesCipher(AesMode mode, byte[] keyBytes) throws IllegalArgumentException, IOException {
    if (null == keyBytes) {
      throw new IllegalArgumentException("Null key bytes");
    }
    
    boolean allZeroKey = true;
    for (byte kb : keyBytes) {
      if (kb != 0) {
        allZeroKey = false;
        break;
      }
    }
    
    if (allZeroKey) {
      throw new IllegalArgumentException("All key bytes are zero");
    }

    aesKey = new EncryptionKey(keyBytes);
    randomGenerator = new SecureRandom();
    localNonce = new byte[NONCE_LENGTH];
    wipedOut = false;
  }

  public static byte[] createModuleAAD(byte[] fileAAD, ModuleType moduleType, 
      short rowGroupOrdinal, short columnOrdinal, short pageOrdinal) {
    byte[] typeOrdinalBytes = new byte[1];
    typeOrdinalBytes[0] = moduleType.getValue();
    if (ModuleType.Footer == moduleType) {
      return concatByteArrays(fileAAD, typeOrdinalBytes);      
    }

    byte[] rowGroupOrdinalBytes = shortToBytesLE(rowGroupOrdinal);
    byte[] columnOrdinalBytes = shortToBytesLE(columnOrdinal);
    if (ModuleType.DataPage != moduleType && ModuleType.DataPageHeader != moduleType) {
      return concatByteArrays(fileAAD, typeOrdinalBytes, rowGroupOrdinalBytes, columnOrdinalBytes); 
    }

    byte[] pageOrdinalBytes = shortToBytesLE(pageOrdinal);
    return concatByteArrays(fileAAD, typeOrdinalBytes, rowGroupOrdinalBytes, columnOrdinalBytes, pageOrdinalBytes);
  }

  public static byte[] createFooterAAD(byte[] aadPrefixBytes) {
    return createModuleAAD(aadPrefixBytes, ModuleType.Footer, (short) -1, (short) -1, (short) -1);
  }

  // Update last two bytes with new page ordinal (instead of creating new page AAD from scratch)
  public static void quickUpdatePageAAD(byte[] pageAAD, short newPageOrdinal) {
    byte[] pageOrdinalBytes = shortToBytesLE(newPageOrdinal);
    System.arraycopy(pageOrdinalBytes, 0, pageAAD, pageAAD.length - 2, 2);
  }

  static byte[] concatByteArrays(byte[]... arrays) {
    int totalLength = 0;
    for (byte[] array : arrays) {
      totalLength += array.length;
    }
    byte[] output = new byte[totalLength];
    int offset = 0;
    for (byte[] array : arrays) {
      System.arraycopy(array, 0, output, offset, array.length);
      offset += array.length;
    }
    return output;
  }

  private static byte[] shortToBytesLE(short input) {
    byte[] output  = new byte[2];
    output[1] = (byte)(0xff & (input >> 8));
    output[0] = (byte)(0xff & input);
    return output;
  }

  public void wipeOut() {
    try {
      aesKey.destroy();
    } catch (DestroyFailedException e) {
      throw new ShouldNeverHappenException(e);
    }
    wipedOut = true;
    cipher = null; // dereference for GC
  }
}

