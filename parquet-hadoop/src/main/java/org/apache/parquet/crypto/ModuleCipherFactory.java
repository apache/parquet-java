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



import org.apache.parquet.format.BlockCipher;

import java.io.IOException;

public class ModuleCipherFactory {


  // Parquet Module types
  public enum ModuleType {
    Footer((byte)0),
    ColumnMetaData((byte)1),
    DataPage((byte)2),
    DictionaryPage((byte)3),
    DataPageHeader((byte)4),
    DictionaryPageHeader((byte)5),
    ColumnIndex((byte)6),
    OffsetIndex((byte)7);

    private final byte value;

    private ModuleType(byte value) {
      this.value = value;
    }

    public byte getValue() {
      return value;
    }
  }

  public static final int SIZE_LENGTH = 4;


  public static BlockCipher.Encryptor getEncryptor(AesMode mode, byte[] keyBytes) 
      throws IllegalArgumentException, IOException {
    switch (mode) {
    case GCM:
      return new AesGcmEncryptor(keyBytes);
    case CTR:
      return new AesCtrEncryptor(keyBytes);
    default:
      throw new IllegalArgumentException("AesMode not supported in ModuleCipherFactory: " + mode);
    }
  }

  public static BlockCipher.Decryptor getDecryptor(AesMode mode, byte[] keyBytes) 
      throws IllegalArgumentException, IOException {
    switch (mode) {
    case GCM:
      return new AesGcmDecryptor(keyBytes);
    case CTR:
      return new AesCtrDecryptor(keyBytes);
    default:
      throw new IllegalArgumentException("AesMode not supported in ModuleCipherFactory: " + mode);
    }
  }
}
