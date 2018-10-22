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

package org.apache.parquet.format;

import java.io.IOException;

public interface BlockCipher{

  public interface Encryptor{
    /**
     * Encrypts the plaintext.
     * Make sure the returned contents starts at offset 0 and fills up the byte array.
     * Input plaintext starts at offset 0, and has a length of plaintext.length.
     * @param plaintext
     * @return ciphertext
     * @throws IOException
     */
    public byte[] encrypt(byte[] plaintext) throws IOException;

    /**
     * Encrypts the plaintext.
     * Make sure the returned contents starts at offset 0 and fills up the byte array.
     * Input plaintext starts at offset, and has a length of len.
     * @param plaintext
     * @param offset
     * @param len
     * @return ciphertext
     * @throws IOException
     */
    public byte[] encrypt(byte[] plaintext, int offset, int len) throws IOException;
  }


  public interface Decryptor{  
    /**
     * Decrypts the ciphertext. 
     * Make sure the returned plaintext starts at offset 0 and and fills up the byte array.
     * Input ciphertext starts at offset 0, and has a length of ciphertext.length.
     * @param ciphertext
     * @return plaintext
     * @throws IOException
     */
    public byte[] decrypt(byte[] ciphertext) throws IOException;

    /**
     * Decrypts the ciphertext. 
     * Make sure the returned plaintext starts at offset 0 and and fills up the byte array.
     * Input ciphertext starts at offset, and has a length of len.
     * @param ciphertext
     * @param offset
     * @param len
     * @return plaintext
     * @throws IOException
     */
    public byte[] decrypt(byte[] ciphertext, int offset, int len) throws IOException;
  }
}


