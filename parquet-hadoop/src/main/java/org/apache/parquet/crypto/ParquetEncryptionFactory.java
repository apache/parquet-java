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

import java.io.IOException;


public class ParquetEncryptionFactory {
    
  /**
   * File encryptor with a default setup: AES-GCM algorithm, uniform encryption (all columns will be encrypted), 
   * no key metadata, no AAD.
   * @param keyBytes
   * @return
   * @throws IOException
   */
  public static ParquetFileEncryptor createFileEncryptor(byte[] keyBytes) throws IOException {
    return createFileEncryptor(new EncryptionSetup(Cipher.AES_GCM_V1, keyBytes, null));
  }
  
  /**
   * File encryptor with custom setup.
   * @param eSetup
   * @return
   * @throws IOException
   */
  public static ParquetFileEncryptor createFileEncryptor(EncryptionSetup eSetup) throws IOException {
    return new ParquetFileEncryptor(eSetup);
  }
  
  /**
   * File decryptor without AAD and with an explicit key. If applied on a file that contains key metadata - 
   * the metadata will be ignored, the file will be decrypted with the provided key.
   * @param keyBytes
   * @return
   * @throws IOException
   */
  public static ParquetFileDecryptor createFileDecryptor(byte[] keyBytes) throws IOException {
    return createFileDecryptor(new DecryptionSetup(keyBytes));
  }
  
  /**
   * File decryptor with a custom setup.
   * @param keyRetriever
   * @return
   * @throws IOException
   */
  public static ParquetFileDecryptor createFileDecryptor(DecryptionSetup dSetup) throws IOException {
    return new ParquetFileDecryptor(dSetup);
  }
}
