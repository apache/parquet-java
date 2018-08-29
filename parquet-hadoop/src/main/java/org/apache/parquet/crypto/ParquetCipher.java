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

import java.util.Locale;

import org.apache.parquet.format.AesGcmV1;
import org.apache.parquet.format.AesGcmCtrV1;
import org.apache.parquet.format.EncryptionAlgorithm;


public enum ParquetCipher {

  AES_GCM_V1(0),
  AES_GCM_CTR_V1(1);

  public static ParquetCipher fromConf(String name) {
    if (name == null) {
      return AES_GCM_V1;
    }
    return valueOf(name.toUpperCase(Locale.ENGLISH));
 }

  public EncryptionAlgorithm getEncryptionAlgorithm() {
    if (0 == algorithmID) {
      return EncryptionAlgorithm.AES_GCM_V1(new AesGcmV1());
    }
    else if (1 == algorithmID) {
      return EncryptionAlgorithm.AES_GCM_CTR_V1(new AesGcmCtrV1());
    }
    else {
      return null;
    }
  }

  private int algorithmID;

  private ParquetCipher(int algorithmID) {
    this.algorithmID = algorithmID;
  }
}
