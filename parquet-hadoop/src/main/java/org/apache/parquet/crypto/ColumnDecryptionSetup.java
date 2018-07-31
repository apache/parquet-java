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

import org.apache.parquet.crypto.ParquetFileDecryptor.ColumnDecryptors;

public class ColumnDecryptionSetup {
  
  private final boolean isEncrypted;
  private final String[] columnPath;
  
  private boolean isEncryptedWithFooterKey;
  private byte[] keyMetaData;
  private byte[] keyBytes;
  private ColumnDecryptors decryptors;

  
  ColumnDecryptionSetup(boolean encrypted, String[] path) {
    this.isEncrypted = encrypted;
    this.columnPath = path;
  }

  String[] getPath() {
    return columnPath;
  }

  void setKeyMetadata(byte[] columnKeyMetadata) {
    keyMetaData = columnKeyMetadata;
  }

  void setEncryptedWithFooterKey(boolean encrFooterKey) {
    isEncryptedWithFooterKey = encrFooterKey;
  }

  boolean isEncrypted() {
    return isEncrypted;
  }

  boolean isEncryptedWithFooterKey() {
    return isEncryptedWithFooterKey;
  }

  byte[] getColumnKeyMetadata() {
    return keyMetaData;
  }

  void setEncryptionKey(byte[] decryptionKey) {
    keyBytes = decryptionKey;
  }

  byte[] getKeyBytes() {
    return keyBytes;
  }

  ColumnDecryptors getDecryptors() {
    return decryptors;
  }

  void setDecryptors(ColumnDecryptors decryptors) {
    this.decryptors = decryptors;
  }
}
