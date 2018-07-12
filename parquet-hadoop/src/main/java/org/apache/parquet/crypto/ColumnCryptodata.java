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
import java.util.Arrays;

import org.apache.parquet.bytes.BytesUtils;

public class ColumnCryptodata {
  
  private final boolean encrypt;
  private final String[] columnPath;
  
  private boolean isEncryptedWithFooterKey;
  private byte[] keyBytes;
  private byte[] keyMetaData;
  private boolean processed ;
  
  /**
   * Convenience constructor for regular (not nested) columns.
   * @param encrypt
   * @param name
   */
  public ColumnCryptodata(boolean encrypt, String name) {
    this(encrypt, new String[] {name});
  }
  
  public ColumnCryptodata(boolean encrypt, String[] path) {
    this.encrypt = encrypt;
    this.columnPath = path;
    isEncryptedWithFooterKey = encrypt;
    processed = false;
  }
  
  public void setEncryptionKey(byte[] keyBytes, byte[] keyMetaData) throws IOException {
    if (processed) throw new IOException("Metadata already processed");
    if (!encrypt) throw new IOException("Setting key on unencrypted column: " + Arrays.toString(columnPath));
    if (null == keyBytes) throw new IOException("Null key for " + Arrays.toString(columnPath));
    //TODO compare to footer key?
    isEncryptedWithFooterKey = false;
    this.keyBytes = keyBytes;
    this.keyMetaData = keyMetaData;
  }
  
  public void setEncryptionKey(byte[] keyBytes, int keyIdMetaData) throws IOException {
    byte[] metaData = BytesUtils.intToBytes(keyIdMetaData);
    setEncryptionKey(keyBytes, metaData);
  }

  String[] getPath() {
    processed = true;
    return columnPath;
  }

  boolean isEncrypted() {
    processed = true;
    return encrypt;
  }

  byte[] getKeyBytes() {
    processed = true;
    return keyBytes;
  }

  boolean isEncryptedWithFooterKey() {
    processed = true;
    if (!encrypt) return false;
    return isEncryptedWithFooterKey;
  }

  byte[] getKeyMetaData() {
    return keyMetaData;
  }
}
