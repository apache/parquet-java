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
import org.apache.parquet.format.ColumnCryptoMetaData;
import org.apache.parquet.format.EncryptionWithColumnKey;
import org.apache.parquet.format.EncryptionWithFooterKey;

public class ColumnMetadata {
  
  private boolean encrypt;
  private boolean isEncryptedWithFooterKey;
  private String[] columnPath;
  private byte[] keyBytes;
  private byte[] keyMetaData;
  private ColumnCryptoMetaData ccmd;
  private boolean processed ;
  private ColumnEncryptors encryptors;
  
  /**
   * Convenience constructor for regular (not nested) columns.
   * @param encrypt
   * @param name
   */
  public ColumnMetadata(boolean encrypt, String name) {
    this(encrypt, new String[] {name});
  }
  
  public ColumnMetadata(boolean encrypt, String[] path) {
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

  public boolean isEncrypted() {
    processed = true;
    return encrypt;
  }
  
  ColumnCryptoMetaData getColumnCryptoMetaData() {
    processed = true;
    if (null != ccmd) return ccmd;
    if (isEncryptedWithFooterKey) {
      ccmd = ColumnCryptoMetaData.ENCRYPTION_WITH_FOOTER_KEY(new EncryptionWithFooterKey());
    }
    else {
      EncryptionWithColumnKey eck = new EncryptionWithColumnKey(Arrays.asList(columnPath));
      if (null != keyMetaData) {
        eck.setColumn_key_metadata(keyMetaData);
      }
      ccmd =  ColumnCryptoMetaData.ENCRYPTION_WITH_COLUMN_KEY(eck);
    }
    return ccmd;
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

  void setEncryptors(ColumnEncryptors encryptors) {
    this.encryptors = encryptors;
  }

  ColumnEncryptors getEncryptors() {
    return encryptors;
  }
}
