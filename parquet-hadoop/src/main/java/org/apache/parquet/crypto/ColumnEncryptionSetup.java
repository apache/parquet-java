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


import java.util.Arrays;

import org.apache.parquet.crypto.ParquetFileEncryptor.ColumnEncryptors;
import org.apache.parquet.format.ColumnCryptoMetaData;
import org.apache.parquet.format.EncryptionWithColumnKey;
import org.apache.parquet.format.EncryptionWithFooterKey;

public class ColumnEncryptionSetup {
  
  private final ColumnEncryptionProperties cmd;
  
  private ColumnCryptoMetaData ccmd;
  private ColumnEncryptors encryptors;
  
  ColumnEncryptionSetup(boolean encrypt, String[] path) {
    this.cmd = new ColumnEncryptionProperties(encrypt, path);
  }
  
  ColumnEncryptionSetup(ColumnEncryptionProperties cmd) {
    this.cmd = cmd;
  }

  ColumnCryptoMetaData getColumnCryptoMetaData() {
    if (null != ccmd) return ccmd;
    if (cmd.isEncryptedWithFooterKey()) {
      ccmd = ColumnCryptoMetaData.ENCRYPTION_WITH_FOOTER_KEY(new EncryptionWithFooterKey());
    }
    else {
      EncryptionWithColumnKey eck = new EncryptionWithColumnKey(Arrays.asList(cmd.getPath()));
      if (null != cmd.getKeyMetaData()) {
        eck.setColumn_key_metadata(cmd.getKeyMetaData());
      }
      ccmd =  ColumnCryptoMetaData.ENCRYPTION_WITH_COLUMN_KEY(eck);
    }
    return ccmd;
  }

  void setEncryptors(ColumnEncryptors encryptors) {
    this.encryptors = encryptors;
  }

  ColumnEncryptors getEncryptors() {
    return encryptors;
  }

  String[] getPath() {
    return cmd.getPath();
  }

  public boolean isEncrypted() {
    return cmd.isEncrypted();
  }

  boolean isEncryptedWithFooterKey() {
    return cmd.isEncryptedWithFooterKey();
  }

  byte[] getKeyBytes() {
    return cmd.getKeyBytes();
  }
}
