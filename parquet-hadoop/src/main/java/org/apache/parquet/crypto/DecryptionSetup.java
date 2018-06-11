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
import java.util.ArrayList;
import java.util.List;

public class DecryptionSetup {

  private byte[] footerKeyBytes;
  private byte[] aadBytes;
  private DecryptionKeyRetriever keyRetriever;
  private List<ColumnMetadata> columnKeyList;
  private boolean setupProcessed;

  /**
   * Configure a file decryptor with an explicit footer key. If applied on a file that contains footer key metadata - 
   * the metadata will be ignored, the footer will be decrypted with the provided explicit key.
   * @param keyBytes
   * @throws IOException 
   */
  public DecryptionSetup(byte[] footerKeyBytes) throws IOException {
    if (null == footerKeyBytes) throw new IOException("Decryption: null footer key");
    this.footerKeyBytes = footerKeyBytes;
    if (! (footerKeyBytes.length == 16 || footerKeyBytes.length == 24 || footerKeyBytes.length == 32)) {
      throw new IOException("Wrong key length " + footerKeyBytes.length);
    }
    setupProcessed = false;
  }

  /**
   * Configure a file decryptor with a key retriever callback. If applied on a file that doesn't contain key metadata - 
   * an exception will be thrown.
   * @param keyRetriever
   */
  public DecryptionSetup(DecryptionKeyRetriever keyRetriever) {
    this.keyRetriever = keyRetriever;
  }

  /**
   * Set the AES-GCM additional authenticated data (AAD).
   * 
   * @param aad
   * @throws IOException 
   */
  public void setAAD(byte[] aad) throws IOException {
    if (setupProcessed) throw new IOException("Setup already processed");
    // TODO if set, throw an exception? or allow to replace
    aadBytes = aad;
  }

  public void setColumnKey(String columnName, byte[] decryptionKey) throws IOException {
    setColumnKey(new String[] {columnName}, decryptionKey);
  }

  /**
   * Configure a column decryptor with an explicit column key. If applied on a file that contains key metadata for this column - 
   * the metadata will be ignored, the column will be decrypted with the provided explicit key.
   * @param 
   * @throws IOException 
   */
  public void setColumnKey(String[] columnPath, byte[] decryptionKey) throws IOException {
    if (setupProcessed) throw new IOException("Setup already processed");
    if (null == decryptionKey) throw new IOException("Decryption: null column key");
    if (! (decryptionKey.length == 16 || decryptionKey.length == 24 || decryptionKey.length == 32)) {
      throw new IOException("Wrong key length " + decryptionKey.length);
    }
    // TODO if set for this column, throw an exception? or allow to replace
    if (null == columnKeyList) columnKeyList = new ArrayList<ColumnMetadata>();
    ColumnMetadata cmd = new ColumnMetadata(true, columnPath);
    try {
      cmd.setEncryptionKey(decryptionKey, null);
    } catch (IOException e) {
      // Doesnt happen, since encr = true
    }
    columnKeyList.add(cmd);
  }

  byte[] getFooterKeyBytes() {
    setupProcessed = true;
    return footerKeyBytes;
  }

  DecryptionKeyRetriever getKeyRetriever() {
    setupProcessed = true;
    return keyRetriever;
  }

  byte[] getAAD() {
    setupProcessed = true;
    return aadBytes;
  }

  byte[] getColumnKey(String[] path) {
    setupProcessed = true;
    if (null == columnKeyList)  return null;
    for (ColumnMetadata col : columnKeyList) {
      if (col.getPath().length != path.length) continue;
      boolean equal = true;
      for (int i =0; i < col.getPath().length; i++) {
        if (!col.getPath()[i].equals(path[i])) {
          equal = false;
          break;
        }
      }
      if (equal) {
        return col.getKeyBytes();
      }
      else {
        continue;
      }
    } 
    return null;
  }
}
