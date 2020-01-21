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

import org.apache.parquet.hadoop.metadata.ColumnPath;

/**
 * This class is only required for setting explicit column decryption keys -
 * to override key retriever (or to provide keys when key metadata and/or
 * key retriever are not available)
 */
public class ColumnDecryptionProperties {

  private final ColumnPath columnPath;
  private final byte[] keyBytes;

  private boolean utilized;

  private ColumnDecryptionProperties(ColumnPath columnPath, byte[] keyBytes) {
    if (null == columnPath) {
      throw new IllegalArgumentException("Null column path");
    }
    if (null == keyBytes) {
      throw new IllegalArgumentException("Null key for column " + columnPath);
    }
    if (!(keyBytes.length == 16 || keyBytes.length == 24 || keyBytes.length == 32)) {
      throw new IllegalArgumentException("Wrong key length: " + keyBytes.length + 
          " on column: " + columnPath);
    }

    this.columnPath = columnPath;
    this.keyBytes = keyBytes;
    this.utilized = false;
  }

  /**
   * Convenience builder for regular (not nested) columns.
   */
  public static Builder builder(String columnName) {
    return builder(ColumnPath.get(columnName));
  }

  public static Builder builder(ColumnPath columnPath) {
    return new Builder(columnPath);
  }

  public static class Builder {
    private final ColumnPath columnPath;
    private byte[] keyBytes;

    private Builder(ColumnPath path) {
      this.columnPath = path;
    }

    /**
     * Set an explicit column key. 
     * If applied on a file that contains key metadata for this column - 
     * the metadata will be ignored, the column will be decrypted with this key.
     * However, if the column was encrypted with the footer key, it will also be decrypted with the
     * footer key, and the column key passed in this method will be ignored.
     * 
     * The key is cloned, and will be wiped out (array values set to 0) upon completion of file reading.
     * Caller is responsible for wiping out the input key array. 
     * 
     * @param columnKey Key length must be either 16, 24 or 32 bytes.
     * @return Builder
     */
    public Builder withKey(byte[] columnKey) {
      if (null != this.keyBytes) {
        throw new IllegalArgumentException("Key already set on column: " + columnPath);
      }
      this.keyBytes = new byte[columnKey.length];
      System.arraycopy(columnKey, 0, this.keyBytes, 0, columnKey.length);
      return this;
    }

    public ColumnDecryptionProperties build() {
      return new ColumnDecryptionProperties(columnPath, keyBytes);
    }
  }

  public ColumnPath getPath() {
    return columnPath;
  }

  public byte[] getKeyBytes() {
    return keyBytes;
  }

  boolean isUtilized() {
    return utilized;
  }

  void setUtilized() {
    utilized = true;
  }

  void wipeOutDecryptionKey() {
    Arrays.fill(keyBytes, (byte)0);
  }

  ColumnDecryptionProperties deepClone() {
    byte[] columnKeyBytes = keyBytes.clone();
    return new ColumnDecryptionProperties(columnPath, columnKeyBytes);
  }
}
