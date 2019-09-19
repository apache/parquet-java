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

import java.security.spec.KeySpec;
import java.util.Arrays;

import javax.crypto.SecretKey;
import javax.security.auth.DestroyFailedException;

// Standard Java encryption key classes' destroy method is not implemented 
// (known gap - currently left for users to fill)
// https://bugs.openjdk.java.net/browse/JDK-8160206
public class EncryptionKey implements SecretKey, KeySpec {

  private static final long serialVersionUID = -6356998586122608817L;

  private final byte[] keyBytes;
  private boolean destroyed;

  EncryptionKey(byte[] key) {
    keyBytes = new byte[key.length];
    System.arraycopy(key, 0, keyBytes, 0, key.length);
    destroyed = false;
  }


  @Override
  public String getAlgorithm() {
    return "AES";
  }

  @Override
  public byte[] getEncoded() {
    return keyBytes;
  }

  @Override
  public String getFormat() {
    return "RAW";
  }

  @Override
  public void destroy() throws DestroyFailedException {
    Arrays.fill(keyBytes, (byte)0);
    destroyed = true;
  }

  @Override
  public boolean isDestroyed() {
    return destroyed;
  }

  public boolean equals(Object o) {
    byte[] keyBytes2 = ((EncryptionKey) o).getEncoded();
    if (keyBytes.length != keyBytes2.length) return false;
    for (int i = 0; i < keyBytes.length; i++) {
      if (keyBytes[i] != keyBytes2[i])
        return false;
    }
    return true;
  }

  public int hashCode() {
    int code = 0;
    for (int i = 0; i < keyBytes.length; i++) {
      code ^= (keyBytes[i] & 0xff) << (i << 3 & 31);
    }
    return code;
  }
}
