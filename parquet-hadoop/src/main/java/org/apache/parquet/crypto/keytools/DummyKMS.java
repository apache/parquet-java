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


package org.apache.parquet.crypto.keytools;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.KeyAccessDeniedException;


public class DummyKMS implements KmsClient {
  
  HashMap<String,String> dataEncryptionKeyMap = null;

  @Override
  public void initialize(Configuration conf) throws IOException {
    String encryptionKeys[] = conf.getTrimmedStrings("encryption.key.list");
    if (null == encryptionKeys || encryptionKeys.length == 0) {
      throw new IOException("No encryption key list");
    }
    dataEncryptionKeyMap = new HashMap<String,String>();
    int nKeys = encryptionKeys.length;
    for (int i=0; i < nKeys; i++) {
      String[] parts = encryptionKeys[i].split(":");
      String keyName = parts[0].trim();
      String key = parts[1].trim();
      //TODO check parts
      dataEncryptionKeyMap.put(keyName, key);
    }
  }

  @Override
  public boolean supportsServerSideWrapping() {
    return false;
  }

  @Override
  public String getKeyFromServer(String keyIdentifier)
      throws UnsupportedOperationException, KeyAccessDeniedException, IOException {
    String key = dataEncryptionKeyMap.get(keyIdentifier);
    if (null == key) {
      throw new IOException("Key not found: " + keyIdentifier);
    }
    return key;
  }

  @Override
  public String wrapDataKeyInServer(String dataKey, String masterKeyIdentifier)
      throws UnsupportedOperationException, KeyAccessDeniedException, IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String unwrapDataKeyInServer(String wrappedDataKey, String masterKeyIdentifier)
      throws UnsupportedOperationException, KeyAccessDeniedException, IOException {
    throw new UnsupportedOperationException();
  }
}