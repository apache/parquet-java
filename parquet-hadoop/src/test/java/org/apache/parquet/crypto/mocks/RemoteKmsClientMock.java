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
package org.apache.parquet.crypto.mocks;


import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.apache.parquet.crypto.keytools.KeyToolkit;
import org.apache.parquet.crypto.keytools.RemoteKmsClient;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class RemoteKmsClientMock extends RemoteKmsClient {
  private static final byte[] FOOTER_MASTER_KEY = "0123456789012345".getBytes();
  private static final byte[] COLUMN_MASTER_KEY1 = "1234567890123450".getBytes();
  private static final byte[] COLUMN_MASTER_KEY2 = "1234567890123451".getBytes();
  private static final String FOOTER_MASTER_KEY_ID = "kf";
  private static final String COLUMN_MASTER_KEY1_ID = "kc1";
  private static final String COLUMN_MASTER_KEY2_ID = "kc2";
  final static byte[] AAD = FOOTER_MASTER_KEY_ID.getBytes(StandardCharsets.UTF_8);

  private Map<String, byte[]> keyMap;


  @Override
  protected void initializeInternal() throws KeyAccessDeniedException {
    keyMap = new HashMap<>(3);
    keyMap.put(FOOTER_MASTER_KEY_ID, FOOTER_MASTER_KEY);
    keyMap.put(COLUMN_MASTER_KEY1_ID, COLUMN_MASTER_KEY1);
    keyMap.put(COLUMN_MASTER_KEY2_ID, COLUMN_MASTER_KEY2);
  }

  @Override
  protected String wrapKeyInServer(byte[] keyBytes, String masterKeyIdentifier) throws KeyAccessDeniedException, UnsupportedOperationException {
    return KeyToolkit.wrapKeyLocally(keyBytes, keyMap.get(masterKeyIdentifier), AAD);
  }

  @Override
  protected byte[] unwrapKeyInServer(String wrappedKey, String masterKeyIdentifier) throws KeyAccessDeniedException, UnsupportedOperationException {
    return KeyToolkit.unwrapKeyLocally(wrappedKey, keyMap.get(masterKeyIdentifier), AAD);
  }

  @Override
  protected byte[] getMasterKeyFromServer(String masterKeyIdentifier) throws KeyAccessDeniedException, UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }
}
