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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple key retriever, based on UTF8 strings as key identifiers
 */
public class DecryptionKeyRetrieverMock implements DecryptionKeyRetriever {
  private final Map<String, byte[]> keyMap = new HashMap<>();

  public DecryptionKeyRetrieverMock putKey(String keyId, byte[] keyBytes) {
    keyMap.put(keyId, keyBytes);
    return this;
  }

  @Override
  public byte[] getKey(byte[] keyMetaData) {
    String keyId = new String(keyMetaData, StandardCharsets.UTF_8);
    return keyMap.get(keyId);
  }
}
