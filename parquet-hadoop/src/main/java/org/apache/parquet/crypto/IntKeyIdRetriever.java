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

import java.util.HashMap;

import org.apache.parquet.bytes.BytesUtils;

// Simple key retriever, based on integer keyID
public class IntKeyIdRetriever implements KeyRetriever{

  private HashMap<Integer,byte[]> keyMap;
  
  public IntKeyIdRetriever() {
    keyMap = new HashMap<Integer,byte[]>();
  }
  
  public void putKey(int keyId, byte[] keyBytes) {
    keyMap.put(new Integer(keyId), keyBytes);
  }
  
  @Override
  public byte[] getKey(byte[] keyMetaData) {
    if (keyMetaData.length != 4) return null;
    int key_id = BytesUtils.bytesToInt(keyMetaData);
    return keyMap.get(new Integer(key_id));
  }
}
