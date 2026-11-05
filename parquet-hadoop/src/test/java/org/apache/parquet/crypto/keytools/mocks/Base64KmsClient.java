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

package org.apache.parquet.crypto.keytools.mocks;

import java.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.apache.parquet.crypto.keytools.KmsClient;

/**
 * A KMS client that "wraps" a key by base64 encoding it. It is not secure, it only exists to read
 * the fixtures written by {@code generate_arrow_fixtures.py}, whose key wrapping it mirrors.
 */
public class Base64KmsClient implements KmsClient {

  @Override
  public void initialize(Configuration configuration, String kmsInstanceID, String kmsInstanceURL, String accessToken)
      throws KeyAccessDeniedException {}

  @Override
  public String wrapKey(byte[] keyBytes, String masterKeyIdentifier) throws KeyAccessDeniedException {
    return Base64.getEncoder().encodeToString(keyBytes);
  }

  @Override
  public byte[] unwrapKey(String wrappedKey, String masterKeyIdentifier) throws KeyAccessDeniedException {
    return Base64.getDecoder().decode(wrappedKey);
  }
}
