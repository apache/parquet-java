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

import org.apache.hadoop.conf.Configuration;

/** Factory for creating {@link KmsClient} instances with programmatically supplied dependencies. */
@FunctionalInterface
public interface KmsClientFactory {

  /**
   * Creates a new KMS client. {@link KeyToolkit} invokes this method for each uncached combination
   * of access token and KMS instance ID, then initializes the returned client before using it.
   *
   * <p>Each invocation must return a distinct, uninitialized client.
   *
   * @param configuration current Hadoop configuration
   * @param kmsInstanceID ID of the KMS instance
   * @param kmsInstanceURL URL of the KMS instance
   * @param accessToken KMS access token
   * @return a new KMS client
   */
  KmsClient createKmsClient(
      Configuration configuration, String kmsInstanceID, String kmsInstanceURL, String accessToken);
}
