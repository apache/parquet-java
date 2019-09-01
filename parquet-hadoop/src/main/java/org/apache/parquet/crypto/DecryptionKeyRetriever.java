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

public interface DecryptionKeyRetriever {

  /**
   * Must be thread-safe, if same KeyRetriever object is passed to multiple file readers.
   * Key length can be either 16, 24 or 32 bytes.
   * Key will be copied by Parquet for each file; the copy will be wiped out (filled with 0) when the file reader is closed.
   * The keys kept in the KeyRetriever object memory should wiped out (if possible) when this object is no longer needed.
   * If your key retrieval code throws runtime exceptions related to access/permission problems
   * (such as Hadoop AccessControlException), catch them and throw the KeyAccessDeniedException.
   * @param keyMetaData
   * @return
   * @throws KeyAccessDeniedException
   * @throws IOException
   */
  public byte[] getKey(byte[] keyMetaData) throws KeyAccessDeniedException, IOException;
}
