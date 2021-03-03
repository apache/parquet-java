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
package org.apache.parquet.hadoop;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ErrorCollector;

import java.io.IOException;

/*
 * This file continues the testing in TestEncryptionOptions. This test goals:
 * 4) Perform interoperability tests with other (eg parquet-cpp) writers, by reading
 *    encrypted files produced by these writers.
 *
 * For a full description and the actual implementation see TestEncryptionOptions.
 */
public class ITTestEncryptionOptions {
  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  TestEncryptionOptions test = new TestEncryptionOptions();

  @Test
  public void testInteropReadEncryptedParquetFiles() throws IOException {
    test.testInteropReadEncryptedParquetFiles(errorCollector);
  }

}
