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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.util.HashMap;
import java.util.Map;

public class SampleEncryptionPropertiesFactory implements EncryptionPropertiesFactory {

  public final static byte[] footerKey = {0x01, 0x02, 0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
    0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10};
  public final static ColumnPath col1 = ColumnPath.fromDotString("col_1");
  public final static byte[] col1Key = {0x02, 0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
    0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11};
  public final static ColumnEncryptionProperties col1EncrProperties = ColumnEncryptionProperties.builder(
    col1.toDotString()).withKey(col1Key).build();
  public final static ColumnPath col2 = ColumnPath.fromDotString("col_2");
  public final static byte[] col2Key = {0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
     0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12};
  public final static ColumnEncryptionProperties col2EncrProperties = ColumnEncryptionProperties.builder(
    col2.toDotString()).withKey(col2Key).build();

  @Override
  public FileEncryptionProperties getFileEncryptionProperties(Configuration fileHadoopConfig, Path tempFilePath,
                                                              WriteSupport.WriteContext fileWriteContext) throws ParquetCryptoRuntimeException {

    Map<ColumnPath, ColumnEncryptionProperties> columnEncPropertiesMap = new HashMap<>();

    columnEncPropertiesMap.put(col1, col1EncrProperties);
    columnEncPropertiesMap.put(col2, col2EncrProperties);

    FileEncryptionProperties.Builder fileEncBuilder = FileEncryptionProperties.builder(footerKey);

    return fileEncBuilder.withAlgorithm(ParquetCipher.AES_GCM_V1).withEncryptedColumns(columnEncPropertiesMap).build();
  }
}
