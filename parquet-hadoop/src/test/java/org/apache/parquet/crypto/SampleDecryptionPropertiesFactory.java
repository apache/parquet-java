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
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ColumnPath;

public class SampleDecryptionPropertiesFactory implements DecryptionPropertiesFactory {

  public static final byte[] FOOTER_KEY = {
    0x01, 0x02, 0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10
  };
  public static final ColumnPath COL1 = ColumnPath.fromDotString("col_1");
  public static final byte[] COL1_KEY = {
    0x02, 0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11
  };
  public static final ColumnDecryptionProperties COL1_ENCR_PROPERTIES = ColumnDecryptionProperties.builder(
          COL1.toDotString())
      .withKey(COL1_KEY)
      .build();
  public static final ColumnPath COL2 = ColumnPath.fromDotString("col_2");
  public static final byte[] COL2_KEY = {
    0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12
  };
  public static final ColumnDecryptionProperties COL2_ENCR_PROPERTIES = ColumnDecryptionProperties.builder(
          COL2.toDotString())
      .withKey(COL2_KEY)
      .build();

  @Override
  public FileDecryptionProperties getFileDecryptionProperties(Configuration hadoopConfig, Path filePath)
      throws ParquetCryptoRuntimeException {

    Map<ColumnPath, ColumnDecryptionProperties> columnDecPropertiesMap = new HashMap<>();

    columnDecPropertiesMap.put(COL1, COL1_ENCR_PROPERTIES);
    columnDecPropertiesMap.put(COL2, COL2_ENCR_PROPERTIES);

    FileDecryptionProperties.Builder fileDecBuilder = FileDecryptionProperties.builder();

    return fileDecBuilder
        .withFooterKey(FOOTER_KEY)
        .withColumnKeys(columnDecPropertiesMap)
        .build();
  }
}
