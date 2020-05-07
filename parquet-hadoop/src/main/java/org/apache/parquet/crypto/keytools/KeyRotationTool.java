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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.EncryptionPropertiesFactory;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.hadoop.util.HiddenFileFilter;

public class KeyRotationTool  {

  public static void rotateMasterKeys(String folderPath, Configuration hadoopConfig) throws IOException, ParquetCryptoRuntimeException, KeyAccessDeniedException {

    Path parentPath = new Path(folderPath);

    FileSystem hadoopFileSystem = parentPath.getFileSystem(hadoopConfig);


    FileStatus[] keyMaterialFiles = hadoopFileSystem.listStatus(parentPath, HiddenFileFilter.INSTANCE);

    for (FileStatus fs : keyMaterialFiles) {
      Path parquetFile = fs.getPath();

      FileKeyMaterialStore sourceKeyMaterialStore = new HadoopFSKeyMaterialStore(hadoopFileSystem, parquetFile);

      EnvelopeKeyManager keyManager = new EnvelopeKeyManager(hadoopConfig, sourceKeyMaterialStore);
      
      FileKeyMaterialStore tempKeyMaterialStore = new HadoopFSKeyMaterialStore(hadoopFileSystem, parquetFile, "_TMP"); // TODO
      
      keyManager.rotateMasterKeys(tempKeyMaterialStore);
    }
  }
}
