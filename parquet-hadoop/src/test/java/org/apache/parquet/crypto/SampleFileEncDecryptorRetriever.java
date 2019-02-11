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
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.ExtType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SampleFileEncDecryptorRetriever implements FileEncDecryptorRetriever {
  private static byte[] encKey = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

  public InternalFileEncryptor getFileEncryptor(Configuration conf, WriteSupport.WriteContext init) throws IOException {
    return new InternalFileEncryptor(getFileEncryptionProperties(conf, init));
  }

  public FileEncryptionProperties getFileEncryptionProperties(Configuration conf, WriteSupport.WriteContext init) throws IOException {
    MessageType schema = init.getSchema();
    List<Type> fields = schema.getFields();
    Map<ColumnPath, ColumnEncryptionProperties> columnPropertyMap = new HashMap<>();
    List<String> currentPath = new ArrayList<>();

    for (Type field : fields) {
      getColumnEncryptionProperties((ExtType<Object>)field, columnPropertyMap, currentPath);
    }

    byte[] footerKeyMetadata = DatatypeConverter.parseBase64Binary("AAA=");

    FileEncryptionProperties encryptionProperties = FileEncryptionProperties.builder(encKey)
      .withFooterKeyMetadata(footerKeyMetadata)
      .withAlgorithm(ParquetCipher.AES_GCM_V1)
      .withColumnProperties(columnPropertyMap, false)
      .withEncryptedFooter(false)
      .withoutAADPrefixStorage()
      .withAADPrefix(new byte[]{2, 3})
      .build();
    return encryptionProperties;
  }

  public InternalFileDecryptor getFileDecryptor(Configuration conf) throws IOException {
    return new InternalFileDecryptor(getFileDecryptionProperties(conf));
  }

  public FileDecryptionProperties getFileDecryptionProperties(Configuration conf) throws IOException {
    KMSClient kmsClient = new KMSClient();
    return FileDecryptionProperties.builder().withKeyRetriever(kmsClient).build();
  }

  private class KMSClient implements DecryptionKeyRetriever {
    public byte[] getKey(byte[] keyMetaData) throws KeyAccessDeniedException, IOException {
      return encKey;
    }
  }

  // TODO: it doesn't support nested columns yet
  private void getColumnEncryptionProperties(ExtType<Object> field,
                                             Map<ColumnPath, ColumnEncryptionProperties> columnPropertyMap,
                                             List<String> currentPath) throws IOException {
    String pathName = field.getName();
    currentPath.add(pathName);
    if (field.isPrimitive()) {
      //leaf node
      Map<String, Object> metaData = field.getMetadata();
        if (metaData != null && metaData.containsKey("encrypted") && (boolean)metaData.get("encrypted"))   {
        byte[] colKeyMetadata = DatatypeConverter.parseBase64Binary((String)metaData.get("columnKeyMetaData"));
        ColumnPath path = ColumnPath.get(currentPath.toArray(new String[0]));
        ColumnEncryptionProperties colEncProp = ColumnEncryptionProperties.builder(path, true)
          .withKey(encKey)
          .withKeyMetaData(colKeyMetadata)
          .build();
        columnPropertyMap.put(path, colEncProp);
      }
    } else {
      //intermediate node containing child(ren)
      List<Type> fields = field.asGroupType().getFields();
      for (Type childField : fields) {
        getColumnEncryptionProperties((ExtType<Object>)childField, columnPropertyMap, currentPath);
      }
    }
    currentPath.remove(currentPath.size() - 1);
  }
}
