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

package org.apache.parquet.crypto.propertiesfactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.DecryptionKeyRetrieverMock;
import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.EncryptionPropertiesFactory;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.ExtType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaCryptoPropertiesFactory implements EncryptionPropertiesFactory, DecryptionPropertiesFactory {

  private static Logger log = LoggerFactory.getLogger(SchemaCryptoPropertiesFactory.class);

  public static final String CONF_ENCRYPTION_ALGORITHM = "parquet.encryption.algorithm";
  public static final String CONF_ENCRYPTION_FOOTER = "parquet.encrypt.footer";
  private static final byte[] FOOTER_KEY = {0x01, 0x02, 0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
    0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10};
  private static final byte[] FOOTER_KEY_METADATA = "footkey".getBytes(StandardCharsets.UTF_8);
  private static final byte[] COL_KEY = {0x02, 0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
    0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11};
  private static final byte[] COL_KEY_METADATA = "col".getBytes(StandardCharsets.UTF_8);

  @Override
  public FileEncryptionProperties getFileEncryptionProperties(Configuration fileHadoopConfig, Path tempFilePath,
                                                              WriteContext fileWriteContext) throws ParquetCryptoRuntimeException {
    MessageType schema = fileWriteContext.getSchema();
    List<Type> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      throw new ParquetCryptoRuntimeException("Null or empty fields is found");
    }

    Map<ColumnPath, ColumnEncryptionProperties> columnPropertyMap = new HashMap<>();
    List<String> currentPath = new ArrayList<>();

    for (Type field : fields) {
      getColumnEncryptionProperties(field, columnPropertyMap, currentPath);
    }

    if (columnPropertyMap.size() == 0) {
      log.debug("No column is encrypted. Returning null so that Parquet can skip. Empty properties will cause Parquet exception");
      return null;
    }

    /**
     * Why we still need footerKeyMetadata even withEncryptedFooter as false? According to the
     * 'Plaintext Footer' section of
     * https://github.com/apache/parquet-format/blob/encryption/Encryption.md, the plaintext footer
     * is signed in order to prevent tampering with the FileMetaData contents. So footerKeyMetadata
     * is always needed. This signature will be verified if parquet-mr code is with parquet-1178.
     * Otherwise, it will be ignored.
     */
    boolean shouldEncryptFooter = getEncryptFooter(fileHadoopConfig);
    FileEncryptionProperties.Builder encryptionPropertiesBuilder =
      FileEncryptionProperties.builder(FOOTER_KEY)
        .withFooterKeyMetadata(FOOTER_KEY_METADATA)
        .withAlgorithm(getParquetCipherOrDefault(fileHadoopConfig))
        .withEncryptedColumns(columnPropertyMap);
    if (!shouldEncryptFooter) {
      encryptionPropertiesBuilder = encryptionPropertiesBuilder.withPlaintextFooter();
    }
    FileEncryptionProperties encryptionProperties = encryptionPropertiesBuilder.build();
    log.info(
      "FileEncryptionProperties is built with, algorithm:{}, footerEncrypted:{}",
      encryptionProperties.getAlgorithm(),
      encryptionProperties.encryptedFooter());
    return encryptionProperties;
  }

  private ParquetCipher getParquetCipherOrDefault(Configuration conf) {
    String algorithm = conf.get(CONF_ENCRYPTION_ALGORITHM, "AES_GCM_CTR_V1");
    log.debug("Encryption algorithm is {}", algorithm);
    return ParquetCipher.valueOf(algorithm.toUpperCase());
  }

  private boolean getEncryptFooter(Configuration conf) {
    boolean encryptFooter = conf.getBoolean(CONF_ENCRYPTION_FOOTER, false);
    log.debug("Encrypt Footer: {}", encryptFooter);
    return encryptFooter;
  }

  private void getColumnEncryptionProperties(Type field, Map<ColumnPath, ColumnEncryptionProperties> columnPropertyMap,
                                             List<String> currentPath) throws ParquetCryptoRuntimeException {
    String pathName = field.getName();
    currentPath.add(pathName);
    if (field.isPrimitive()) {
      if (field instanceof ExtType) {
        log.debug("Leaf node {} is being checked crypto settings", field.getName());
        // leaf node
        Map<String, Object> metaData = ((ExtType<Object>) field).getMetadata();
        if (metaData != null && metaData.containsKey("encrypted")) {
          boolean encryptFlag;
          if ((metaData.get("encrypted") instanceof String)) {
            encryptFlag = Boolean.parseBoolean((String) metaData.get("encrypted"));
          } else {
            encryptFlag = (boolean) metaData.get("encrypted");
          }
          if (encryptFlag) {
            log.info("Field {} is to be in encrypted mode", field.getName());
            ColumnPath path = ColumnPath.get(currentPath.toArray(new String[0]));
            ColumnEncryptionProperties colEncProp =
              ColumnEncryptionProperties.builder(path)
                .withKey(COL_KEY)
                .withKeyMetaData(COL_KEY_METADATA)
                .build();
            columnPropertyMap.put(path, colEncProp);
          }
        }
      }
    } else {
      // intermediate node containing child(ren)
      List<Type> fields = field.asGroupType().getFields();
      for (Type childField : fields) {
        getColumnEncryptionProperties(childField, columnPropertyMap, currentPath);
      }
    }
    currentPath.remove(currentPath.size() - 1);
  }

  @Override
  public FileDecryptionProperties getFileDecryptionProperties(Configuration hadoopConfig, Path filePath)
    throws ParquetCryptoRuntimeException {
    DecryptionKeyRetrieverMock keyRetriever = new DecryptionKeyRetrieverMock();
    keyRetriever.putKey("footkey", FOOTER_KEY);
    keyRetriever.putKey("col", COL_KEY);
    return FileDecryptionProperties.builder().withPlaintextFilesAllowed().withKeyRetriever(keyRetriever).build();
  }
}
