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
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;
import org.apache.parquet.hadoop.util.ConfigurationUtil;

import java.io.IOException;

import static org.apache.parquet.Preconditions.checkNotNull;

public class CryptoClassLoader {

  public static final String CRYPTO_METADATA_RETRIEVER_CLASS  = "parquet.crypto.encryptor.decryptor.retriever.class";

  public static boolean metadataRetrieverExists(Configuration configuration) {
    return configuration.get(CRYPTO_METADATA_RETRIEVER_CLASS) != null;
  }

  /**
   * Calling user configured class to create encryptor
   * @param conf Hadoop configuration
   * @return
   * @throws IOException
   */
  public static InternalFileEncryptor getParquetFileEncryptorOrNull(Configuration conf, WriteContext writeContext) throws IOException {
    if (!metadataRetrieverExists(conf)) {
      return null;
    }

    FileEncDecryptorRetriever fileEncDecryptorRetriever = getFileEncDecryptorRetriever(conf);
    return fileEncDecryptorRetriever.getFileEncryptor(conf, writeContext);
  }

  /**
   * Calling user configured class to create encryption properties
   * @param conf Hadoop configuration
   * @return
   * @throws IOException
   */
  public static FileEncryptionProperties getFileEncryptionPropertiesOrNull(Configuration conf, WriteContext writeContext) throws IOException {
    if (!metadataRetrieverExists(conf)) {
      return null;
    }

    FileEncDecryptorRetriever fileEncDecryptorRetriever = getFileEncDecryptorRetriever(conf);
    return fileEncDecryptorRetriever.getFileEncryptionProperties(conf, writeContext);
  }

  /**
   * Calling user configured class to create decryptor
   * @param conf Hadoop configuration
   * @return
   * @throws IOException
   */
  public static InternalFileDecryptor getParquetFileDecryptorOrNull(Configuration conf) throws IOException {
    if (!metadataRetrieverExists(conf)) {
      return null;
    }

    FileEncDecryptorRetriever fileEncDecryptorRetriever = getFileEncDecryptorRetriever(conf);
    return fileEncDecryptorRetriever.getFileDecryptor(conf);
  }

  /**
   * Calling user configured class to create decryption properties
   * @param conf Hadoop configuration
   * @return
   * @throws IOException
   */
  public static FileDecryptionProperties getFileDecryptionPropertiesOrNull(Configuration conf) throws IOException {
    if (!metadataRetrieverExists(conf)) {
      return null;
    }

    FileEncDecryptorRetriever fileEncDecryptorRetriever = getFileEncDecryptorRetriever(conf);
    return fileEncDecryptorRetriever.getFileDecryptionProperties(conf);
  }

  /**
   * If CRYPTO_METADATA_RETRIEVER_CLASS exists in configuration, the relative class will be invoked.
   * The class should implement the interface of FileEncDecryptorRetriever.
   *
   * @param conf to find the configuration for the write support class
   * @return the configured crypto metadata retriever(FileEncDecryptorRetriever)
   */
  @SuppressWarnings("unchecked")
  private static FileEncDecryptorRetriever getFileEncDecryptorRetriever(Configuration conf){

    final Class<?> cryptoMetadataRetrieverClass = ConfigurationUtil.getClassFromConfig(conf,
                                                                                       CRYPTO_METADATA_RETRIEVER_CLASS,
                                                                                       FileEncDecryptorRetriever.class);
    try {
        return (FileEncDecryptorRetriever)checkNotNull(cryptoMetadataRetrieverClass,
                                              "cryptoMetadataRetrieverClass").newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
        throw new BadConfigurationException("could not instantiate crypto metadata retriever class: "
                                            + cryptoMetadataRetrieverClass, e);
    }
  }
}

