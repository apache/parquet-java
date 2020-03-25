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
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * CryptoPropertiesFactory interface enables transparent activation of Parquet encryption.
 *
 * It's customized implementations produce encryption and decryption properties for each Parquet file, using the information
 * available in Parquet file writers and readers: file path, file extended schema (in writers only) - and also Hadoop
 * configuration properties that can pass custom parameters required by a crypto factory. A factory implementation can
 * use or ignore any of these parameters.
 *
 * The example could be as below.
 *
 * 1. Write a class to implement CryptoPropertiesFactory.
 * 2. Set configuration of "parquet.encryption.factory.class" with the fully qualified name of this class.
 *    For example, we can set the configuration in SparkSession as below.
 *       SparkSession spark = SparkSession
 *                   .config("parquet.encryption.factory.class",
 *                    "xxx.xxx.CryptoPropertiesImpl")
 *
 *    This class will be invoked when the static method loadFactory() is called.
 */
public interface CryptoPropertiesFactory {

  Logger LOG = LoggerFactory.getLogger(CryptoPropertiesFactory.class);
  String CRYPTO_FACTORY_CLASS_PROPERTY_NAME = "parquet.encryption.factory.class";

  /**
   * Load CryptoPropertiesFactory class specified by CRYPTO_FACTORY_CLASS_PROPERTY_NAME as the path in the configuration
   *
   * @param conf Configuration where user specifies the class path
   * @return object with class CryptoPropertiesFactory if user specified the class path and invoking of
   * the class succeeds, null if user doesn't specify the class path
   * @throws BadConfigurationException if the instantiation of the configured class fails
   */
  static CryptoPropertiesFactory loadFactory(Configuration conf) {
    final Class<?> cryptoPropertiesFactoryClass = ConfigurationUtil.getClassFromConfig(conf,
      CRYPTO_FACTORY_CLASS_PROPERTY_NAME, CryptoPropertiesFactory.class);

    if (null == cryptoPropertiesFactoryClass) {
      LOG.debug("CryptoPropertiesFactory is not configured - name not found in hadoop config");
      return null;
    }

    try {
      CryptoPropertiesFactory cryptoFactory = (CryptoPropertiesFactory)cryptoPropertiesFactoryClass.newInstance();
      return cryptoFactory;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new BadConfigurationException("could not instantiate CryptoPropertiesFactory class: "
        + cryptoPropertiesFactoryClass, e);
    }
  }

  /**
   * Get FileEncryptionProperties object which is created by the implementation of this interface. Please see
   * the unit test (TBD) for example
   *
   * @param fileHadoopConfig Configuration that is used to pass the needed information, e.g. KMS uri
   * @param tempFilePath File path of the parquet file being written.
   *                     Can be used for AAD prefix creation, key material management, etc.
   *                     Implementations must not presume the path is permanent,
   *                     as the file can be moved or renamed later
   * @param fileWriteContext WriteContext to provide information like schema to build the FileEncryptionProperties
   * @return
   * @throws IOException
   */
  FileEncryptionProperties getFileEncryptionProperties(Configuration fileHadoopConfig, Path tempFilePath,
                                                       WriteContext fileWriteContext)  throws IOException;

  /**
   *
   * @param hadoopConfig Configuration that is used to pass the needed information, e.g. KMS uri
   * @param filePath File path of the parquet file
   *                 Can be used for AAD prefix verification, part of key metadata etc
   * @return
   * @throws IOException
   */
  FileDecryptionProperties getFileDecryptionProperties(Configuration hadoopConfig, Path filePath)  throws IOException;
}
