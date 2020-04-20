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
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DecryptionPropertiesFactory interface enables transparent activation of Parquet decryption.
 *
 * It's customized implementations produce decryption properties for each Parquet file, using the input information
 * available in Parquet file readers: file path and Hadoop configuration properties that can pass custom parameters
 * required by a crypto factory. A factory implementation can use or ignore any of these inputs.
 *
 * The example usage could be as below.
 *   1. Write a class to implement DecryptionPropertiesFactory.
 *   2. Set configuration of "parquet.crypto.factory.class" with the fully qualified name of this class.
 *      For example, we can set the configuration in SparkSession as below.
 *         SparkSession spark = SparkSession
 *                     .config("parquet.crypto.factory.class",
 *                     "xxx.xxx.DecryptionPropertiesClassLoaderImpl")
 *
 * The implementation of this interface will be instantiated by {@link #loadFactory(Configuration)}.
 */
public interface DecryptionPropertiesFactory {

  Logger LOG = LoggerFactory.getLogger(DecryptionPropertiesFactory.class);
  String CRYPTO_FACTORY_CLASS_PROPERTY_NAME = "parquet.crypto.factory.class";

  /**
   * Load DecryptionPropertiesFactory class specified by CRYPTO_FACTORY_CLASS_PROPERTY_NAME as the path in the configuration
   *
   * @param conf Configuration where user specifies the class path
   * @return object with class DecryptionPropertiesFactory if user specified the class path and invoking of
   * the class succeeds, null if user doesn't specify the class path
   * @throws BadConfigurationException if the instantiation of the configured class fails
   */
  static DecryptionPropertiesFactory loadFactory(Configuration conf) {
    final Class<?> decryptionPropertiesFactoryClass = ConfigurationUtil.getClassFromConfig(conf,
      CRYPTO_FACTORY_CLASS_PROPERTY_NAME, DecryptionPropertiesFactory.class);

    if (null == decryptionPropertiesFactoryClass) {
      LOG.debug("DecryptionPropertiesFactory is not configured - name not found in hadoop config");
      return null;
    }

    try {
      return (DecryptionPropertiesFactory) decryptionPropertiesFactoryClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new BadConfigurationException("could not instantiate decryptionPropertiesFactoryClass class: "
        + decryptionPropertiesFactoryClass, e);
    }
  }

  /**
   * Get FileDecryptionProperties object which is created by the implementation of this interface. Please see
   * the unit test SampleDecryptionPropertiesFactory for example
   *
   * @param hadoopConfig Configuration that is used to pass the needed information, e.g. KMS uri
   * @param filePath File path of the parquet file
   *                 Can be used for AAD prefix verification, part of key metadata etc
   * @return object with class of FileDecryptionProperties
   * @throws ParquetCryptoRuntimeException if there is an exception while creating the object
   */
  FileDecryptionProperties getFileDecryptionProperties(Configuration hadoopConfig, Path filePath) throws ParquetCryptoRuntimeException;
}
