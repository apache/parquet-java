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

import static org.apache.parquet.Preconditions.checkNotNull;

public interface CryptoPropertiesFactory {

  Logger LOG = LoggerFactory.getLogger(CryptoPropertiesFactory.class);
  String CRYPTO_FACTORY_CLASS_PROPERTY_NAME = "parquet.encryption.factory.class";

  /**
   * Load CryptoPropertiesFactory class specified by CRYPTO_FACTORY_CLASS_PROPERTY_NAME as the path in the configuration
   *
   * @param conf Configuration where user specifies the class path
   * @return return object with class CryptoPropertiesFactory if user specified the class path and invoking of
   * the class succeeds, null if user doesn't specify the class path. RunTimeException with type BadConfigurationException
   * will be thrown if invoking the configured class fails
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
   * @param tempFilePath File path of the parquet file
   * @param fileWriteContext WriteContext to provide information like schema to build the FileEncryptionProperties
   * @return
   * @throws IOException
   */
  FileEncryptionProperties getFileEncryptionProperties(Configuration fileHadoopConfig, Path tempFilePath,
                                                       WriteContext fileWriteContext)  throws IOException;

  /**
   *
   * @param hadoopConfig Configuration that is used to pass the needed information, e.g. KMS uri
   * @param filePath  File path of the parquet file
   * @return
   * @throws IOException
   */
  FileDecryptionProperties getFileDecryptionProperties(Configuration hadoopConfig, Path filePath)  throws IOException;
}
