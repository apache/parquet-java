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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CryptoPropertiesFactory {
  private static final Logger LOG = LoggerFactory.getLogger(CryptoPropertiesFactory.class);
  
  public static final String CRYPTO_FACTORY_CLASS_PROPERTY_NAME = "parquet.encryption.factory.class";
  
  public static CryptoPropertiesFactory loadFactory(Configuration conf) throws IOException {
    if (null == conf) {
      LOG.debug("CryptoPropertiesFactory is not configured - null hadoop config");
      return null;
    }

    return getCryptoPropertiesFactory(conf);
  }

  private static CryptoPropertiesFactory getCryptoPropertiesFactory(Configuration hadoopConfig) {
    final Class<?> cryptoPropertiesFactoryClass = ConfigurationUtil.getClassFromConfig(hadoopConfig,
      CRYPTO_FACTORY_CLASS_PROPERTY_NAME, CryptoPropertiesFactory.class);
    
    if (null == cryptoPropertiesFactoryClass) {
      LOG.debug("CryptoPropertiesFactory is not configured - name not found in hadoop config");
      return null;
    }
    
    LOG.debug("CryptoPropertiesFactory implementation is: " + cryptoPropertiesFactoryClass);
    
    try {
      CryptoPropertiesFactory cryptoFactory = (CryptoPropertiesFactory)cryptoPropertiesFactoryClass.newInstance();
      cryptoFactory.initialize(hadoopConfig);
      return cryptoFactory;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new BadConfigurationException("could not instantiate crypto metadata retriever class: "
        + cryptoPropertiesFactoryClass, e);
    }
  }

  public abstract void initialize(Configuration hadoopConfig);

  public abstract FileEncryptionProperties getFileEncryptionProperties(
    Configuration fileHadoopConfig, Path tempFilePath,
    WriteContext fileWriteContext)  throws IOException;

  public abstract FileDecryptionProperties getFileDecryptionProperties(
    Configuration hadoopConfig, Path filePath)  throws IOException;
}
