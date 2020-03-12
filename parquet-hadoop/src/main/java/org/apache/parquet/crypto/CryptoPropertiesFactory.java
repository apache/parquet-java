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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CryptoPropertiesFactory {
  private static final Logger LOG = LoggerFactory.getLogger(CryptoPropertiesFactory.class);
  
  public static final String CRYPTO_FACTORY_CLASS_PROPERTY_NAME = "encryption.factory.class";

  public static CryptoPropertiesFactory get(Configuration hadoopConfig) throws IOException {
    CryptoPropertiesFactory cryptoFactory = null;
    String factoryClassName = hadoopConfig.getTrimmed(CRYPTO_FACTORY_CLASS_PROPERTY_NAME);
    if (StringUtils.isEmpty(factoryClassName)) {
      LOG.debug("CryptoPropertiesFactory is not configured");
      return null;
    }
    
    LOG.debug("CryptoPropertiesFactory implementation is: " + factoryClassName);

    try {
      cryptoFactory = (Class.forName(factoryClassName).asSubclass(CryptoPropertiesFactory.class)).newInstance();
    } catch (Exception e) {
      throw new IOException("Failed to instantiate CryptoPropertiesFactory " + factoryClassName, e);
    }

    cryptoFactory.initialize(hadoopConfig);
    return cryptoFactory;
  }

  public abstract void initialize(Configuration hadoopConfig);

  public abstract FileEncryptionProperties getFileEncryptionProperties(Configuration fileHadoopConfig, Path tempFilePath, 
      MessageType fileSchema)  throws IOException;
  
  public abstract FileDecryptionProperties getFileDecryptionProperties(Configuration hadoopConfig, Path filePath)  throws IOException;
}
