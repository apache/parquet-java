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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.junit.jupiter.api.Test;

public class EncryptionPropertiesFactoryTest {
  @Test
  public void testLoadEncPropertiesFactory() {
    Configuration conf = new Configuration();
    conf.set(
        EncryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME,
        "org.apache.parquet.crypto.SampleEncryptionPropertiesFactory");

    EncryptionPropertiesFactory encryptionPropertiesFactory = EncryptionPropertiesFactory.loadFactory(conf);
    FileEncryptionProperties encryptionProperties =
        encryptionPropertiesFactory.getFileEncryptionProperties(conf, null, null);

    assertThat(encryptionProperties.getFooterKey()).isEqualTo(SampleEncryptionPropertiesFactory.FOOTER_KEY);
    assertThat(encryptionProperties.getColumnProperties(SampleEncryptionPropertiesFactory.COL1))
        .isEqualTo(SampleEncryptionPropertiesFactory.COL1_ENCR_PROPERTIES);
    assertThat(encryptionProperties.getColumnProperties(SampleEncryptionPropertiesFactory.COL2))
        .isEqualTo(SampleEncryptionPropertiesFactory.COL2_ENCR_PROPERTIES);
  }

  @Test
  public void testLoadEncPropertiesFactoryParquetConfiguration() {
    ParquetConfiguration conf = new PlainParquetConfiguration();
    conf.set(
        EncryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME,
        "org.apache.parquet.crypto.SampleEncryptionPropertiesFactory");

    EncryptionPropertiesFactory encryptionPropertiesFactory = EncryptionPropertiesFactory.loadFactory(conf);
    FileEncryptionProperties encryptionProperties = encryptionPropertiesFactory.getFileEncryptionProperties(
        ConfigurationUtil.createHadoopConfiguration(conf), null, null);

    assertThat(encryptionProperties.getFooterKey()).isEqualTo(SampleEncryptionPropertiesFactory.FOOTER_KEY);
    assertThat(encryptionProperties.getColumnProperties(SampleEncryptionPropertiesFactory.COL1))
        .isEqualTo(SampleEncryptionPropertiesFactory.COL1_ENCR_PROPERTIES);
    assertThat(encryptionProperties.getColumnProperties(SampleEncryptionPropertiesFactory.COL2))
        .isEqualTo(SampleEncryptionPropertiesFactory.COL2_ENCR_PROPERTIES);
  }
}
