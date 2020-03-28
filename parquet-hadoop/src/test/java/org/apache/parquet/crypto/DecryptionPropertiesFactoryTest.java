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
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class DecryptionPropertiesFactoryTest {
  @Test
  public void testLoadDecPropertiesFactory() {
    Configuration conf = new Configuration();
    conf.set(EncryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME,
      "org.apache.parquet.crypto.SampleDecryptionPropertiesFactory");

    DecryptionPropertiesFactory decryptionPropertiesFactory = DecryptionPropertiesFactory.loadFactory(conf);
    FileDecryptionProperties decryptionProperties = decryptionPropertiesFactory.getFileDecryptionProperties(conf, null);

    assertArrayEquals(decryptionProperties.getFooterKey(), SampleDecryptionPropertiesFactory.FOOTER_KEY);
    assertArrayEquals(decryptionProperties.getColumnKey(SampleDecryptionPropertiesFactory.COL1),
      SampleDecryptionPropertiesFactory.COL1_KEY);
    assertArrayEquals(decryptionProperties.getColumnKey(SampleDecryptionPropertiesFactory.COL2),
      SampleDecryptionPropertiesFactory.COL2_KEY);
  }
}
