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
import org.apache.parquet.schema.ExtType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.format.EncryptionAlgorithm;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CryptoClassLoaderTest {

  @Test
  public void metadataRetrieverExists() {
    Configuration configuration = new Configuration();
    assertFalse(CryptoClassLoader.metadataRetrieverExists(configuration));
    setRetrieverClass(configuration);
    assertTrue(CryptoClassLoader.metadataRetrieverExists(configuration));
  }

  @Test
  public void getParquetFileEncryptorOrNull() throws IOException {
    Configuration configuration = new Configuration();
    setRetrieverClass(configuration);
    WriteSupport.WriteContext writeContext = createWriteContext();
    InternalFileEncryptor internalFileEncryptor = CryptoClassLoader.getParquetFileEncryptorOrNull(configuration, writeContext);
    assertNotNull(internalFileEncryptor);
    EncryptionAlgorithm encryptionAlgorithm = internalFileEncryptor.getEncryptionAlgorithm();
    assertTrue(encryptionAlgorithm.isSetAES_GCM_V1());
  }

  @Test
  public void getFileEncryptionPropertiesOrNull() throws IOException {
    Configuration configuration = new Configuration();
    setRetrieverClass(configuration);
    WriteSupport.WriteContext writeContext = createWriteContext();
    FileEncryptionProperties fileEncryptionProperties = CryptoClassLoader.getFileEncryptionPropertiesOrNull(configuration, writeContext);
    assertFalse(fileEncryptionProperties.encryptedFooter());
  }

  @Test
  public void getParquetFileDecryptorOrNull() throws IOException {
    Configuration configuration = new Configuration();
    setRetrieverClass(configuration);
    InternalFileDecryptor internalFileDecryptor = CryptoClassLoader.getParquetFileDecryptorOrNull(configuration);
    assertNotNull(internalFileDecryptor);
    assertNull(internalFileDecryptor.getFileAAD());
  }

  @Test
  public void getFileDecryptionPropertiesOrNull() throws IOException {
    Configuration configuration = new Configuration();
    setRetrieverClass(configuration);
    FileDecryptionProperties fileDecryptionProperties = CryptoClassLoader.getFileDecryptionPropertiesOrNull(configuration);
    assertNotNull(fileDecryptionProperties);
    assertNull(fileDecryptionProperties.getAADPrefix());
  }

  private WriteSupport.WriteContext createWriteContext() {
    Type type =  new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "test");
    Type extType = new ExtType<>(type);
    Map<String, Object> metaData = new HashMap<>();
    metaData.put("encrypted", true);
    metaData.put("columnKeyMetaData", "AAA=");
    ((ExtType) extType).setMetadata(metaData);
    MessageType messageType = new MessageType("testCol", extType);
    Map<String, String> extraMetaData = new HashMap<>();
    return new WriteSupport.WriteContext(messageType, extraMetaData);
  }

  private void setRetrieverClass(Configuration configuration) {
    configuration.set(CryptoClassLoader.CRYPTO_METADATA_RETRIEVER_CLASS, org.apache.parquet.crypto.SampleFileEncDecryptorRetriever.class.getName());
  }
}
