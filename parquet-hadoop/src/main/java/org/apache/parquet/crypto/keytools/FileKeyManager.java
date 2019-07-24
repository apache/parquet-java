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



package org.apache.parquet.crypto.keytools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Management of file encryption keys, and their metadata. 
 * For scalability, implementing code is recommended to run locally / not to make remote calls -
 * except for calls to KMS server, via the pre-defined KmsClient interface. 
 *
 * Implementing class instance should be created per each Parquet file. The methods don't need to
 * be thread-safe.
 */
public abstract class FileKeyManager {
  private static final Logger LOG = LoggerFactory.getLogger(FileKeyManager.class);
  public static final String DEFAULT_KMS_INSTANCE_ID = "DEFAULT";

  protected KmsClient kmsClient;
  protected String kmsInstanceID;

  /**
   * 
   * @param configuration
   * @param kmsClient
   * @param keyMaterialStore
   * @param fileID
   * @throws IOException
   */
  public abstract void initialize(Configuration configuration, KmsClient kmsClient, KeyMaterialStore keyMaterialStore, String fileID) throws IOException;
  
  /**
   * File writing/encryption: Generates or fetches a column encryption key, and creates its metadata. 
   * Eg can generate a random data key, and wrap it with a master key. ColumnKeyID is the master key then.
   * Or can fetch the data key from KMS. ColumnKeyID is the data key in this case.
   * @param column
   * @param columnKeyID 
   * @return
   * @throws IOException
   */
  public abstract KeyWithMetadata getColumnEncryptionKey(ColumnPath column, String columnKeyID) throws IOException;
  
  /**
   * File writing/encryption: Generates or fetches a footer encryption/signing key, and creates its metadata.
   * Similar to column keys, but separate because footer key metadata can store additional information (such as 
   * KMS instance identity) - so it doesn't have to be stored in each column key metadata.
   * @param footerKeyID
   * @return
   * @throws IOException
   */
  public abstract KeyWithMetadata getFooterEncryptionKey(String footerKeyID) throws IOException;
  
  /**
   * File reading/decryption: Returns key retrieval callback.
   * @return
   */
  public abstract DecryptionKeyRetriever getDecryptionKeyRetriever();
  
  /**
   * Wipes keys in memory.
   * Flushes key material to external store (if in use).
   */
  public abstract void close();


  /**
   * Create and initialize a file key manager class, based on hadoop configuration.
   * The configuration should specify the file key manager class.
   * If kms instance ID can be fetched from the configuration or has a default value,
   * then create and initialize a KMSClient here too.
   * Otherwise, KMS instance ID should be fetched from parquet file metadata and only then KMS client should be created.
   * @param configuration Hadoop configuration
   * @return
   * @throws IOException
   */
  public static FileKeyManager getKeyManager(Configuration configuration) throws IOException {
    // Create per-file KeyManager
    FileKeyManager keyManager = null;
    String keyManagerClassName = configuration.getTrimmed("encryption.key.manager.class");
    if (null == keyManagerClassName || keyManagerClassName.equals(WrappedKeyManager.class.getCanonicalName())) {
      keyManager = new WrappedKeyManager();
    }
    else {
      try {
        keyManager = ((Class<? extends FileKeyManager>) Class.forName(keyManagerClassName)).newInstance(); // TODO
      }
      catch (Exception e) {
        throw new IOException("Failed to instantiate FileKeyManager " + keyManagerClassName, e);
      }
    }
    KmsClient kmsClient = keyManager.getKmsClient(configuration);

    keyManager.initialize(configuration, kmsClient, null, null); //TODO add external storage?
    return keyManager;
  }

  /**
   * KMSClient created if KMS instance is passed in configuration or if KMS client has a default value.
   * @param configuration
   * @return
   * @throws IOException
   */
  protected KmsClient getKmsClient(Configuration configuration) throws IOException {
    kmsInstanceID = configuration.getTrimmed("encryption.kms.instance.id");
    if (null == kmsInstanceID) {
      LOG.warn("encryption.kms.instance.id not defined. Setting default KMS instance ID value.");
      kmsInstanceID = DEFAULT_KMS_INSTANCE_ID;
    }
    return getKmsClient(configuration, kmsInstanceID);
  }

  /**
   * Create and initialize a KMSClient. Called when KMS instance ID should be known.
   * @param configuration
   * @param kmsInstanceID
   * @return
   * @throws IOException
   */
  protected static KmsClient getKmsClient(Configuration configuration, String kmsInstanceID) throws IOException {
    KmsClient kmsClient = instantiateKmsClient(configuration);
    try {
      kmsClient.initialize(configuration, kmsInstanceID);
    } catch (IOException e) {
      LOG.warn("Cannot create KMS client. If encryption.kms.instance.id not defined, will be expecting to use " +
              "default KMS instance ID, if relevant, or key metadata from parquet file.");
      return null;
    }
    return kmsClient;
  }

  private static KmsClient instantiateKmsClient(Configuration configuration) throws IOException {
    String kmsClientClassName = configuration.getTrimmed("encryption.kms.client.class");
    if (null == kmsClientClassName) {
      throw new IOException("Undefined encryption.kms.client.class");
    }

    KmsClient kmsClient = null;
    try {
      kmsClient = ((Class<? extends KmsClient>) Class.forName(kmsClientClassName)).newInstance(); // TODO
    } catch (Exception e) {
      throw new IOException("Failed to instantiate KmsClient " + kmsClientClassName, e);
    }
    return kmsClient;
  }
}
