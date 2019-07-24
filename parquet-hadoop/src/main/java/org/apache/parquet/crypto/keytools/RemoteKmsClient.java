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

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An abstract class for implementation of a remote-KMS client.
 * Both KMS instance ID and KMS URL need to be defined in order to access such a KMS.
 * The concrete implementation should implement getKeyFromServer() and/or
 * wrapDataKeyInServer() with unwrapDataKeyInServer() methods.
 */
public abstract class RemoteKmsClient implements KmsClient {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteKmsClient.class);

  protected String kmsInstanceID;
  protected String kmsURL;
  // Example value that matches the pattern:    vault-instance-1: http://127.0.0.1:8200
  protected Pattern kmsUrlListItemPattern = Pattern.compile("^(\\S+)\\s*:\\s*(\\w*://\\S+)$");

  /**
   *  Initialize the KMS Client with KMS instance ID and URL.
   *  When reading a parquet file, the KMS instance ID can be either specified in configuration
   *  or read from parquet file metadata, or default if there is a default value for this KMS type.
   *  When writing a parquet file, the KMS instance ID has to be specified in configuration
   *  or default if there is a default value for this KMS type.
   *  The KMS URL has to be specified in configuration either specifically or as a mapping of KMS instance ID to KMS URL,
   *  e.g. { "kmsInstanceID1": "kmsURL1", "kmsInstanceID2" : "kmsURL2" }, but not both.
   * @param configuration Hadoop configuration
   * @param kmsInstanceID instance ID of the KMS managed by this KmsClient. When reading a parquet file, the KMS
   *                      instance ID can be either specified in configuration or read from parquet file metadata.
   *                      When writing a parquet file, the KMS instance ID has to be specified in configuration.
   *                      KMSClient implementation could have a default value for this.
   * @throws IOException
   */
  @Override
  public void initialize(Configuration configuration, String kmsInstanceID) throws IOException {
    this.kmsInstanceID = kmsInstanceID;
    setKmsURL(configuration);
    initializeInternal(configuration);
  }

  protected abstract void initializeInternal(Configuration configuration) throws IOException;

  private void setKmsURL(Configuration configuration) throws IOException {
    final String kmsUrlProperty = configuration.getTrimmed("encryption.kms.instance.url");
    final String[] kmsUrlList = configuration.getTrimmedStrings("encryption.kms.instance.url.list");
    if (StringUtils.isEmpty(kmsUrlProperty) && ArrayUtils.isEmpty(kmsUrlList)) {
      throw new IOException("KMS URL is not set.");
    }
    if (!StringUtils.isEmpty(kmsUrlProperty) && !ArrayUtils.isEmpty(kmsUrlList)) {
      throw new IOException("KMS URL is ambiguous: " +
              "it should either be set in encryption.kms.instance.url or in encryption.kms.instance.url.list");
    }
    if (!StringUtils.isEmpty(kmsUrlProperty)) {
      kmsURL = kmsUrlProperty;
    } else {
      if (StringUtils.isEmpty(kmsInstanceID) ) {
        throw new IOException("Missing kms instance id value. Cannot find a matching KMS URL mapping.");
      }
      Map<String, String> kmsUrlMap = new HashMap<String, String>(kmsUrlList.length);
      int nKeys = kmsUrlList.length;
      for (int i=0; i < nKeys; i++) {
        Matcher m = kmsUrlListItemPattern.matcher(kmsUrlList[i]);
        if (!m.matches() || (m.groupCount() != 2)) {
          throw new IOException(String.format("String %s doesn't match pattern %s for KMS URL mapping",
                  kmsUrlList[i], kmsUrlListItemPattern.toString()));
        }
        String instanceID = m.group(1);
        String kmsURL = m.group(2);
        //TODO check parts
        kmsUrlMap.put(instanceID, kmsURL);
      }
      kmsURL = kmsUrlMap.get(kmsInstanceID);
      if (StringUtils.isEmpty(kmsURL) ) {
        throw new IOException(String.format("Missing KMS URL for kms instance ID [%s] in KMS URL mapping",
                kmsInstanceID));
      }
    }
  }


  @Override
  public abstract boolean supportsServerSideWrapping();

  /**
   * This method should be implemented by the concrete RemoteKmsClient implementation,
   * otherwise it throws an UnsupportedOperationException.
   */
  @Override
  public String getKeyFromServer(String keyIdentifier)
      throws UnsupportedOperationException, KeyAccessDeniedException, IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * This method should be implemented by the concrete RemoteKmsClient implementation,
   * otherwise it throws an UnsupportedOperationException.
   */
  @Override
  public String wrapDataKeyInServer(String dataKey, String masterKeyIdentifier)
      throws UnsupportedOperationException, KeyAccessDeniedException, IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * This method should be implemented by the concrete RemoteKmsClient implementation,
   * otherwise it throws an UnsupportedOperationException.
   */
  @Override
  public String unwrapDataKeyInServer(String wrappedDataKey, String masterKeyIdentifier)
      throws UnsupportedOperationException, KeyAccessDeniedException, IOException {
    throw new UnsupportedOperationException();
  }
}
