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
package org.apache.parquet.crypto.keytools.samples;

import okhttp3.ConnectionSpec;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.keytools.KeyToolkit;
import org.apache.parquet.crypto.keytools.KmsClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * An example of KmsClient implementation. Not for production use!
 */
public class VaultClient implements KmsClient {
  private static final Logger LOG = LoggerFactory.getLogger(VaultClient.class);

  private static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json; charset=utf-8");
  private static final String DEFAULT_TRANSIT_ENGINE = "/v1/transit/";
  private static final String transitWrapEndpoint = "encrypt/";
  private static final String transitUnwrapEndpoint = "decrypt/";
  private static final String tokenHeader="X-Vault-Token";
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private String kmsToken;
  private Configuration hadoopConfiguration;

  private String endPointPrefix;
  private OkHttpClient httpClient = new OkHttpClient.Builder()
    .connectionSpecs(Arrays.asList(ConnectionSpec.MODERN_TLS, ConnectionSpec.COMPATIBLE_TLS))
    .build();

  @Override
  public void initialize(Configuration configuration, String kmsInstanceID, String kmsInstanceURL, String accessToken) 
      throws KeyAccessDeniedException {
    hadoopConfiguration = configuration;
    checkToken(accessToken);
    kmsToken = accessToken;

    if (kmsInstanceURL.equals(KmsClient.KMS_INSTANCE_URL_DEFAULT)) {
      throw new ParquetCryptoRuntimeException("Vault URL not provided");
    }

    if (!kmsInstanceURL.endsWith("/")) {
      kmsInstanceURL += "/";
    }

    String transitEngine = DEFAULT_TRANSIT_ENGINE;
    if (!kmsInstanceID.equals(KmsClient.KMS_INSTANCE_ID_DEFAULT)) {
      transitEngine = "/v1/" + kmsInstanceID;
      if (!transitEngine.endsWith("/")) {
        transitEngine += "/";
      }
    }

    endPointPrefix = kmsInstanceURL + transitEngine;
  }

  @Override
  public String wrapKey(byte[] keyBytes, String masterKeyIdentifier)
      throws KeyAccessDeniedException {
    refreshToken();
    Map<String, String> writeKeyMap = new HashMap<String, String>(1);
    final String dataKeyStr = Base64.getEncoder().encodeToString(keyBytes);
    writeKeyMap.put("plaintext", dataKeyStr);
    String response = getContentFromTransitEngine(endPointPrefix + transitWrapEndpoint, 
        buildPayload(writeKeyMap), masterKeyIdentifier);
    String ciphertext = parseReturn(response, "ciphertext");
    return ciphertext;
  }

  @Override
  public byte[] unwrapKey(String wrappedKey, String masterKeyIdentifier)
      throws KeyAccessDeniedException {
    refreshToken();
    Map<String, String> writeKeyMap = new HashMap<String, String>(1);
    writeKeyMap.put("ciphertext", wrappedKey);
    String response = getContentFromTransitEngine(endPointPrefix + transitUnwrapEndpoint, 
        buildPayload(writeKeyMap), masterKeyIdentifier);
    String plaintext = parseReturn(response, "plaintext");
    final byte[] key = Base64.getDecoder().decode(plaintext);
    return key;
  }

  private String buildPayload(Map<String, String> paramMap) {
    String jsonValue;
    try {
      jsonValue = objectMapper.writeValueAsString(paramMap);
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Failed to build payload", e);
    }
    return jsonValue;
  }

  private void checkToken(String token) {
    if (null == token || token.isEmpty() || token.equals(KmsClient.KEY_ACCESS_TOKEN_DEFAULT)) {
      throw new ParquetCryptoRuntimeException("Wrong Vault token : " + token);
    }
  }

  private void refreshToken() {
    kmsToken = hadoopConfiguration.getTrimmed(KeyToolkit.KEY_ACCESS_TOKEN_PROPERTY_NAME);
    checkToken(kmsToken);
  }

  private String getContentFromTransitEngine(String endPoint, String jPayload, String masterKeyIdentifier) {
    LOG.info("masterKeyIdentifier: " + masterKeyIdentifier);

    final RequestBody requestBody = RequestBody.create(JSON_MEDIA_TYPE, jPayload);
    Request request = new Request.Builder()
        .url(endPoint + masterKeyIdentifier)
        .header(tokenHeader,  kmsToken)
        .post(requestBody).build();

    return executeAndGetResponse(endPoint, request);
  }

  private String executeAndGetResponse(String endPoint, Request request) {
    Response response = null;
    try {
      response = httpClient.newCall(request).execute();
      final String responseBody = response.body().string();
      if (response.isSuccessful()) {
        return responseBody;
      } else {
        if ((401 == response.code()) || (403 == response.code())) {
          throw new KeyAccessDeniedException(responseBody);
        }
        throw new IOException("Vault call [" + endPoint + "] didn't succeed: " + responseBody);
      }
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Vault call [" + request.url().toString() + endPoint + "] didn't succeed", e);
    } finally {
      if (null != response) {
        response.close();
      }
    }
  }

  private static String parseReturn(String response, String searchKey) {
    String matchingValue;
    try {
      matchingValue = objectMapper.readTree(response).findValue(searchKey).textValue();
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Failed to parse vault response. " + searchKey + " not found."  + response, e);
    }

    if(null == matchingValue) {
      throw new ParquetCryptoRuntimeException("Failed to match vault response. " + searchKey + " not found."  + response);
    }
    return matchingValue;
  }
}
