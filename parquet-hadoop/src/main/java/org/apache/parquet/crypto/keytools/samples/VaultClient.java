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

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.apache.parquet.crypto.keytools.EnvelopeKeyManager;
import org.apache.parquet.crypto.keytools.RemoteKmsClient;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class VaultClient extends RemoteKmsClient {
  private static final Logger LOG = LoggerFactory.getLogger(VaultClient.class);
  private static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json; charset=utf-8");
  private static final String DEFAULT_TRANSIT_ENGINE = "/v1/transit/";
  private static final String transitWrapEndpoint = "encrypt/";
  private static final String transitUnwrapEndpoint = "decrypt/";
  private static final String DEFAULT_KV_ENGINE = "/v1/secret/data/keys";
  private static final String tokenHeader="X-Vault-Token";
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private String transitEngine = DEFAULT_TRANSIT_ENGINE;
  private String getKeyEndpoint = DEFAULT_KV_ENGINE;
  private OkHttpClient httpClient = new OkHttpClient();

  private String vaultToken;

  @Override
  protected void initializeInternal(Configuration conf) throws IOException {
    vaultToken = conf.getTrimmed(EnvelopeKeyManager.KEY_ACCESS_TOKEN_PROPERTY_NAME);
    if (StringUtils.isEmpty(vaultToken)) {
      throw new IOException("Missing token");
    }
    if (EnvelopeKeyManager.DEFAULT_KMS_INSTANCE_ID != kmsInstanceID) {
      transitEngine = "/v1/" + kmsInstanceID;
      if (!transitEngine.endsWith("/")) {
        transitEngine += "/";
      }
      getKeyEndpoint = "/v1/" + kmsInstanceID;
    }
  }

  @Override
  public String wrapDataKeyInServer(byte[] dataKey, String masterKeyIdentifier) throws IOException, KeyAccessDeniedException, UnsupportedOperationException {
    Map<String, String> writeKeyMap = new HashMap<String, String>(1);
    final String dataKeyStr = Base64.getEncoder().encodeToString(dataKey);
    writeKeyMap.put("plaintext", dataKeyStr);
    String response = getContentFromTransitEngine(transitEngine + transitWrapEndpoint, buildPayload(writeKeyMap), masterKeyIdentifier);
    String ciphertext = parseReturn(response, "ciphertext");
    return ciphertext;
  }

  @Override
  public byte[] unwrapDataKeyInServer(String wrappedKey, String masterKeyIdentifier) throws IOException, KeyAccessDeniedException, UnsupportedOperationException {
    Map<String, String> writeKeyMap = new HashMap<String, String>(1);
    writeKeyMap.put("ciphertext", wrappedKey);
    String response = getContentFromTransitEngine(transitEngine + transitUnwrapEndpoint, buildPayload(writeKeyMap), masterKeyIdentifier);
    String plaintext = parseReturn(response, "plaintext");
    final byte[] key = Base64.getDecoder().decode(plaintext);
    return key;
  }

  @Override
  protected byte[] getKeyFromServer(String key) throws IOException, KeyAccessDeniedException, UnsupportedOperationException {
    LOG.info("standardKeyIdentifier:  " + key);

    final String endpoint = this.kmsURL + getKeyEndpoint;
    Request request = new Request.Builder()
      .url(endpoint)
      .header(tokenHeader,  vaultToken)
      .get().build();

    String response = executeAndGetResponse(endpoint, request);

    JsonNode keysNode = objectMapper.readTree(response).get("data").get("data");
    byte[] matchingValue = null;
    if (null != keysNode) {
      matchingValue = keysNode.findValue(key).getBinaryValue();
    }

    if(null == matchingValue) {
      throw new IOException("Failed to parse vault response. " + key + " not found."  + response);
    }

    return matchingValue;
  }

  private String buildPayload(Map<String, String> paramMap) throws IOException {
    String jsonValue = objectMapper.writeValueAsString(paramMap);
    return jsonValue;
  }

  private String getContentFromTransitEngine(String endPoint, String jPayload, String masterKeyIdentifier) throws IOException, KeyAccessDeniedException {
    LOG.info("masterKeyIdentifier: " + masterKeyIdentifier);
    String masterKeyID = masterKeyIdentifier;

    final RequestBody requestBody = RequestBody.create(JSON_MEDIA_TYPE, jPayload);
    Request request = new Request.Builder()
            .url(this.kmsURL + endPoint + masterKeyID)
            .header(tokenHeader,  vaultToken)
            .post(requestBody).build();

    return executeAndGetResponse(endPoint, request);
  }

  private String executeAndGetResponse(String endPoint, Request request) throws IOException, KeyAccessDeniedException {
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
      throw new IOException("Vault call [" + request.url().toString() + endPoint + "] didn't succeed", e);
    } finally {
      if (null != response) {
        response.close();
      }
    }
  }


  private static String parseReturn(String response, String searchKey) throws IOException {
    String matchingValue = objectMapper.readTree(response).findValue(searchKey).getTextValue();

    if(null == matchingValue) {
      throw new IOException("Failed to parse vault response. " + searchKey + " not found."  + response);
    }
    return matchingValue;
  }

}
