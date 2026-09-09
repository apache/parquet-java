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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class KeyToolkitTest {

  private static final long CACHE_LIFETIME_MILLIS = 60_000;

  @AfterEach
  public void clearCaches() {
    KeyToolkit.removeCacheEntriesForAllTokens();
  }

  @Test
  public void prefersConfiguredKmsClientFactory() {
    Configuration configuration = new Configuration(false);
    configuration.set(KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME, ReflectiveKmsClient.class.getName());
    ConstructorInjectedKmsClient client = new ConstructorInjectedKmsClient("dependency");
    AtomicInteger factoryCalls = new AtomicInteger();
    KeyToolkit.setKmsClientFactory(configuration, () -> {
      factoryCalls.incrementAndGet();
      return client;
    });

    KmsClient first = KeyToolkit.getKmsClient("instance", "url", configuration, "token", CACHE_LIFETIME_MILLIS);
    KmsClient second = KeyToolkit.getKmsClient("instance", "url", configuration, "token", CACHE_LIFETIME_MILLIS);

    assertThat(first).isSameAs(client);
    assertThat(second).isSameAs(client);
    assertThat(factoryCalls).hasValue(1);
    assertThat(client.configuration).isSameAs(configuration);
    assertThat(client.kmsInstanceID).isEqualTo("instance");
    assertThat(client.kmsInstanceURL).isEqualTo("url");
    assertThat(client.accessToken).isEqualTo("token");
    assertThat(client.initializeCalls).isEqualTo(1);
  }

  @Test
  public void scopesKmsClientFactoryAndCacheToConfiguration() {
    Configuration firstConfiguration = new Configuration(false);
    Configuration secondConfiguration = new Configuration(false);
    ConstructorInjectedKmsClient firstClient = new ConstructorInjectedKmsClient("first");
    ConstructorInjectedKmsClient secondClient = new ConstructorInjectedKmsClient("second");
    KeyToolkit.setKmsClientFactory(firstConfiguration, () -> firstClient);
    KeyToolkit.setKmsClientFactory(secondConfiguration, () -> secondClient);

    KmsClient first =
        KeyToolkit.getKmsClient("DEFAULT", "DEFAULT", firstConfiguration, "DEFAULT", CACHE_LIFETIME_MILLIS);
    KmsClient second =
        KeyToolkit.getKmsClient("DEFAULT", "DEFAULT", secondConfiguration, "DEFAULT", CACHE_LIFETIME_MILLIS);

    assertThat(first).isSameAs(firstClient);
    assertThat(second).isSameAs(secondClient);
  }

  @Test
  public void factoryRegistrationDoesNotReuseCachedReflectiveClient() {
    Configuration reflectiveConfiguration = new Configuration(false);
    reflectiveConfiguration.set(KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME, ReflectiveKmsClient.class.getName());
    KmsClient reflectiveClient =
        KeyToolkit.getKmsClient("instance", "url", reflectiveConfiguration, "token", CACHE_LIFETIME_MILLIS);

    Configuration factoryConfiguration = new Configuration(false);
    ConstructorInjectedKmsClient factoryClient = new ConstructorInjectedKmsClient("dependency");
    KeyToolkit.setKmsClientFactory(factoryConfiguration, () -> factoryClient);
    KmsClient actual =
        KeyToolkit.getKmsClient("instance", "url", factoryConfiguration, "token", CACHE_LIFETIME_MILLIS);

    assertThat(reflectiveClient).isInstanceOf(ReflectiveKmsClient.class);
    assertThat(actual).isSameAs(factoryClient);
  }

  @Test
  public void replacingKmsClientFactoryDiscardsCachedClient() {
    Configuration configuration = new Configuration(false);
    ConstructorInjectedKmsClient firstClient = new ConstructorInjectedKmsClient("first");
    ConstructorInjectedKmsClient replacementClient = new ConstructorInjectedKmsClient("replacement");
    KeyToolkit.setKmsClientFactory(configuration, () -> firstClient);
    KmsClient first = KeyToolkit.getKmsClient("instance", "url", configuration, "token", CACHE_LIFETIME_MILLIS);

    KeyToolkit.setKmsClientFactory(configuration, () -> replacementClient);
    KmsClient replacement =
        KeyToolkit.getKmsClient("instance", "url", configuration, "token", CACHE_LIFETIME_MILLIS);

    assertThat(first).isSameAs(firstClient);
    assertThat(replacement).isSameAs(replacementClient);
  }

  @Test
  public void removeCacheEntriesForTokenClearsOnlyMatchingFactoryClients() {
    Configuration configuration = new Configuration(false);
    AtomicInteger factoryCalls = new AtomicInteger();
    KeyToolkit.setKmsClientFactory(
        configuration,
        () -> new ConstructorInjectedKmsClient(Integer.toString(factoryCalls.incrementAndGet())));
    KmsClient firstTokenClient =
        KeyToolkit.getKmsClient("instance", "url", configuration, "first-token", CACHE_LIFETIME_MILLIS);
    KmsClient otherTokenClient =
        KeyToolkit.getKmsClient("instance", "url", configuration, "other-token", CACHE_LIFETIME_MILLIS);

    KeyToolkit.removeCacheEntriesForToken("first-token");

    KmsClient refreshedFirstTokenClient =
        KeyToolkit.getKmsClient("instance", "url", configuration, "first-token", CACHE_LIFETIME_MILLIS);
    KmsClient cachedOtherTokenClient =
        KeyToolkit.getKmsClient("instance", "url", configuration, "other-token", CACHE_LIFETIME_MILLIS);
    assertThat(refreshedFirstTokenClient).isNotSameAs(firstTokenClient);
    assertThat(cachedOtherTokenClient).isSameAs(otherTokenClient);
    assertThat(factoryCalls).hasValue(3);
  }

  @Test
  public void removeCacheEntriesForAllTokensClearsFactoryClients() {
    Configuration configuration = new Configuration(false);
    AtomicInteger factoryCalls = new AtomicInteger();
    KeyToolkit.setKmsClientFactory(
        configuration,
        () -> new ConstructorInjectedKmsClient(Integer.toString(factoryCalls.incrementAndGet())));
    KmsClient firstTokenClient =
        KeyToolkit.getKmsClient("instance", "url", configuration, "first-token", CACHE_LIFETIME_MILLIS);
    KmsClient secondTokenClient =
        KeyToolkit.getKmsClient("instance", "url", configuration, "second-token", CACHE_LIFETIME_MILLIS);

    KeyToolkit.removeCacheEntriesForAllTokens();

    KmsClient refreshedFirstTokenClient =
        KeyToolkit.getKmsClient("instance", "url", configuration, "first-token", CACHE_LIFETIME_MILLIS);
    KmsClient refreshedSecondTokenClient =
        KeyToolkit.getKmsClient("instance", "url", configuration, "second-token", CACHE_LIFETIME_MILLIS);
    assertThat(refreshedFirstTokenClient).isNotSameAs(firstTokenClient);
    assertThat(refreshedSecondTokenClient).isNotSameAs(secondTokenClient);
    assertThat(factoryCalls).hasValue(4);
  }

  @Test
  public void rejectsNullKmsClientFromFactory() {
    Configuration configuration = new Configuration(false);
    KeyToolkit.setKmsClientFactory(configuration, () -> null);

    assertThatThrownBy(
            () -> KeyToolkit.getKmsClient("instance", "url", configuration, "token", CACHE_LIFETIME_MILLIS))
        .isInstanceOf(ParquetCryptoRuntimeException.class)
        .hasMessage("KmsClientFactory returned null");
  }

  @Test
  public void usesConfiguredClassWhenFactoryIsNotSet() {
    Configuration configuration = new Configuration(false);
    configuration.set(KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME, ReflectiveKmsClient.class.getName());

    KmsClient client = KeyToolkit.getKmsClient("instance", "url", configuration, "token", CACHE_LIFETIME_MILLIS);

    assertThat(client).isInstanceOf(ReflectiveKmsClient.class);
    assertThat(((ReflectiveKmsClient) client).initializeCalls).isEqualTo(1);
  }

  private static class ConstructorInjectedKmsClient implements KmsClient {
    private final String dependency;
    private Configuration configuration;
    private String kmsInstanceID;
    private String kmsInstanceURL;
    private String accessToken;
    private int initializeCalls;

    private ConstructorInjectedKmsClient(String dependency) {
      this.dependency = dependency;
    }

    @Override
    public void initialize(
        Configuration configuration, String kmsInstanceID, String kmsInstanceURL, String accessToken) {
      this.configuration = configuration;
      this.kmsInstanceID = kmsInstanceID;
      this.kmsInstanceURL = kmsInstanceURL;
      this.accessToken = accessToken;
      initializeCalls++;
    }

    @Override
    public String wrapKey(byte[] keyBytes, String masterKeyIdentifier) {
      return dependency;
    }

    @Override
    public byte[] unwrapKey(String wrappedKey, String masterKeyIdentifier) {
      return dependency.getBytes();
    }
  }

  public static class ReflectiveKmsClient implements KmsClient {
    private int initializeCalls;

    public ReflectiveKmsClient() {}

    @Override
    public void initialize(
        Configuration configuration, String kmsInstanceID, String kmsInstanceURL, String accessToken) {
      initializeCalls++;
    }

    @Override
    public String wrapKey(byte[] keyBytes, String masterKeyIdentifier) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] unwrapKey(String wrappedKey, String masterKeyIdentifier) {
      throw new UnsupportedOperationException();
    }
  }
}
