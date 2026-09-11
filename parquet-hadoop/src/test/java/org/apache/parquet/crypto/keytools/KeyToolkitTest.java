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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class KeyToolkitTest {

  private static final long CACHE_LIFETIME_MILLIS = 60_000;
  private static final String MASTER_KEY_ID = "shared-master-key";

  private final List<Configuration> configurationsWithFactories = new ArrayList<>();

  @AfterEach
  public void clearCaches() {
    for (Configuration configuration : configurationsWithFactories) {
      KeyToolkit.removeKmsClientFactory(configuration);
    }
    KeyToolkit.removeCacheEntriesForAllTokens();
  }

  private void setKmsClientFactory(Configuration configuration, KmsClientFactory factory) {
    KeyToolkit.setKmsClientFactory(configuration, factory);
    configurationsWithFactories.add(configuration);
  }

  @Test
  public void prefersConfiguredKmsClientFactory() {
    Configuration configuration = new Configuration(false);
    configuration.set(KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME, ReflectiveKmsClient.class.getName());
    ConstructorInjectedKmsClient client = new ConstructorInjectedKmsClient("dependency");
    AtomicInteger factoryCalls = new AtomicInteger();
    setKmsClientFactory(configuration, (conf, kmsId, kmsUrl, token) -> {
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
  public void factoryRegistrationSurvivesConfigurationMutationAndReceivesCurrentContext() {
    Configuration configuration = new Configuration(false);
    ConstructorInjectedKmsClient client = new ConstructorInjectedKmsClient("dependency");
    List<Configuration> factoryConfigurations = new ArrayList<>();
    List<String> factoryValues = new ArrayList<>();
    List<String> factoryKmsInstanceIDs = new ArrayList<>();
    List<String> factoryKmsInstanceURLs = new ArrayList<>();
    List<String> factoryAccessTokens = new ArrayList<>();
    setKmsClientFactory(configuration, (currentConfiguration, kmsInstanceID, kmsInstanceURL, accessToken) -> {
      factoryConfigurations.add(currentConfiguration);
      factoryValues.add(currentConfiguration.get("custom.factory.parameter"));
      factoryKmsInstanceIDs.add(kmsInstanceID);
      factoryKmsInstanceURLs.add(kmsInstanceURL);
      factoryAccessTokens.add(accessToken);
      return client;
    });

    configuration.set("custom.factory.parameter", "updated");

    KmsClient actual = KeyToolkit.getKmsClient("instance", "url", configuration, "token", CACHE_LIFETIME_MILLIS);

    assertThat(actual).isSameAs(client);
    assertThat(factoryConfigurations).containsExactly(configuration);
    assertThat(factoryValues).containsExactly("updated");
    assertThat(factoryKmsInstanceIDs).containsExactly("instance");
    assertThat(factoryKmsInstanceURLs).containsExactly("url");
    assertThat(factoryAccessTokens).containsExactly("token");
  }

  @Test
  public void configurationCopyUsesRegisteredKmsClientFactory() {
    Configuration configuration = new Configuration(false);
    ConstructorInjectedKmsClient client = new ConstructorInjectedKmsClient("dependency");
    setKmsClientFactory(configuration, (conf, kmsId, kmsUrl, token) -> client);
    Configuration copy = new Configuration(configuration);

    KmsClient actual = KeyToolkit.getKmsClient("instance", "url", copy, "token", CACHE_LIFETIME_MILLIS);

    assertThat(actual).isSameAs(client);
    assertThat(client.configuration).isSameAs(copy);
  }

  @Test
  public void missingFactoryForConfigurationCopyFailsEncryptionPropertiesCreation() {
    Configuration configuration = new Configuration(false);
    configuration.set(PropertiesDrivenCryptoFactory.UNIFORM_KEY_PROPERTY_NAME, MASTER_KEY_ID);
    setKmsClientFactory(
        configuration, (conf, kmsId, kmsUrl, token) -> new ConstructorInjectedKmsClient("dependency"));
    Configuration copy = new Configuration(configuration);
    KeyToolkit.removeKmsClientFactory(configuration);

    assertThatThrownBy(() -> new PropertiesDrivenCryptoFactory()
            .getFileEncryptionProperties(copy, new Path("encrypted.parquet"), null))
        .isInstanceOf(ParquetCryptoRuntimeException.class)
        .hasMessage("No KmsClientFactory is registered for this configuration");
  }

  @Test
  public void createsDistinctKmsClientsForDifferentAccessTokens() {
    Configuration configuration = new Configuration(false);
    List<ConstructorInjectedKmsClient> clients = new ArrayList<>();
    setKmsClientFactory(configuration, (conf, kmsId, kmsUrl, token) -> {
      ConstructorInjectedKmsClient client = new ConstructorInjectedKmsClient("client-" + clients.size());
      clients.add(client);
      return client;
    });

    KmsClient first =
        KeyToolkit.getKmsClient("instance", "url", configuration, "first-token", CACHE_LIFETIME_MILLIS);
    KmsClient second =
        KeyToolkit.getKmsClient("instance", "url", configuration, "second-token", CACHE_LIFETIME_MILLIS);

    assertThat(clients).hasSize(2);
    assertThat(first).isSameAs(clients.get(0));
    assertThat(second).isSameAs(clients.get(1));
    assertThat(first).isNotSameAs(second);
    assertThat(clients.get(0).accessToken).isEqualTo("first-token");
    assertThat(clients.get(1).accessToken).isEqualTo("second-token");
    assertThat(clients.get(0).initializeCalls).isEqualTo(1);
    assertThat(clients.get(1).initializeCalls).isEqualTo(1);
  }

  @Test
  public void scopesKmsClientFactoryAndCacheToConfiguration() {
    Configuration firstConfiguration = new Configuration(false);
    Configuration secondConfiguration = new Configuration(false);
    ConstructorInjectedKmsClient firstClient = new ConstructorInjectedKmsClient("first");
    ConstructorInjectedKmsClient secondClient = new ConstructorInjectedKmsClient("second");
    setKmsClientFactory(firstConfiguration, (conf, kmsId, kmsUrl, token) -> firstClient);
    setKmsClientFactory(secondConfiguration, (conf, kmsId, kmsUrl, token) -> secondClient);

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
    setKmsClientFactory(factoryConfiguration, (conf, kmsId, kmsUrl, token) -> factoryClient);
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
    setKmsClientFactory(configuration, (conf, kmsId, kmsUrl, token) -> firstClient);
    KmsClient first = KeyToolkit.getKmsClient("instance", "url", configuration, "token", CACHE_LIFETIME_MILLIS);

    setKmsClientFactory(configuration, (conf, kmsId, kmsUrl, token) -> replacementClient);
    KmsClient replacement =
        KeyToolkit.getKmsClient("instance", "url", configuration, "token", CACHE_LIFETIME_MILLIS);

    assertThat(first).isSameAs(firstClient);
    assertThat(replacement).isSameAs(replacementClient);
  }

  @Test
  public void removeCacheEntriesForTokenClearsOnlyMatchingFactoryClients() {
    Configuration configuration = new Configuration(false);
    AtomicInteger factoryCalls = new AtomicInteger();
    setKmsClientFactory(
        configuration,
        (conf, kmsId, kmsUrl, token) ->
            new ConstructorInjectedKmsClient(Integer.toString(factoryCalls.incrementAndGet())));
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
    setKmsClientFactory(
        configuration,
        (conf, kmsId, kmsUrl, token) ->
            new ConstructorInjectedKmsClient(Integer.toString(factoryCalls.incrementAndGet())));
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
    setKmsClientFactory(configuration, (conf, kmsId, kmsUrl, token) -> null);

    assertThatThrownBy(
            () -> KeyToolkit.getKmsClient("instance", "url", configuration, "token", CACHE_LIFETIME_MILLIS))
        .isInstanceOf(ParquetCryptoRuntimeException.class)
        .hasMessage("KmsClientFactory returned null");
  }

  @Test
  public void removeKmsClientFactoryRemovesRegistrationForClientRetainingConfiguration() {
    Configuration configuration = new Configuration(false);
    configuration.set(KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME, ReflectiveKmsClient.class.getName());
    ConstructorInjectedKmsClient factoryClient = new ConstructorInjectedKmsClient("dependency");
    setKmsClientFactory(configuration, (conf, kmsId, kmsUrl, token) -> factoryClient);
    KmsClient registered =
        KeyToolkit.getKmsClient("instance", "url", configuration, "token", CACHE_LIFETIME_MILLIS);
    KeyToolkit.KmsClientCacheContext cacheContext = KeyToolkit.getKmsClientCacheContext(configuration);
    cacheContext
        .getKekWriteCache()
        .getOrCreateInternalCache("token", CACHE_LIFETIME_MILLIS)
        .computeIfAbsent("instance", ignored -> new ConcurrentHashMap<>())
        .put("master-key", new KeyToolkit.KeyEncryptionKey(new byte[16], new byte[16], "wrapped"));
    cacheContext
        .getKekReadCache()
        .getOrCreateInternalCache("token", CACHE_LIFETIME_MILLIS)
        .put("kek", new byte[16]);

    KeyToolkit.removeKmsClientFactory(configuration);

    KmsClient fallback = KeyToolkit.getKmsClient("instance", "url", configuration, "token", CACHE_LIFETIME_MILLIS);
    assertThat(registered).isSameAs(factoryClient);
    assertThat(factoryClient.configuration).isSameAs(configuration);
    assertThat(fallback).isInstanceOf(ReflectiveKmsClient.class);
    assertThat(cacheContext.getKmsClientCache().getOrCreateInternalCache("token", CACHE_LIFETIME_MILLIS))
        .isEmpty();
    assertThat(cacheContext.getKekWriteCache().getOrCreateInternalCache("token", CACHE_LIFETIME_MILLIS))
        .isEmpty();
    assertThat(cacheContext.getKekReadCache().getOrCreateInternalCache("token", CACHE_LIFETIME_MILLIS))
        .isEmpty();
  }

  @Test
  public void isolatesDoubleWrappingWriteCacheByFactoryRegistration() {
    TrackingKmsClient firstClient = new TrackingKmsClient("0123456789012346", false);
    TrackingKmsClient secondClient = new TrackingKmsClient("6543210987654321", false);
    Configuration firstConfiguration = newFactoryConfiguration(firstClient);
    Configuration secondConfiguration = newFactoryConfiguration(secondClient);
    byte[] dataKey = new byte[16];

    new FileKeyWrapper(firstConfiguration, null).getEncryptionKeyMetadata(dataKey, MASTER_KEY_ID, true);
    new FileKeyWrapper(secondConfiguration, null).getEncryptionKeyMetadata(dataKey, MASTER_KEY_ID, true);

    assertThat(firstClient.wrapCalls).hasValue(1);
    assertThat(secondClient.wrapCalls).hasValue(1);
  }

  @Test
  public void isolatesDoubleWrappingWriteCacheByKmsInstanceForConfigurationCopies() {
    String firstKmsInstanceID = "first-instance";
    String secondKmsInstanceID = "second-instance";
    TrackingKmsClient firstClient = new TrackingKmsClient("0123456789012346", false);
    TrackingKmsClient secondClient = new TrackingKmsClient("6543210987654321", false);
    Configuration firstConfiguration = new Configuration(false);
    firstConfiguration.setBoolean(KeyToolkit.DOUBLE_WRAPPING_PROPERTY_NAME, true);
    firstConfiguration.set(KeyToolkit.KEY_ACCESS_TOKEN_PROPERTY_NAME, "shared-token");
    firstConfiguration.set(KeyToolkit.KMS_INSTANCE_ID_PROPERTY_NAME, firstKmsInstanceID);
    setKmsClientFactory(
        firstConfiguration,
        (conf, kmsId, kmsUrl, token) -> kmsId.equals(firstKmsInstanceID) ? firstClient : secondClient);
    Configuration secondConfiguration = new Configuration(firstConfiguration);
    secondConfiguration.set(KeyToolkit.KMS_INSTANCE_ID_PROPERTY_NAME, secondKmsInstanceID);
    byte[] dataKey = new byte[16];

    byte[] firstMetadata =
        new FileKeyWrapper(firstConfiguration, null).getEncryptionKeyMetadata(dataKey, MASTER_KEY_ID, true);
    byte[] secondMetadata =
        new FileKeyWrapper(secondConfiguration, null).getEncryptionKeyMetadata(dataKey, MASTER_KEY_ID, true);

    assertThat(firstClient.wrapCalls).hasValue(1);
    assertThat(secondClient.wrapCalls).hasValue(1);
    assertThat(KeyMaterial.parse(new String(firstMetadata, StandardCharsets.UTF_8))
            .getKmsInstanceID())
        .isEqualTo(firstKmsInstanceID);
    assertThat(KeyMaterial.parse(new String(secondMetadata, StandardCharsets.UTF_8))
            .getKmsInstanceID())
        .isEqualTo(secondKmsInstanceID);

    assertThat(new FileKeyUnwrapper(firstConfiguration, new Path("first.parquet")).getKey(firstMetadata))
        .isEqualTo(dataKey);
    KeyToolkit.getKmsClientCacheContext(firstConfiguration)
        .getKekReadCache()
        .clear();
    assertThat(new FileKeyUnwrapper(secondConfiguration, new Path("second.parquet")).getKey(secondMetadata))
        .isEqualTo(dataKey);
  }

  @Test
  public void isolatesDoubleWrappingReadCacheByFactoryRegistration() {
    TrackingKmsClient permittedClient = new TrackingKmsClient("0123456789012346", false);
    Configuration permittedConfiguration = newFactoryConfiguration(permittedClient);
    byte[] dataKey = new byte[16];
    byte[] keyMetadata =
        new FileKeyWrapper(permittedConfiguration, null).getEncryptionKeyMetadata(dataKey, MASTER_KEY_ID, true);
    FileKeyUnwrapper permittedUnwrapper =
        new FileKeyUnwrapper(permittedConfiguration, new Path("encrypted.parquet"));
    assertThat(permittedUnwrapper.getKey(keyMetadata)).isEqualTo(dataKey);

    TrackingKmsClient deniedClient = new TrackingKmsClient("6543210987654321", true);
    Configuration deniedConfiguration = newFactoryConfiguration(deniedClient);
    FileKeyUnwrapper deniedUnwrapper = new FileKeyUnwrapper(deniedConfiguration, new Path("encrypted.parquet"));

    assertThatThrownBy(() -> deniedUnwrapper.getKey(keyMetadata))
        .isInstanceOf(ParquetCryptoRuntimeException.class)
        .hasMessage("KMS access denied");
    assertThat(deniedClient.unwrapCalls).hasValue(1);
  }

  @Test
  public void usesConfiguredClassWhenFactoryIsNotSet() {
    Configuration configuration = new Configuration(false);
    configuration.set(KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME, ReflectiveKmsClient.class.getName());

    KmsClient client = KeyToolkit.getKmsClient("instance", "url", configuration, "token", CACHE_LIFETIME_MILLIS);

    assertThat(client).isInstanceOf(ReflectiveKmsClient.class);
    assertThat(((ReflectiveKmsClient) client).initializeCalls).isEqualTo(1);
  }

  private Configuration newFactoryConfiguration(KmsClient kmsClient) {
    Configuration configuration = new Configuration(false);
    configuration.setBoolean(KeyToolkit.DOUBLE_WRAPPING_PROPERTY_NAME, true);
    setKmsClientFactory(configuration, (conf, kmsId, kmsUrl, token) -> kmsClient);
    return configuration;
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

  private static class TrackingKmsClient implements KmsClient {
    private final byte[] masterKey;
    private final boolean denyUnwrap;
    private final AtomicInteger wrapCalls = new AtomicInteger();
    private final AtomicInteger unwrapCalls = new AtomicInteger();

    private TrackingKmsClient(String masterKey, boolean denyUnwrap) {
      this.masterKey = masterKey.getBytes(StandardCharsets.UTF_8);
      this.denyUnwrap = denyUnwrap;
    }

    @Override
    public void initialize(
        Configuration configuration, String kmsInstanceID, String kmsInstanceURL, String accessToken) {}

    @Override
    public String wrapKey(byte[] keyBytes, String masterKeyIdentifier) {
      wrapCalls.incrementAndGet();
      return KeyToolkit.encryptKeyLocally(
          keyBytes, masterKey, masterKeyIdentifier.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public byte[] unwrapKey(String wrappedKey, String masterKeyIdentifier) {
      unwrapCalls.incrementAndGet();
      if (denyUnwrap) {
        throw new ParquetCryptoRuntimeException("KMS access denied");
      }
      return KeyToolkit.decryptKeyLocally(
          wrappedKey, masterKey, masterKeyIdentifier.getBytes(StandardCharsets.UTF_8));
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
