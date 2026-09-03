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

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.keytools.KeyToolkit;
import org.apache.parquet.crypto.keytools.KmsClient;
import org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory;
import org.apache.parquet.crypto.keytools.mocks.InMemoryKMS;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestKmsUrlRead {

  private static final List<SingleRow> DATA = Collections.unmodifiableList(SingleRow.generateRandomData(5000));
  private static final String UNIFORM_MASTER_KEY =
      Base64.getEncoder().encodeToString("0123456789012346".getBytes(StandardCharsets.UTF_8));
  private static final String UNIFORM_MASTER_KEY_ID = "ku";
  private static final String KEY_LIST = UNIFORM_MASTER_KEY_ID + ": " + UNIFORM_MASTER_KEY;
  private static final String STORED_KMS_URL = "stored-kms-url";

  private static Path filePath;

  public static class UnitestUrlReadKMS extends InMemoryKMS {
    private static String staticKmsURL;

    @Override
    public synchronized void initialize(
        Configuration configuration, String kmsInstanceID, String kmsInstanceURL, String accessToken) {
      staticKmsURL = kmsInstanceURL;
      super.initialize(configuration, kmsInstanceID, kmsInstanceURL, accessToken);
    }

    static String getStaticKmsURL() {
      return staticKmsURL;
    }
  }

  @BeforeAll
  public static void writeEncryptedFile() throws IOException {
    Configuration writeConf = new Configuration();
    writeConf.set(
        EncryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME,
        PropertiesDrivenCryptoFactory.class.getName());
    writeConf.set(PropertiesDrivenCryptoFactory.UNIFORM_KEY_PROPERTY_NAME, UNIFORM_MASTER_KEY_ID);
    writeConf.set(KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME, InMemoryKMS.class.getName());
    writeConf.set(KeyToolkit.KMS_INSTANCE_URL_PROPERTY_NAME, STORED_KMS_URL);
    writeConf.set(InMemoryKMS.KEY_LIST_PROPERTY_NAME, KEY_LIST);
    writeConf.set(KeyToolkit.KEY_ACCESS_TOKEN_PROPERTY_NAME, "writer-token");

    filePath = new Path(Files.createTempFile("test-kms-url_", ".parquet")
        .toAbsolutePath()
        .toString());

    MessageType schema = SingleRow.getSchema();
    SimpleGroupFactory f = new SimpleGroupFactory(schema);

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(filePath)
        .withConf(writeConf)
        .withWriteMode(OVERWRITE)
        .withType(schema)
        .build()) {

      for (SingleRow singleRow : DATA) {
        writer.write(f.newGroup()
            .append(SingleRow.BOOLEAN_FIELD_NAME, singleRow.boolean_field)
            .append(SingleRow.INT32_FIELD_NAME, singleRow.int32_field)
            .append(SingleRow.FLOAT_FIELD_NAME, singleRow.float_field)
            .append(SingleRow.DOUBLE_FIELD_NAME, singleRow.double_field)
            .append(SingleRow.BINARY_FIELD_NAME, Binary.fromConstantByteArray(singleRow.ba_field))
            .append(
                SingleRow.FIXED_LENGTH_BINARY_FIELD_NAME,
                Binary.fromConstantByteArray(singleRow.flba_field))
            .append(SingleRow.PLAINTEXT_INT32_FIELD_NAME, singleRow.plaintext_int32_field));
      }
    }
  }

  @Test
  public void testReadWithoutKeys() throws IOException {
    Configuration readConf = new Configuration();
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), filePath)
        .withConf(readConf)
        .build()) {
      assertThatThrownBy(reader::read)
          .isInstanceOf(ParquetCryptoRuntimeException.class)
          .hasMessageContaining("Trying to read file with encrypted footer. No keys available");
    }
  }

  @Test
  public void testDefaultKmsUrl() throws IOException {
    // Reader with basic decryption properties
    Configuration readConf = basicDecryptionConfig();
    // triggers creation of new kms client instance
    readConf.set(KeyToolkit.KEY_ACCESS_TOKEN_PROPERTY_NAME, "reader1-token");

    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), filePath)
        .withConf(readConf)
        .build()) {
      reader.read();
    }

    // Make sure KMS URL is the default string (not taken from storage).
    assertThat(KmsClient.KMS_INSTANCE_ID_DEFAULT.equals(UnitestUrlReadKMS.getStaticKmsURL()));
  }

  @Test
  public void testStoredKmsUrl() throws IOException {
    Configuration readConf = basicDecryptionConfig();

    // Enable reading KMS URL from storage
    readConf.set(KeyToolkit.KMS_ENABLE_URL_READ_PROPERTY_NAME, "true");
    readConf.set(KeyToolkit.KEY_ACCESS_TOKEN_PROPERTY_NAME, "reader2-token");

    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), filePath)
        .withConf(readConf)
        .build()) {
      reader.read();
    }

    // Verify the stored value
    assertThat(STORED_KMS_URL.equals(UnitestUrlReadKMS.getStaticKmsURL()));
  }

  @Test
  public void testSetKmsUrl() throws IOException {
    Configuration readConf = basicDecryptionConfig();

    // Set KMS URL value in the reader
    String readerSetURL = "reader-set-kms-url";
    readConf.set(KeyToolkit.KMS_INSTANCE_URL_PROPERTY_NAME, readerSetURL);
    readConf.set(KeyToolkit.KEY_ACCESS_TOKEN_PROPERTY_NAME, "reader3-token");

    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), filePath)
        .withConf(readConf)
        .build()) {
      reader.read();
    }

    // Verify the set value
    assertThat(readerSetURL.equals(UnitestUrlReadKMS.getStaticKmsURL()));
  }

  @AfterAll
  public static void deleteFile() throws IOException {
    filePath.getFileSystem(new Configuration()).delete(filePath, false);
  }

  private Configuration basicDecryptionConfig() {
    Configuration readConf = new Configuration();
    readConf.set(
        EncryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME,
        PropertiesDrivenCryptoFactory.class.getName());
    readConf.set(KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME, UnitestUrlReadKMS.class.getName());
    readConf.set(InMemoryKMS.KEY_LIST_PROPERTY_NAME, KEY_LIST);

    return readConf;
  }
}
