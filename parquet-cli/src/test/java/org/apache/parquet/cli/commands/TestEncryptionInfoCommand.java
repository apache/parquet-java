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
package org.apache.parquet.cli.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.EncryptionPropertiesFactory;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * This test checks the CLI functionality for encryption info.
 */
public class TestEncryptionInfoCommand extends ParquetFileTest {

  private static final byte[] FOOTER_KEY = "0123456789012345".getBytes(StandardCharsets.UTF_8);
  private static final byte[] COLUMN_KEY = "1234567890123450".getBytes(StandardCharsets.UTF_8);

  private static final String FOOTER_KEY_ID = "kf";
  private static final String COLUMN_KEY_ID = "kc";

  private static final String FACTORY_CLASS = "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory";
  private static final String MOCK_KMS_CLASS = "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS";

  private File unencryptedFile;
  private File columnEncryptedFile;
  private File footerEncryptedFile;

  private enum EncMode {
    NONE, // No encryption
    COLUMN_AND_FOOTER, // Both column and footer encrypted
    COLUMN_PLAINTEXT_FOOTER // Column encrypted, footer plaintext (legacy compatible)
  }

  @Before
  public void setUp() throws IOException {
    super.setUp();
    createTestFiles();
  }

  private void createTestFiles() throws IOException {
    unencryptedFile = createParquetFile("unencrypted.parquet", EncMode.NONE);
    columnEncryptedFile = createParquetFile("column_encrypted.parquet", EncMode.COLUMN_AND_FOOTER);
    footerEncryptedFile = createParquetFile("footer_encrypted.parquet", EncMode.COLUMN_PLAINTEXT_FOOTER);
  }

  private File createParquetFile(String filename, EncMode mode) throws IOException {
    File file = new File(getTempFolder(), filename);
    Path fsPath = new Path(file.getPath());

    Configuration conf = getHadoopConfiguration(mode);
    FileEncryptionProperties fileEncryptionProperties = null;

    try {
      if (null == conf) {
        conf = new Configuration();
      } else {
        EncryptionPropertiesFactory cryptoFactory = EncryptionPropertiesFactory.loadFactory(conf);
        fileEncryptionProperties = cryptoFactory.getFileEncryptionProperties(conf, fsPath, null);
      }
    } catch (Exception e) {
      throw new IOException("Failed to create encryption properties for " + filename, e);
    }

    MessageType schema = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.INT32)
        .named("id")
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("ssn")
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("name")
        .named("schema");

    SimpleGroupFactory fact = new SimpleGroupFactory(schema);
    GroupWriteSupport.setSchema(schema, conf);

    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(fsPath)
        .withConf(conf)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withRowGroupSize(1024)
        .withPageSize(1024)
        .withDictionaryPageSize(512)
        .withDictionaryEncoding(true)
        .withValidation(false)
        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
        .withEncryption(fileEncryptionProperties);

    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < 5; i++) {
        writer.write(fact.newGroup()
            .append("id", i)
            .append("ssn", "123-45-" + String.format("%04d", i))
            .append("name", "User" + i));
      }
    }

    return file;
  }

  private Configuration getHadoopConfiguration(EncMode mode) {
    if (mode == EncMode.NONE) {
      return null;
    }

    Configuration conf = new Configuration();

    conf.set("parquet.crypto.factory.class", FACTORY_CLASS);
    conf.set("parquet.encryption.kms.client.class", MOCK_KMS_CLASS);
    conf.set(
        "parquet.encryption.key.list",
        FOOTER_KEY_ID + ":" + bytesToBase64(FOOTER_KEY) + ", " + COLUMN_KEY_ID + ":"
            + bytesToBase64(COLUMN_KEY));

    if (mode == EncMode.COLUMN_AND_FOOTER) {
      conf.set(PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME, COLUMN_KEY_ID + ":ssn");
      conf.set(PropertiesDrivenCryptoFactory.FOOTER_KEY_PROPERTY_NAME, FOOTER_KEY_ID);
    } else if (mode == EncMode.COLUMN_PLAINTEXT_FOOTER) {
      conf.set(PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME, COLUMN_KEY_ID + ":ssn");
      conf.set(PropertiesDrivenCryptoFactory.FOOTER_KEY_PROPERTY_NAME, FOOTER_KEY_ID);
      conf.setBoolean(PropertiesDrivenCryptoFactory.PLAINTEXT_FOOTER_PROPERTY_NAME, true);
    }

    return conf;
  }

  private Configuration getDecryptionConfiguration() {
    Configuration conf = new Configuration();
    conf.set("parquet.crypto.factory.class", FACTORY_CLASS);
    conf.set("parquet.encryption.kms.client.class", MOCK_KMS_CLASS);
    conf.set(
        "parquet.encryption.key.list",
        FOOTER_KEY_ID + ":" + bytesToBase64(FOOTER_KEY) + ", " + COLUMN_KEY_ID + ":"
            + bytesToBase64(COLUMN_KEY));
    return conf;
  }

  private static String bytesToBase64(byte[] bytes) {
    return Base64.getEncoder().encodeToString(bytes);
  }

  @Test
  public void testUnencryptedFile() throws IOException {
    EncryptionInfoCommand command = new EncryptionInfoCommand(createLogger());
    command.targets = Arrays.asList(unencryptedFile.getAbsolutePath());
    command.setConf(new Configuration());

    int result = command.run();
    assertEquals(0, result);
  }

  @Test
  public void testColumnEncryptedFile() throws IOException {
    EncryptionInfoCommand cmd = new EncryptionInfoCommand(createLogger());
    cmd.targets = Arrays.asList(columnEncryptedFile.getAbsolutePath());
    cmd.setConf(new Configuration());

    try {
      cmd.run();
      fail("Expected ParquetCryptoRuntimeException for encrypted file without keys");
    } catch (ParquetCryptoRuntimeException e) {
      assertTrue(e.getMessage().toLowerCase().contains("encrypted")
          || e.getMessage().toLowerCase().contains("key"));
    }
  }

  @Test
  public void testFooterEncryptedFile() throws IOException {
    EncryptionInfoCommand cmd = new EncryptionInfoCommand(createLogger());
    cmd.targets = Arrays.asList(footerEncryptedFile.getAbsolutePath());
    cmd.setConf(getDecryptionConfiguration());

    int result = cmd.run();
    assertEquals(0, result);
  }

  @Test
  public void testCommandWorksWithRealFile() throws IOException {
    File file = parquetFile();
    EncryptionInfoCommand command = new EncryptionInfoCommand(createLogger());
    command.targets = Arrays.asList(file.getAbsolutePath());
    command.setConf(new Configuration());

    int result = command.run();
    assertEquals(0, result);
  }

  @Test
  public void testCommandExists() {
    new EncryptionInfoCommand(LoggerFactory.getLogger(TestEncryptionInfoCommand.class));
  }

  @Test
  public void testColumnEncryptedFileWithKeys() throws IOException {
    assertTrue(columnEncryptedFile.exists());

    EncryptionInfoCommand cmd = new EncryptionInfoCommand(createLogger());
    cmd.targets = Arrays.asList(columnEncryptedFile.getAbsolutePath());
    cmd.setConf(getDecryptionConfiguration());
    int rc = cmd.run();
    assertEquals(0, rc);

    Configuration hadoopConfig = getDecryptionConfiguration();
    FileDecryptionProperties fileDecryptionProperties = null;

    DecryptionPropertiesFactory cryptoFactory = DecryptionPropertiesFactory.loadFactory(hadoopConfig);
    fileDecryptionProperties = cryptoFactory.getFileDecryptionProperties(
        hadoopConfig, new Path(columnEncryptedFile.getAbsolutePath()));

    InputFile inputFile = HadoopInputFile.fromPath(new Path(columnEncryptedFile.getAbsolutePath()), hadoopConfig);
    ParquetMetadata footer;
    try (ParquetFileReader reader = ParquetFileReader.open(
        inputFile,
        ParquetReadOptions.builder()
            .withDecryption(fileDecryptionProperties)
            .build())) {
      footer = reader.getFooter();
    }
    Set<String> encrypted = EncryptionInfoCommand.findEncryptedColumns(footer);
    assertEquals(Collections.singleton("ssn"), encrypted);
  }

  @Test
  public void testFooterEncryptedFileWithKeys() throws IOException {
    withLogger((logger, events) -> {
      EncryptionInfoCommand cmd = new EncryptionInfoCommand(logger);
      cmd.targets = Arrays.asList(footerEncryptedFile.getAbsolutePath());
      cmd.setConf(getDecryptionConfiguration());

      int rc = cmd.run();
      assertEquals(0, rc);

      boolean foundColumnSummary = false;
      boolean foundSsn = false;

      for (org.slf4j.event.LoggingEvent event : events) {
        String message = event.getMessage();
        if (message.contains("Column encryption summary:")) {
          foundColumnSummary = true;
        }
        if (message.contains("ssn")) {
          foundSsn = true;
        }
      }

      assertTrue("Should contain column encryption summary", foundColumnSummary);
      assertTrue("Should contain ssn column", foundSsn);
    });
  }
}
