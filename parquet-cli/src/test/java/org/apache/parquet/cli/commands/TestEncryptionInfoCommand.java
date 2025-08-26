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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestEncryptionInfoCommand extends ParquetFileTest {

  private static final byte[] FOOTER_KEY = "0123456789012345".getBytes();
  private static final byte[] COLUMN_KEY = "1234567890123450".getBytes();
  private static final String FOOTER_KEY_ID = "kf";
  private static final String COLUMN_KEY_ID = "kc";

  private File unencryptedFile;
  private File columnEncryptedFile;
  private File footerEncryptedFile;

  @Before
  public void setUp() throws IOException {
    super.setUp();
    createTestFiles();
  }

  private void createTestFiles() throws IOException {
    unencryptedFile = createParquetFile("unencrypted.parquet", null);

    FileEncryptionProperties columnEncProps = createColumnEncryptionProperties();
    columnEncryptedFile = createParquetFile("column_encrypted.parquet", columnEncProps);

    FileEncryptionProperties footerEncProps = createFooterEncryptionProperties();
    footerEncryptedFile = createParquetFile("footer_encrypted.parquet", footerEncProps);
  }

  private FileEncryptionProperties createColumnEncryptionProperties() {
    String columnKeyMetadata = "{\"key\":\"" + COLUMN_KEY_ID + "\"}";
    String footerKeyMetadata = "{\"key\":\"" + FOOTER_KEY_ID + "\"}";

    ColumnEncryptionProperties columnProps = ColumnEncryptionProperties.builder("ssn")
        .withKey(COLUMN_KEY)
        .withKeyMetaData(columnKeyMetadata.getBytes(StandardCharsets.UTF_8))
        .build();

    Map<ColumnPath, ColumnEncryptionProperties> columnMap = new HashMap<>();
    columnMap.put(columnProps.getPath(), columnProps);

    return FileEncryptionProperties.builder(FOOTER_KEY)
        .withFooterKeyMetadata(footerKeyMetadata.getBytes(StandardCharsets.UTF_8))
        .withEncryptedColumns(columnMap)
        .build();
  }

  private FileEncryptionProperties createFooterEncryptionProperties() {
    // Use JSON metadata format that PropertiesDrivenCryptoFactory expects
    String footerKeyMetadata = "{\"key\":\"" + FOOTER_KEY_ID + "\"}";

    return FileEncryptionProperties.builder(FOOTER_KEY)
        .withFooterKeyMetadata(footerKeyMetadata.getBytes(StandardCharsets.UTF_8))
        .build();
  }

  private FileDecryptionProperties createDecryptionProperties() {
    DecryptionKeyRetriever keyRetriever = new DecryptionKeyRetriever() {
      @Override
      public byte[] getKey(byte[] keyMetaData) throws KeyAccessDeniedException, ParquetCryptoRuntimeException {
        String keyID = new String(keyMetaData);
        if (FOOTER_KEY_ID.equals(keyID)) {
          return FOOTER_KEY;
        }
        if (COLUMN_KEY_ID.equals(keyID)) {
          return COLUMN_KEY;
        }
        return null;
      }
    };

    return FileDecryptionProperties.builder().withKeyRetriever(keyRetriever).build();
  }

  private File createParquetFile(String filename, FileEncryptionProperties encryptionProps) throws IOException {
    File file = new File(getTempFolder(), filename);
    Path fsPath = new Path(file.getPath());
    Configuration conf = new Configuration();

    MessageType schema = Types.buildMessage()
        .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32)
        .named("id")
        .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
        .named("ssn")
        .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
        .named("name")
        .named("schema");

    SimpleGroupFactory fact = new SimpleGroupFactory(schema);
    GroupWriteSupport.setSchema(schema, conf);

    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(fsPath)
        .withConf(conf)
        .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withRowGroupSize(1024)
        .withPageSize(1024)
        .withDictionaryPageSize(512)
        .withDictionaryEncoding(true)
        .withValidation(false)
        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0);

    if (encryptionProps != null) {
      builder.withEncryption(encryptionProps);
    }

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

  private static void enableCliDecryptionKeys() {
    System.setProperty(
        "parquet.crypto.factory.class", "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory");
    System.setProperty(
        "parquet.encryption.key.list",
        FOOTER_KEY_ID + ":" + bytesToHex(FOOTER_KEY) + "," + COLUMN_KEY_ID + ":" + bytesToHex(COLUMN_KEY));
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  @Test
  public void testUnencryptedFile() throws IOException {
    EncryptionInfoCommand command = new EncryptionInfoCommand(createLogger());
    command.targets = java.util.Arrays.asList(unencryptedFile.getAbsolutePath());
    command.setConf(new Configuration());

    int result = command.run();
    assertEquals(0, result);
  }

  @Test
  public void testColumnEncryptedFile() throws IOException {
    EncryptionInfoCommand cmd = new EncryptionInfoCommand(createLogger());
    cmd.targets = java.util.Arrays.asList(columnEncryptedFile.getAbsolutePath());
    cmd.setConf(new Configuration());

    try {
      cmd.run();
      fail("Expected ParquetCryptoRuntimeException for encrypted file without keys");
    } catch (org.apache.parquet.crypto.ParquetCryptoRuntimeException e) {
      assertTrue(e.getMessage().contains("encrypted") || e.getMessage().contains("key"));
    }
  }

  @Test
  public void testFooterEncryptedFile() throws IOException {
    EncryptionInfoCommand cmd = new EncryptionInfoCommand(createLogger());
    cmd.targets = java.util.Arrays.asList(footerEncryptedFile.getAbsolutePath());
    cmd.setConf(new Configuration());

    try {
      cmd.run();
      fail("Expected ParquetCryptoRuntimeException for encrypted file without keys");
    } catch (org.apache.parquet.crypto.ParquetCryptoRuntimeException e) {
      assertTrue(e.getMessage().contains("encrypted") || e.getMessage().contains("key"));
    }
  }

  @Test
  public void testCommandWorksWithRealFile() throws IOException {
    File file = parquetFile();
    EncryptionInfoCommand command = new EncryptionInfoCommand(createLogger());
    command.targets = java.util.Arrays.asList(file.getAbsolutePath());
    command.setConf(new Configuration());

    int result = command.run();
    assertEquals(0, result);
  }

  @Test
  public void testCommandExists() {
    EncryptionInfoCommand cmd = new EncryptionInfoCommand(LoggerFactory.getLogger(TestEncryptionInfoCommand.class));
  }

  @Test
  public void testColumnEncryptedFileWithKeys() throws IOException {
    assertTrue(columnEncryptedFile.exists());

    enableCliDecryptionKeys();

    EncryptionInfoCommand cmd = new EncryptionInfoCommand(createLogger());
    cmd.targets = java.util.Arrays.asList(columnEncryptedFile.getAbsolutePath());
    cmd.setConf(new Configuration());

    int rc = cmd.run();
    assertEquals(0, rc);

    FileDecryptionProperties decryptionProps = createDecryptionProperties();
    ParquetReadOptions options = ParquetReadOptions.builder().withDecryption(decryptionProps).build();
    InputFile inputFile = HadoopInputFile.fromPath(new Path(columnEncryptedFile.getAbsolutePath()), new Configuration());
    ParquetMetadata footer;
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile, options)) {
      footer = reader.getFooter();
    }
    Set<String> encrypted = EncryptionInfoCommand.findEncryptedColumns(footer);
    assertEquals(Collections.singleton("ssn"), encrypted);
  }

  @Test
  public void testFooterEncryptedFileWithKeys() throws IOException {
    FileDecryptionProperties decryptionProps = createDecryptionProperties();
    Configuration verifyConf = new Configuration();

    ParquetReadOptions readOptions =
        ParquetReadOptions.builder().withDecryption(decryptionProps).build();
    InputFile inputFile = HadoopInputFile.fromPath(new Path(footerEncryptedFile.getPath()), verifyConf);

    try {
      ParquetFileReader.open(inputFile, readOptions);
      fail("Expected ParquetCryptoRuntimeException for footer encrypted file without keys");
    } catch (ParquetCryptoRuntimeException e) {
    }

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    enableCliDecryptionKeys();
    Logger logger = createLogger(new PrintStream(out));
    EncryptionInfoCommand cmd = new EncryptionInfoCommand(logger);
    cmd.targets = java.util.Arrays.asList(footerEncryptedFile.getAbsolutePath());
    cmd.setConf(new Configuration());

    int rc = cmd.run();
    assertEquals(0, rc);
    String output = out.toString();
    assertTrue(output.contains("Encryption type: ENCRYPTED_FOOTER"));
    assertTrue(output.contains("Footer is encrypted"));
  }
}
