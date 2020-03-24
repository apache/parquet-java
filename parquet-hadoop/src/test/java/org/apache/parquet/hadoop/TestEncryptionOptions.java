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
package org.apache.parquet.hadoop;


import static org.junit.Assert.assertEquals;
import static org.apache.parquet.hadoop.TestUtils.enforceEmptyDir;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.*;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TemporaryFolder;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;


import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;


/*
 * This file contains samples for writing and reading encrypted Parquet files in different
 * encryption and decryption configurations. The samples have the following goals:
 * 1) Demonstrate usage of different options for data encryption and decryption.
 * 2) Produce encrypted files for interoperability tests with other (eg parquet-cpp)
 *    readers that support encryption.
 * 3) Produce encrypted files with plaintext footer, for testing the ability of legacy
 *    readers to parse the footer and read unencrypted columns.
 * 4) Perform interoperability tests with other (eg parquet-cpp) writers, by reading
 *    encrypted files produced by these writers.
 *
 * The write sample produces number of parquet files, each encrypted with a different
 * encryption configuration as described below.
 * The name of each file is in the form of:
 * tester<encryption config number>.parquet.encrypted.
 *
 * The read sample creates a set of decryption configurations and then uses each of them
 * to read all encrypted files in the input directory.
 *
 * The different encryption and decryption configurations are listed below.
 *
 *
 * A detailed description of the Parquet Modular Encryption specification can be found
 * here:
 * https://github.com/apache/parquet-format/blob/encryption/Encryption.md
 *
 * The write sample creates files with eight columns in the following
 * encryption configurations:
 *
 *  - Encryption configuration 1:   Encrypt all columns and the footer with the same key.
 *                                  (uniform encryption)
 *  - Encryption configuration 2:   Encrypt two columns and the footer, with different
 *                                  keys.
 *  - Encryption configuration 3:   Encrypt two columns, with different keys.
 *                                  Don?t encrypt footer (to enable legacy readers)
 *                                  - plaintext footer mode.
 *  - Encryption configuration 4:   Encrypt two columns and the footer, with different
 *                                  keys. Supply aad_prefix for file identity
 *                                  verification.
 *  - Encryption configuration 5:   Encrypt two columns and the footer, with different
 *                                  keys. Supply aad_prefix, and call
 *                                  disable_aad_prefix_storage to prevent file
 *                                  identity storage in file metadata.
 *  - Encryption configuration 6:   Encrypt two columns and the footer, with different
 *                                  keys. Use the alternative (AES_GCM_CTR_V1) algorithm.
 *  - Encryption configuration 7:   Do not encrypt anything
 *
 *
 * The read sample uses each of the following decryption configurations to read every
 * encrypted files in the input directory:
 *
 *  - Decryption configuration 1:   Decrypt using key retriever that holds the keys of
 *                                  two encrypted columns and the footer key.
 *  - Decryption configuration 2:   Decrypt using key retriever that holds the keys of
 *                                  two encrypted columns and the footer key. Supplies
 *                                  aad_prefix to verify file identity.
 *  - Decryption configuration 3:   Decrypt using explicit column and footer keys
 *                                  (instead of key retrieval callback).
 *  - Decryption configuration 4:   Do not decrypt anything*
 */
public class TestEncryptionOptions {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  byte[] FOOTER_ENCRYPTION_KEY = new String("0123456789012345").getBytes();
  byte[] COLUMN_ENCRYPTION_KEY1 = new String("1234567890123450").getBytes();
  byte[] COLUMN_ENCRYPTION_KEY2 = new String("1234567890123451").getBytes();
  String fileName = "tester";

  @Test
  public void testInterop() throws IOException {
    Path rootPath = new Path(temporaryFolder.getRoot().getPath());
    interopTestWriteEncryptedParquetFiles(rootPath);
    interopTestReadEncryptedParquetFiles(rootPath);
  }

  private void interopTestWriteEncryptedParquetFiles(Path root) throws IOException {
    byte[] AADPrefix = root.getName().getBytes(StandardCharsets.UTF_8);
    Configuration conf = new Configuration();
    /**********************************************************************************
     Creating a number of Encryption configurations
     **********************************************************************************/

    // This array will hold various encryption configuraions.
    int numberOfEncryptionModes = 7;
    FileEncryptionProperties[] encryptionPropertiesList = getEncryptionConfigurations(AADPrefix, numberOfEncryptionModes);


    MessageType schema = parseMessageType(
      "message test { "
        + "required boolean boolean_field; "
        + "required int32 int32_field; "
        + "required float float_field; "
        + "required double double_field; "
        + "} ");

    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);


    for (int encryptionMode = 0; encryptionMode < numberOfEncryptionModes; encryptionMode++) {
      int mode = encryptionMode + 1;
      Path file = new Path(root, fileName + mode + ".parquet.encrypted");

      System.out.println("\nWrite " + file.toString());
      ParquetWriter<Group> writer = new ParquetWriter<Group>(
        file,
        new GroupWriteSupport(),
        UNCOMPRESSED, 1024, 1024, 512, true, false,
        ParquetWriter.DEFAULT_WRITER_VERSION, conf,
        encryptionPropertiesList[encryptionMode]);

      for (int i = 0; i < 100; i++) {
        boolean expect = false;
        if ((i % 2) == 0)
          expect = true;
        float float_val = (float) i * 1.1f;
        double double_val = (i * 1.1111111);

        writer.write(
          f.newGroup()
            .append("boolean_field", expect)
            .append("int32_field", i)
            .append("float_field", float_val)
            .append("double_field", double_val));

      }
      writer.close();
    }
  }

  private void interopTestReadEncryptedParquetFiles(Path root) throws IOException {
    byte[] AADPrefix = root.getName().getBytes(StandardCharsets.UTF_8);
    Configuration conf = new Configuration();
    /**********************************************************************************
     Creating a number of Decryption configurations
     **********************************************************************************/

    // This array will hold various decryption configurations.
    int numberOfDecryptionModes = 4;
    FileDecryptionProperties[] decryptionPropertiesList = getDecryptionConfigurations(AADPrefix, numberOfDecryptionModes);

    for (int decryptionMode = 0; decryptionMode < numberOfDecryptionModes; decryptionMode++) {
//      PrintDecryptionConfiguration(decryptionMode + 1);
      System.out.println("==> Decryption configuration " + (decryptionMode + 1));
      FileDecryptionProperties fileDecryptionProperties = decryptionPropertiesList[decryptionMode];

      File folder = new File(root.toString());
      File[] listOfFiles = folder.listFiles();

      for (int fileNum = 0; fileNum < listOfFiles.length; fileNum++) {
        Path file = new Path(root, listOfFiles[fileNum].toString());
        if (!file.toString().endsWith("parquet.encrypted")) { // Skip non encrypted files
          continue;
        }
        System.out.println("--> Read file " + file.toString());
        ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).
          withDecryption(fileDecryptionProperties).
          withConf(conf).build();
        try {
          for (int i = 0; i < 500; i++) {
            Group group = null;
            group = reader.read();
            boolean expect = false;
            if ((i % 2) == 0)
              expect = true;
            boolean bool_res = group.getBoolean("boolean_field", 0);
            if (bool_res != expect)
              System.out.println("Wrong bool");
            int int_res = group.getInteger("int32_field", 0);
            if (int_res != i)
              System.out.println("Wrong int");
            float float_res = group.getFloat("float_field", 0);
            float tmp1 = (float) i * 1.1f;
            if (float_res != tmp1) System.out.println("Wrong float");

            double double_res = group.getDouble("double_field", 0);
            double tmp = (i * 1.1111111);
            if (double_res != tmp)
              System.out.println("Wrong double");
          }
        } catch (Exception e) {
          String errorMessage = e.getMessage();
          checkResult(file.toString(), decryptionMode, (null == errorMessage ? "" : errorMessage));
        }
      }
    }
  }

  private FileEncryptionProperties[] getEncryptionConfigurations(byte[] AADPrefix, int numberOfEncryptionModes) {
    FileEncryptionProperties[] encryptionPropertiesList = new FileEncryptionProperties[numberOfEncryptionModes];

    // Encryption configuration 1: Encrypt all columns and the footer with the same key.
    // (uniform encryption)
    String footerKeyName = "kf";

    byte[] footerKeyMetadata = footerKeyName.getBytes(StandardCharsets.UTF_8);
    // Add to list of encryption configurations.
    encryptionPropertiesList[0] = FileEncryptionProperties.builder(FOOTER_ENCRYPTION_KEY)
      .withFooterKeyMetadata(footerKeyMetadata).build();


    // Encryption configuration 2: Encrypt two columns and the footer, with different keys.
    ColumnEncryptionProperties columnProperties20 = ColumnEncryptionProperties
      .builder("double_field")
      .withKey(COLUMN_ENCRYPTION_KEY1)
      .withKeyID("kc1")
      .build();

    ColumnEncryptionProperties columnProperties21 = ColumnEncryptionProperties
      .builder("float_field")
      .withKey(COLUMN_ENCRYPTION_KEY2)
      .withKeyID("kc2")
      .build();
    Map<ColumnPath, ColumnEncryptionProperties> columnPropertiesMap2 = new HashMap<>();

    columnPropertiesMap2.put(columnProperties20.getPath(), columnProperties20);
    columnPropertiesMap2.put(columnProperties21.getPath(), columnProperties21);

    encryptionPropertiesList[1] = FileEncryptionProperties.builder(FOOTER_ENCRYPTION_KEY)
      .withFooterKeyMetadata(footerKeyMetadata)
      .withEncryptedColumns(columnPropertiesMap2)
      .build();

    // Encryption configuration 3: Encrypt two columns, with different keys.
    // Don't encrypt footer.
    // (plaintext footer mode, readable by legacy readers)
    Map<ColumnPath, ColumnEncryptionProperties> columnPropertiesMap3 = new HashMap<>();
    ColumnEncryptionProperties columnProperties30 = ColumnEncryptionProperties
      .builder("double_field")
      .withKey(COLUMN_ENCRYPTION_KEY1)
      .withKeyID("kc1")
      .build();

    ColumnEncryptionProperties columnProperties31 = ColumnEncryptionProperties
      .builder("float_field")
      .withKey(COLUMN_ENCRYPTION_KEY2)
      .withKeyID("kc2")
      .build();
    columnPropertiesMap3.put(columnProperties30.getPath(), columnProperties30);
    columnPropertiesMap3.put(columnProperties31.getPath(), columnProperties31);

    encryptionPropertiesList[2] = FileEncryptionProperties.builder(FOOTER_ENCRYPTION_KEY)
      .withFooterKeyMetadata(footerKeyMetadata)
      .withEncryptedColumns(columnPropertiesMap3)
      .withPlaintextFooter()
      .build();

    // Encryption configuration 4: Encrypt two columns and the footer, with different keys.
    // Use aad_prefix.
    Map<ColumnPath, ColumnEncryptionProperties> columnPropertiesMap4 = new HashMap<>();
    ColumnEncryptionProperties columnProperties40 = ColumnEncryptionProperties
      .builder("double_field")
      .withKey(COLUMN_ENCRYPTION_KEY1)
      .withKeyID("kc1")
      .build();

    ColumnEncryptionProperties columnProperties41 = ColumnEncryptionProperties
      .builder("float_field")
      .withKey(COLUMN_ENCRYPTION_KEY2)
      .withKeyID("kc2")
      .build();
    columnPropertiesMap4.put(columnProperties40.getPath(), columnProperties40);
    columnPropertiesMap4.put(columnProperties41.getPath(), columnProperties41);

    encryptionPropertiesList[3] = FileEncryptionProperties.builder(FOOTER_ENCRYPTION_KEY)
      .withFooterKeyMetadata(footerKeyMetadata)
      .withEncryptedColumns(columnPropertiesMap4)
      .withAADPrefix(AADPrefix)
      .build();

    // Encryption configuration 5: Encrypt two columns and the footer, with different keys.
    // Use aad_prefix and disable_aad_prefix_storage.
    Map<ColumnPath, ColumnEncryptionProperties> columnPropertiesMap5 = new HashMap<>();
    ColumnEncryptionProperties columnProperties50 = ColumnEncryptionProperties
      .builder("double_field")
      .withKey(COLUMN_ENCRYPTION_KEY1)
      .withKeyID("kc1")
      .build();

    ColumnEncryptionProperties columnProperties51 = ColumnEncryptionProperties
      .builder("float_field")
      .withKey(COLUMN_ENCRYPTION_KEY2)
      .withKeyID("kc2")
      .build();
    columnPropertiesMap5.put(columnProperties50.getPath(), columnProperties50);
    columnPropertiesMap5.put(columnProperties51.getPath(), columnProperties51);

    encryptionPropertiesList[4] = FileEncryptionProperties.builder(FOOTER_ENCRYPTION_KEY)
      .withFooterKeyMetadata(footerKeyMetadata)
      .withEncryptedColumns(columnPropertiesMap5)
      .withAADPrefix(AADPrefix)
      .withoutAADPrefixStorage()
      .build();

    // Encryption configuration 6: Encrypt two columns and the footer, with different keys.
    // Use AES_GCM_CTR_V1 algorithm.
    Map<ColumnPath, ColumnEncryptionProperties> columnPropertiesMap6 = new HashMap<>();
    ColumnEncryptionProperties columnProperties60 = ColumnEncryptionProperties
      .builder("double_field")
      .withKey(COLUMN_ENCRYPTION_KEY1)
      .withKeyID("kc1")
      .build();

    ColumnEncryptionProperties columnProperties61 = ColumnEncryptionProperties
      .builder("float_field")
      .withKey(COLUMN_ENCRYPTION_KEY2)
      .withKeyID("kc2")
      .build();
    columnPropertiesMap6.put(columnProperties50.getPath(), columnProperties60);
    columnPropertiesMap6.put(columnProperties51.getPath(), columnProperties61);

    encryptionPropertiesList[5] = FileEncryptionProperties.builder(FOOTER_ENCRYPTION_KEY)
      .withFooterKeyMetadata(footerKeyMetadata)
      .withEncryptedColumns(columnPropertiesMap6)
      .withAlgorithm(ParquetCipher.AES_GCM_CTR_V1)
      .build();

    // Encryption configuration 7: Do not encrypt anything
    encryptionPropertiesList[6] = null;
    return encryptionPropertiesList;
  }


  private FileDecryptionProperties[] getDecryptionConfigurations(byte[] AADPrefix, int numberOfDecryptionModes) {
    FileDecryptionProperties[] decryptionPropertiesList = new FileDecryptionProperties[numberOfDecryptionModes];

    // Decryption configuration 1: Decrypt using key retriever callback that holds the keys
    // of two encrypted columns and the footer key.
    StringKeyIdRetriever kr1 = new StringKeyIdRetriever();
    kr1.putKey("kf", FOOTER_ENCRYPTION_KEY);
    kr1.putKey("kc1", COLUMN_ENCRYPTION_KEY1);
    kr1.putKey("kc2", COLUMN_ENCRYPTION_KEY2);

    decryptionPropertiesList[0] = FileDecryptionProperties.builder()
      .withKeyRetriever(kr1)
      .build();

    // Decryption configuration 2: Decrypt using key retriever callback that holds the keys
    // of two encrypted columns and the footer key. Supply aad_prefix.
    StringKeyIdRetriever kr2 = new StringKeyIdRetriever();
    kr2.putKey("kf", FOOTER_ENCRYPTION_KEY);
    kr2.putKey("kc1", COLUMN_ENCRYPTION_KEY1);
    kr2.putKey("kc2", COLUMN_ENCRYPTION_KEY2);

    decryptionPropertiesList[1] = FileDecryptionProperties.builder()
      .withKeyRetriever(kr2)
      .withAADPrefix(AADPrefix)
      .build();

    // Decryption configuration 3: Decrypt using explicit column and footer keys. Supply
    // aad_prefix.
    Map<ColumnPath, ColumnDecryptionProperties> columnMap = new HashMap<>();
    ColumnDecryptionProperties columnDecryptionProps0 = ColumnDecryptionProperties
      .builder("double_field")
      .withKey(COLUMN_ENCRYPTION_KEY1)
      .build();

    ColumnDecryptionProperties columnDecryptionProps1 = ColumnDecryptionProperties
      .builder("float_field")
      .withKey(COLUMN_ENCRYPTION_KEY2)
      .build();

    columnMap.put(columnDecryptionProps0.getPath(), columnDecryptionProps0);
    columnMap.put(columnDecryptionProps1.getPath(), columnDecryptionProps1);

    decryptionPropertiesList[2] = FileDecryptionProperties.builder().withColumnKeys(columnMap).
      withFooterKey(FOOTER_ENCRYPTION_KEY).build();

    // Decryption configuration 4: Do not decrypt anything.
    decryptionPropertiesList[3] = null;
    return decryptionPropertiesList;
  }


  // Check that the decryption result is as expected.
  private void checkResult(String file, int exampleId, String exceptionMsg) {
    int encryptionConfigurationNumber = -1;
    // Extract encryptionConfigurationNumber from the parquet file name.
    Pattern p = Pattern.compile("tester([0-9]+)\\.parquet.encrypted");
    Matcher m = p.matcher(file);

    if (m.find()) {
      encryptionConfigurationNumber = Integer.parseInt(m.group(1));
    } else {
      Assert.fail("Error: Error parsing filename to extract encryption configuration number. ");
    }
    int decryptionConfigurationNumber = exampleId + 1;

    // Encryption_configuration 5 contains aad_prefix and
    // disable_aad_prefix_storage.
    // An exception is expected to be thrown if the file is not decrypted with aad_prefix.
    if (encryptionConfigurationNumber == 5) {
      if (decryptionConfigurationNumber == 1 || decryptionConfigurationNumber == 3) {
        if (!exceptionMsg.contains("AAD")) {
          errorCollector.addError(new Exception(String.format("E%d D%d Error: Expecting AAD related exception, but got [%s]",
            encryptionConfigurationNumber, decryptionConfigurationNumber, exceptionMsg)));
        }
        return;
      }
    }
    // Decryption configuration 2 contains aad_prefix. An exception is expected to
    // be thrown if the file was not encrypted with the same aad_prefix.
    if (decryptionConfigurationNumber == 2) {
      if (encryptionConfigurationNumber != 5 && encryptionConfigurationNumber != 4 && encryptionConfigurationNumber != 7) {
        if (!exceptionMsg.contains("AAD")) {
          errorCollector.addError(new Exception(String.format("E%d D%d Error: Expecting AAD related exception, but got [%s]",
            encryptionConfigurationNumber, decryptionConfigurationNumber, exceptionMsg)));
        }
        return;
      }
    }
    // Encryption_configuration 7 has null encryptor, so parquet is plaintext.
    // An exception is expected to be thrown if the file is being decrypted.
    if (encryptionConfigurationNumber == 7) {
      if ((0 <= decryptionConfigurationNumber) && (decryptionConfigurationNumber <= 3)) {
        if (!exceptionMsg.endsWith("Applying decryptor on plaintext file")) {
          errorCollector.addError(new Exception(String.format("E%d D%d Error: Expecting exception Applying decryptor on plaintext file, but got [%s]",
            encryptionConfigurationNumber, decryptionConfigurationNumber, exceptionMsg)));
        }
        return;
      }
    }
    // Decryption configuration 4 is null, so only plaintext file can be read. An exception is expected to
    // be thrown if the file is encrypted.
    if (decryptionConfigurationNumber == 4) {
      if (encryptionConfigurationNumber != 7) {
        if (!exceptionMsg.endsWith("No keys available") && !exceptionMsg.endsWith("Null File Decryptor")) {
          errorCollector.addError(new Exception(String.format("E%d D%d Error: Expecting No keys available exception, but got [%s]",
            encryptionConfigurationNumber, decryptionConfigurationNumber, exceptionMsg)));
        }
        return;
      }
    }
    if (null != exceptionMsg && !exceptionMsg.equals("")) {
      errorCollector.addError(new Exception(String.format("E%d D%d Error: Unexpected exception was thrown: " + exceptionMsg,
        encryptionConfigurationNumber, decryptionConfigurationNumber)));
    }
  }

//  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();
    Path root = new Path(temporaryFolder.getRoot().getPath());
    enforceEmptyDir(conf, root);

    Random random = new Random();
    int numberOfEncryptionModes = 5;
    FileEncryptionProperties[] encryptionPropertiesList = new FileEncryptionProperties[numberOfEncryptionModes];
    FileDecryptionProperties[] decryptionPropertiesList = new FileDecryptionProperties[numberOfEncryptionModes];

    // #0 Unencrypted - make sure null encryption properties don't break regular Parquet
    encryptionPropertiesList[0] = null;
    decryptionPropertiesList[0] = null;

    // #1 Basic encryption setup
    byte[] encryptionKey = new byte[16];
    random.nextBytes(encryptionKey);
    FileEncryptionProperties encryptionProperties = FileEncryptionProperties.builder(encryptionKey).build();
    FileDecryptionProperties decryptionProperties = FileDecryptionProperties.builder().withFooterKey(encryptionKey).build();
    encryptionPropertiesList[1] = encryptionProperties;
    decryptionPropertiesList[1] = decryptionProperties;

    // #2 Default algorithm, non-uniform encryption, key metadata, key retriever, AAD prefix
    byte[] footerKey = new byte[16];
    random.nextBytes(footerKey);
    byte[] columnKey0 = new byte[16];
    random.nextBytes(columnKey0);
    byte[] columnKey1 = new byte[16];
    random.nextBytes(columnKey1);
    ColumnEncryptionProperties columnProperties0 = ColumnEncryptionProperties.builder("binary_field")
      .withKey(columnKey0)
      .withKeyID("ck0")
      .build();
    ColumnEncryptionProperties columnProperties1 = ColumnEncryptionProperties.builder("int32_field")
      .withKey(columnKey1)
      .withKeyID("ck1")
      .build();
    HashMap<ColumnPath, ColumnEncryptionProperties> columnPropertiesMap = new HashMap<ColumnPath, ColumnEncryptionProperties>();
    columnPropertiesMap.put(columnProperties0.getPath(), columnProperties0);
    columnPropertiesMap.put(columnProperties1.getPath(), columnProperties1);
    byte[] AADPrefix = root.getName().getBytes(StandardCharsets.UTF_8);
    encryptionProperties = FileEncryptionProperties.builder(footerKey)
      .withFooterKeyID("fk")
      .withAADPrefix(AADPrefix)
      .withEncryptedColumns(columnPropertiesMap)
      .build();
    StringKeyIdRetriever keyRetriever = new StringKeyIdRetriever();
    keyRetriever.putKey("fk", footerKey);
    keyRetriever.putKey("ck0", columnKey0);
    keyRetriever.putKey("ck1", columnKey1);
    decryptionProperties = FileDecryptionProperties.builder()
      .withKeyRetriever(keyRetriever)
      .build();
    encryptionPropertiesList[2] = encryptionProperties;
    decryptionPropertiesList[2] = decryptionProperties;

    // #3 GCM_CTR algorithm, non-uniform encryption, key metadata, key retriever, AAD
    columnProperties0 = ColumnEncryptionProperties.builder("binary_field")
      .withKey(columnKey0)
      .withKeyID("ck0")
      .build();
    columnProperties1 = ColumnEncryptionProperties.builder("int32_field")
      .withKey(columnKey1)
      .withKeyID("ck1")
      .build();
    columnPropertiesMap = new HashMap<ColumnPath, ColumnEncryptionProperties>();
    columnPropertiesMap.put(columnProperties0.getPath(), columnProperties0);
    columnPropertiesMap.put(columnProperties1.getPath(), columnProperties1);
    encryptionProperties = FileEncryptionProperties.builder(footerKey)
      .withAlgorithm(ParquetCipher.AES_GCM_CTR_V1)
      .withFooterKeyID("fk")
      .withAADPrefix(AADPrefix)
      .withEncryptedColumns(columnPropertiesMap)
      .build();
    encryptionPropertiesList[3] = encryptionProperties;
    decryptionPropertiesList[3] = decryptionProperties; // Same decryption properties

    // #4  Plaintext footer, default algorithm, key metadata, key retriever, AAD
    columnProperties0 = ColumnEncryptionProperties.builder("binary_field")
      .withKey(columnKey0)
      .withKeyID("ck0")
      .build();
    columnProperties1 = ColumnEncryptionProperties.builder("int32_field")
      .withKey(columnKey1)
      .withKeyID("ck1")
      .build();
    columnPropertiesMap = new HashMap<ColumnPath, ColumnEncryptionProperties>();
    columnPropertiesMap.put(columnProperties0.getPath(), columnProperties0);
    columnPropertiesMap.put(columnProperties1.getPath(), columnProperties1);
    encryptionProperties = FileEncryptionProperties.builder(footerKey)
      .withFooterKeyID("fk")
      .withPlaintextFooter()
      .withAADPrefix(AADPrefix)
      .withEncryptedColumns(columnPropertiesMap)
      .build();
    encryptionPropertiesList[4] = encryptionProperties;
    decryptionPropertiesList[4] = decryptionProperties; // Same decryption properties


    MessageType schema = parseMessageType(
      "message test { "
        + "required binary binary_field; "
        + "required int32 int32_field; "
        + "required int64 int64_field; "
        + "required boolean boolean_field; "
        + "required float float_field; "
        + "required double double_field; "
        + "required fixed_len_byte_array(3) flba_field; "
        + "required int96 int96_field; "
        + "} ");
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);

    for (int encryptionMode = 0; encryptionMode < numberOfEncryptionModes; encryptionMode++) {
      System.out.println("MODE: "+encryptionMode);

      Path file = new Path(root, "m_" + encryptionMode + ".parquet.encrypted");
      ParquetWriter<Group> writer = new ParquetWriter<Group>(
        file,
        new GroupWriteSupport(),
        UNCOMPRESSED, 1024, 1024, 512, true, false, ParquetWriter.DEFAULT_WRITER_VERSION, conf,
        encryptionPropertiesList[encryptionMode]);
      for (int i = 0; i < 1000; i++) {
        writer.write(
          f.newGroup()
            .append("binary_field", "test" + i)
            .append("int32_field", 32)
            .append("int64_field", 64l)
            .append("boolean_field", true)
            .append("float_field", 1.0f)
            .append("double_field", 2.0d)
            .append("flba_field", "foo")
            .append("int96_field", Binary.fromConstantByteArray(new byte[12])));
      }
      writer.close();

      FileDecryptionProperties fileDecryptionProperties = decryptionPropertiesList[encryptionMode];
      ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
        .withDecryption(fileDecryptionProperties).withConf(conf).build();
      for (int i = 0; i < 1000; i++) {
        Group group = null;
        group= reader.read();
        assertEquals("test" + i, group.getBinary("binary_field", 0).toStringUsingUTF8());
        assertEquals(32, group.getInteger("int32_field", 0));
        assertEquals(64l, group.getLong("int64_field", 0));
        assertEquals(true, group.getBoolean("boolean_field", 0));
        assertEquals(1.0f, group.getFloat("float_field", 0), 0.001);
        assertEquals(2.0d, group.getDouble("double_field", 0), 0.001);
        assertEquals("foo", group.getBinary("flba_field", 0).toStringUsingUTF8());
        assertEquals(Binary.fromConstantByteArray(new byte[12]),
          group.getInt96("int96_field",0));
      }
      reader.close();
    }
    enforceEmptyDir(conf, root);
  }

}
