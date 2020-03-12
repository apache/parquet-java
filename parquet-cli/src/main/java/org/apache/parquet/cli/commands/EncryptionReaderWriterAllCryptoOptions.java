/* Licensed to the Apache Software Foundation (ASF) under one
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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.crypto.*;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import java.io.IOException;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;


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
 * Usage: java -cp 'parquet-mr/parquet-cli/target/dependency/*:parquet-mr/parquet-cli/target/*' 
          org.apache.parquet.cli.Main eit -w  -p  <write/read> <path-to-directory-of-parquet-files>
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
 */


@Parameters(commandDescription = "Execute iterop tests")
public class EncryptionReaderWriterAllCryptoOptions extends BaseCommand {

  byte[] FOOTER_ENCRYPTION_KEY = new String("0123456789012345").getBytes();
  byte[] COLUMN_ENCRYPTION_KEY1 = new String("1234567890123450").getBytes();
  byte[] COLUMN_ENCRYPTION_KEY2 = new String("1234567890123451").getBytes();
  String fileName = "tester";
  byte[] AADPrefix = fileName.getBytes(StandardCharsets.UTF_8);
  Configuration conf = new Configuration();

  @Parameter(names={"-p", "--parquet-files-path"},
      description="path to parquet-files")
  String parquetFilesDir = "target/tests/TestEncryption/";

  @Parameter(names={"-w", "--write-parquet"},
      description="Execute write parquet tests")
  boolean writeParquet = false;

  @Parameter(names={"-r", "--read-parquet"},
      description="Execute read parquet tests")
  boolean readParquet = false;

  public EncryptionReaderWriterAllCryptoOptions(Logger console) {
    super(console);
  }

  private void InteropTestWriteEncryptedParquetFiles(Path root) throws IOException {

    /**********************************************************************************
     Creating a number of Encryption configurations
     **********************************************************************************/

    // This array will hold various encryption configuraions.
    int numberOfEncryptionModes = 6;
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

  private void InteropTestReadEncryptedParquetFiles(Path root) throws IOException{

    /**********************************************************************************
     Creating a number of Decryption configurations
     **********************************************************************************/

    // This array will hold various decryption configurations.
    int numberOfDecryptionModes = 3;
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

    for (int decryptionMode = 0; decryptionMode < numberOfDecryptionModes; decryptionMode++) {
      PrintDecryptionConfiguration(decryptionMode + 1);

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
        String exceptionMsg = "";
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
          exceptionMsg = e.getMessage();
        }
        CheckResult(file.toString(), decryptionMode, exceptionMsg);
      }
    }
  }

  // Check that the decryption result is as expected.
  private void CheckResult(String file, int exampleId, String exceptionMsg) {
    int encryptionConfigurationNumber = -1;
    // Extract encryptionConfigurationNumber from the parquet file name.
    Pattern p = Pattern.compile("tester([0-9]+)\\.parquet.encrypted");
    Matcher m = p.matcher(file);

    if (m.find()) {
      encryptionConfigurationNumber = Integer.parseInt(m.group(1));
    } else {
      System.out.println("Error: Error parsing filename to extract encryption configuration number. ");
    }
    int decryptionConfigurationNumber = exampleId + 1;

    // Encryption_configuration number five contains aad_prefix and
    // disable_aad_prefix_storage.
    // An exception is expected to be thrown if the file is not decrypted with aad_prefix.
    if (encryptionConfigurationNumber == 5) {
      if (decryptionConfigurationNumber == 1 || decryptionConfigurationNumber == 3) {
        if (!exceptionMsg.contains("AAD")) {
          System.out.println("Error: Expecting AAD related exception.");
        }
        return;
      }
    }
    // Decryption configuration number two contains aad_prefix. An exception is expected to
    // be thrown if the file was not encrypted with the same aad_prefix.
    if (decryptionConfigurationNumber == 2) {
      if (encryptionConfigurationNumber != 5 && encryptionConfigurationNumber != 4) {
        if (!exceptionMsg.contains("AAD")) {
          System.out.println("Error: Expecting AAD related exception." + exceptionMsg);
        }
        return;
      }
    }
    if (null != exceptionMsg && !exceptionMsg.equals(""))
      System.out.println("Error: Unexpected exception was thrown: " + exceptionMsg);
  }

  private void PrintDecryptionConfiguration(int configuration) {
    System.out.print("\n\nDecryption configuration ");
    if (configuration == 1) {
      System.out.println("1: \n\nDecrypt using key retriever that holds" +
          " the keys of two encrypted columns and the footer key.");
    } else if (configuration == 2) {
      System.out.println("2: \n\nDecrypt using key retriever that holds" +
          " the keys of two encrypted columns and the footer key. Pass aad_prefix.");
    } else if (configuration == 3) {
      System.out.println("3: \n\nDecrypt using explicit column and footer keys.");
    }  else {
      System.out.println("Unknown configuraion");
    }
    System.out.println("");
  }

  @Override
  public int run() throws IOException {
    Path root = new Path(parquetFilesDir);

    if (this.writeParquet) {
      InteropTestWriteEncryptedParquetFiles(root);
    }
    if (this.readParquet) {
      InteropTestReadEncryptedParquetFiles(root);
    }
    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Show the first 10 records in file \"data.avro\":",
        "data.avro",
        "# Show the first 50 records in file \"data.parquet\":",
        "data.parquet -n 50"
        );
  }
}