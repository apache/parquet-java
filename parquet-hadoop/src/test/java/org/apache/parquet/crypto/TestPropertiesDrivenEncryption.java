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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.keytools.KeyToolkit;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;


/*
 * This file contains samples for writing and reading encrypted Parquet files in different
 * encryption and decryption configurations, set using a properties-driven interface.
 *
 * The write sample produces number of parquet files, each encrypted with a different
 * encryption configuration as described below.
 * The name of each file is in the form of:
 * <encryption-configuration-name>.parquet.encrypted
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
 *  - ENCRYPT_COLUMNS_AND_FOOTER:   Encrypt two columns and the footer, with different
 *                                  keys.
 *  - ENCRYPT_COLUMNS_PLAINTEXT_FOOTER:   Encrypt two columns, with different keys.
 *                                  Do not encrypt footer (to enable legacy readers)
 *                                  - plaintext footer mode.
 *  - ENCRYPT_COLUMNS_AND_FOOTER_CTR:   Encrypt two columns and the footer, with different
 *                                  keys. Use the alternative (AES_GCM_CTR_V1) algorithm.
 *  - WRITE_PLAINTEXT:   Do not encrypt anything
 *
 *
 *
 * The read sample uses each of the following decryption configurations to read every
 * encrypted files in the input directory:
 *
 *  - DECRYPT_COLUMNS_AND_FOOTER:   Decrypt using key retriever that holds the keys of
 *                                  two encrypted columns and the footer key.
 *  - READ_PLAINTEXT:   Do not decrypt anything.
 */
public class TestPropertiesDrivenEncryption {
  private static final Logger LOG = LoggerFactory.getLogger(TestPropertiesDrivenEncryption.class);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  private static final Base64.Encoder encoder = Base64.getEncoder();
  private static final byte[] FOOTER_ENCRYPTION_KEY = new String("0123456789012345").getBytes();
  private static final byte[] COLUMN_ENCRYPTION_KEY1 = new String("1234567890123450").getBytes();
  private static final byte[] COLUMN_ENCRYPTION_KEY2 = new String("1234567890123451").getBytes();
  private static final String KEY_LIST =
    "kc1: " + encoder.encodeToString(COLUMN_ENCRYPTION_KEY1) +
    ", kc2: " +  encoder.encodeToString(COLUMN_ENCRYPTION_KEY2) +
    ", kf: "  + encoder.encodeToString(FOOTER_ENCRYPTION_KEY);
  private static final String COLUMN_KEY_MAPPING = "kc1: double_field; kc2: float_field";
  private static final boolean plaintextFilesAllowed = true;

  final WriteSupport<Group> writeSupport = new GroupWriteSupport();

  @Test
  public void testWriteReadEncryptedParquetFiles() throws IOException {
    Path rootPath = new Path(temporaryFolder.getRoot().getPath());
    LOG.info("======== testWriteReadEncryptedParquetFiles {} ========", rootPath.toString());
    // This map will hold various encryption configurations.
    Map<EncryptionConfiguration, Configuration> encryptionPropertiesMap = getHadoopConfigurationForEncryption();
    testWriteEncryptedParquetFiles(rootPath, encryptionPropertiesMap);
    // This map will hold various decryption configurations.
    Map<DecryptionConfiguration, Configuration> decryptionPropertiesMap = getHadoopConfigurationForDecryption();
    testReadEncryptedParquetFiles(rootPath, decryptionPropertiesMap);
  }


  private void testWriteEncryptedParquetFiles(Path root, Map<EncryptionConfiguration, Configuration> encryptionPropertiesMap) throws IOException {
    MessageType schema = parseMessageType(
      "message test { "
        + "required boolean boolean_field; "
        + "required int32 int32_field; "
        + "required float float_field; "
        + "required double double_field; "
        + "} ");

    SimpleGroupFactory f = new SimpleGroupFactory(schema);

    for (Map.Entry<EncryptionConfiguration, Configuration> encryptionConfigurationEntry : encryptionPropertiesMap.entrySet()) {
      KeyToolkit.removeCacheEntriesForToken(KeyToolkit.DEFAULT_ACCESS_TOKEN);
      EncryptionConfiguration encryptionConfiguration = encryptionConfigurationEntry.getKey();
      Configuration conf = encryptionConfigurationEntry.getValue();

      Path file = new Path(root, encryptionConfiguration.toString() + ".parquet.encrypted");
      LOG.info("\nWrite " + file.toString());

      FileEncryptionProperties fileEncryptionProperties = null;
      if (null == conf) {
        conf = new Configuration();
      } else {

        EncryptionPropertiesFactory cryptoFactory = EncryptionPropertiesFactory.loadFactory(conf);
        fileEncryptionProperties = cryptoFactory.getFileEncryptionProperties(conf, file, null);
      }
      GroupWriteSupport.setSchema(schema, conf);

      ParquetWriter<Group> writer =
        new ParquetWriter<Group>(
        file,
          new GroupWriteSupport(),
        UNCOMPRESSED, 1024, 1024, 512, true, false,
        ParquetWriter.DEFAULT_WRITER_VERSION, conf,
          fileEncryptionProperties);

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

  private void testReadEncryptedParquetFiles(Path root, Map<DecryptionConfiguration, Configuration> decryptionPropertiesMap) throws IOException {
    for (Map.Entry<DecryptionConfiguration, Configuration> decryptionConfigurationEntry : decryptionPropertiesMap.entrySet()) {
      DecryptionConfiguration decryptionConfiguration = decryptionConfigurationEntry.getKey();
      LOG.info("\n\n");
      LOG.info("==> Decryption configuration {}\n", decryptionConfiguration);

      File folder = new File(root.toString());
      File[] listOfFiles = folder.listFiles();

      for (int fileNum = 0; fileNum < listOfFiles.length; fileNum++) {
        KeyToolkit.removeCacheEntriesForToken(KeyToolkit.DEFAULT_ACCESS_TOKEN);
        Path file = new Path(listOfFiles[fileNum].getAbsolutePath());
        if (!file.getName().endsWith("parquet.encrypted")) { // Skip non encrypted files
          continue;
        }
        EncryptionConfiguration encryptionConfiguration = getEncryptionConfigurationFromFilename(file.getName());
        if (null == encryptionConfiguration) {
          continue;
        }
        LOG.info("--> Read file {} {}", file.toString(), encryptionConfiguration);

        FileDecryptionProperties fileDecryptionProperties = null;
        Configuration hadoopConfig = decryptionConfigurationEntry.getValue();
        if (null == hadoopConfig) {
          hadoopConfig = new Configuration();
        } else {
          DecryptionPropertiesFactory cryptoFactory = DecryptionPropertiesFactory.loadFactory(hadoopConfig);
          fileDecryptionProperties = cryptoFactory.getFileDecryptionProperties(hadoopConfig, file);
        }

        // Read only the non-encrypted columns
        if ((decryptionConfiguration == DecryptionConfiguration.READ_PLAINTEXT) &&
          (encryptionConfiguration == EncryptionConfiguration.ENCRYPT_COLUMNS_PLAINTEXT_FOOTER)) {
          hadoopConfig.set("parquet.read.schema", Types.buildMessage()
            .required(BOOLEAN).named("boolean_field")
            .required(INT32).named("int32_field")
            .named("FormatTestObject").toString());
        }
        ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).
          withDecryption(fileDecryptionProperties).
          withConf(hadoopConfig).build();
        try {
          for (int i = 0; i < 500; i++) {
            Group group = null;
            group = reader.read();
            boolean expect = false;
            if ((i % 2) == 0)
              expect = true;
            boolean bool_res = group.getBoolean("boolean_field", 0);
            if (bool_res != expect)
              addErrorToErrorCollectorAndLog("Wrong bool", encryptionConfiguration, decryptionConfiguration);
            int int_res = group.getInteger("int32_field", 0);
            if (int_res != i)
              addErrorToErrorCollectorAndLog("Wrong int", encryptionConfiguration, decryptionConfiguration);
            if (decryptionConfiguration != DecryptionConfiguration.READ_PLAINTEXT) {
              float float_res = group.getFloat("float_field", 0);
              float tmp1 = (float) i * 1.1f;
              if (float_res != tmp1)
                addErrorToErrorCollectorAndLog("Wrong float", encryptionConfiguration, decryptionConfiguration);

              double double_res = group.getDouble("double_field", 0);
              double tmp = (i * 1.1111111);
              if (double_res != tmp)
                addErrorToErrorCollectorAndLog("Wrong double", encryptionConfiguration, decryptionConfiguration);
            }
          }
        } catch (Exception e) {
          String errorMessage = e.getMessage();
          checkResult(file.getName(), decryptionConfiguration, (null == errorMessage ? "" : errorMessage));
        }
        hadoopConfig.unset("parquet.read.schema");
      }
    }
  }


  /**
   * Create a number of Encryption configurations
   */
  private Map<EncryptionConfiguration, Configuration> getHadoopConfigurationForEncryption() {
    EncryptionConfiguration[] encryptionConfigurations = EncryptionConfiguration.values();
    Map<EncryptionConfiguration, Configuration> encryptionPropertiesMap = new HashMap<>(encryptionConfigurations.length);

    for (int i = 0; i < encryptionConfigurations.length; ++i) {
      EncryptionConfiguration encryptionConfiguration = encryptionConfigurations[i];
      Configuration conf = new Configuration();
      conf.set("encryption.kms.client.class", "org.apache.parquet.crypto.keytools.samples.InMemoryKMS");
      switch (encryptionConfiguration) {
        case ENCRYPT_COLUMNS_AND_FOOTER:
          // Encrypt two columns and the footer, with different keys.
          conf.set(EncryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME,
            "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory");
          conf.set("encryption.key.list", KEY_LIST);
          conf.set("encryption.column.keys", COLUMN_KEY_MAPPING);
          conf.set("encryption.footer.key", "kf");

          encryptionPropertiesMap.put(EncryptionConfiguration.ENCRYPT_COLUMNS_AND_FOOTER, conf);
          break;

        case ENCRYPT_COLUMNS_PLAINTEXT_FOOTER:
          // Encrypt two columns, with different keys.
          // Don't encrypt footer.
          // (plaintext footer mode, readable by legacy readers)
          conf.set(EncryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME,
            "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory");
          conf.set("encryption.key.list", KEY_LIST);
          conf.set("encryption.column.keys", COLUMN_KEY_MAPPING);
          conf.set("encryption.footer.key", "kf");
          conf.setBoolean("encryption.plaintext.footer", true);

          encryptionPropertiesMap.put(EncryptionConfiguration.ENCRYPT_COLUMNS_PLAINTEXT_FOOTER, conf);
          break;

        case ENCRYPT_COLUMNS_AND_FOOTER_CTR:
          // Encrypt two columns and the footer, with different keys.
          // Use AES_GCM_CTR_V1 algorithm.
          conf.set(EncryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME,
            "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory");
          conf.set("encryption.algorithm", "AES_GCM_CTR_V1");
          conf.set("encryption.key.list", KEY_LIST);
          conf.set("encryption.column.keys", COLUMN_KEY_MAPPING);
          conf.set("encryption.footer.key", "kf");

          encryptionPropertiesMap.put(EncryptionConfiguration.ENCRYPT_COLUMNS_AND_FOOTER_CTR, conf);
          break;

        case WRITE_PLAINTEXT:
          // Do not encrypt anything
          encryptionPropertiesMap.put(EncryptionConfiguration.WRITE_PLAINTEXT, null);
          break;
      }
    }
    return encryptionPropertiesMap;
  }


  /**
   * Create a number of Decryption configurations
   */
  private Map<DecryptionConfiguration, Configuration>  getHadoopConfigurationForDecryption() {
    DecryptionConfiguration[] decryptionConfigurations = DecryptionConfiguration.values();
    Map<DecryptionConfiguration, Configuration> decryptionPropertiesMap = new HashMap<>(decryptionConfigurations.length);

    for (DecryptionConfiguration decryptionConfiguration : decryptionConfigurations) {
      Configuration hadoopConfig = new Configuration();
      hadoopConfig.set("encryption.kms.client.class", "org.apache.parquet.crypto.keytools.samples.InMemoryKMS");
      switch (decryptionConfiguration) {
        case DECRYPT_COLUMNS_AND_FOOTER:
          // Decrypt using key retriever callback that holds the keys
          // of two encrypted columns and the footer key.
          hadoopConfig.set(EncryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME,
            "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory");
          hadoopConfig.set("encryption.key.list", KEY_LIST);
          decryptionPropertiesMap.put(DecryptionConfiguration.DECRYPT_COLUMNS_AND_FOOTER, hadoopConfig);
        break;

        case READ_PLAINTEXT:
          // Do not decrypt anything.
          decryptionPropertiesMap.put(DecryptionConfiguration.READ_PLAINTEXT, null);
          break;
      }
    }
    return decryptionPropertiesMap;
  }


  /**
   * Check that the decryption result is as expected.
   */
  private void checkResult(String file, DecryptionConfiguration decryptionConfiguration, String exceptionMsg) {
    // Extract encryptionConfigurationNumber from the parquet file name.
    EncryptionConfiguration encryptionConfiguration = getEncryptionConfigurationFromFilename(file);

    if (!plaintextFilesAllowed) {
      // Encryption_configuration null encryptor, so parquet is plaintext.
      // An exception is expected to be thrown if the file is being decrypted.
      if (encryptionConfiguration == EncryptionConfiguration.WRITE_PLAINTEXT) {
        if (decryptionConfiguration == DecryptionConfiguration.DECRYPT_COLUMNS_AND_FOOTER) {
          if (!exceptionMsg.endsWith("Applying decryptor on plaintext file")) {
            addErrorToErrorCollectorAndLog("Expecting exception Applying decryptor on plaintext file",
              exceptionMsg, encryptionConfiguration, decryptionConfiguration);
          } else {
            LOG.info("Exception as expected: " + exceptionMsg);
          }
          return;
        }
      }
    }
    // Decryption configuration is null, so only plaintext file can be read. An exception is expected to
    // be thrown if the file is encrypted.
    if (decryptionConfiguration == DecryptionConfiguration.READ_PLAINTEXT) {
      if ((encryptionConfiguration != EncryptionConfiguration.WRITE_PLAINTEXT &&
        encryptionConfiguration != EncryptionConfiguration.ENCRYPT_COLUMNS_PLAINTEXT_FOOTER)) {
        if (!exceptionMsg.endsWith("No encryption key list") && !exceptionMsg.endsWith("No keys available")) {
          addErrorToErrorCollectorAndLog("Expecting  No keys available exception", exceptionMsg,
            encryptionConfiguration, decryptionConfiguration);
        } else {
          LOG.info("Exception as expected: " + exceptionMsg);
        }
        return;
      }
    }
    if (null != exceptionMsg && !exceptionMsg.equals("")) {
      addErrorToErrorCollectorAndLog("Didn't expect an exception", exceptionMsg,
        encryptionConfiguration, decryptionConfiguration);
    }
  }

  private EncryptionConfiguration getEncryptionConfigurationFromFilename(String file) {
    if (!file.endsWith(".parquet.encrypted")) {
      return null;
    }
    String fileNamePrefix = file.replaceFirst(".parquet.encrypted", "");
    try {
      EncryptionConfiguration encryptionConfiguration = EncryptionConfiguration.valueOf(fileNamePrefix.toUpperCase());
      return encryptionConfiguration;
    } catch (IllegalArgumentException e) {
      LOG.error("File name doesn't match any known encryption configuration: " + file);
      errorCollector.addError(e);
      return null;
    }
  }

  private void addErrorToErrorCollectorAndLog(String errorMessage, String exceptionMessage, EncryptionConfiguration encryptionConfiguration,
                                              DecryptionConfiguration decryptionConfiguration) {
    String fullErrorMessage = String.format("%s - %s Error: %s, but got [%s]",
      encryptionConfiguration, decryptionConfiguration, errorMessage, exceptionMessage);

    errorCollector.addError(new Throwable(fullErrorMessage));
    LOG.error(fullErrorMessage);
  }

  private void addErrorToErrorCollectorAndLog(String errorMessage, EncryptionConfiguration encryptionConfiguration,
                                                     DecryptionConfiguration decryptionConfiguration) {
    String fullErrorMessage = String.format("%s - %s Error: %s",
      encryptionConfiguration, decryptionConfiguration, errorMessage);

    errorCollector.addError(new Throwable(fullErrorMessage));
    LOG.error(fullErrorMessage);
  }

  public enum EncryptionConfiguration {
    ENCRYPT_COLUMNS_AND_FOOTER("ENCRYPT_COLUMNS_AND_FOOTER"),
    ENCRYPT_COLUMNS_PLAINTEXT_FOOTER("ENCRYPT_COLUMNS_PLAINTEXT_FOOTER"),
    ENCRYPT_COLUMNS_AND_FOOTER_CTR("ENCRYPT_COLUMNS_AND_FOOTER_CTR"),
    WRITE_PLAINTEXT("WRITE_PLAINTEXT");

    private final String configurationName;

    EncryptionConfiguration(String configurationName) {
      this.configurationName = configurationName;
    }

    @Override
    public String toString() {
      return configurationName;
    }
  }


  public enum DecryptionConfiguration {
    DECRYPT_COLUMNS_AND_FOOTER("DECRYPT_COLUMNS_AND_FOOTER"),
    READ_PLAINTEXT("READ_PLAINTEXT");

    private final String configurationName;

    DecryptionConfiguration(String configurationName) {
      this.configurationName = configurationName;
    }

    @Override
    public String toString() {
      return configurationName;
    }
  }

}
