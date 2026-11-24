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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

/**
 * Utility class for creating encrypted Parquet files for testing CLI commands.
 */
public final class EncryptedParquetFileTestHelper {

  // Standard test encryption keys (16 bytes for AES-128)
  public static final byte[] FOOTER_KEY = {
    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10
  };

  public static final byte[] COLUMN_KEY_1 = {
    0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11
  };

  public static final byte[] COLUMN_KEY_2 = {
    0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13
  };

  public static final String FOOTER_KEY_HEX = "0102030405060708090a0b0c0d0e0f10";
  public static final String COLUMN_KEY_1_HEX = "02030405060708090a0b0c0d0e0f1011";
  public static final String COLUMN_KEY_2_HEX = "0405060708090a0b0c0d0e0f10111213";

  public static final String COLUMN_KEYS_CONFIG = COLUMN_KEY_1_HEX + ":name,email;" + COLUMN_KEY_2_HEX + ":phone";

  private EncryptedParquetFileTestHelper() {}

  public static File createEncryptedParquetFile(File tempDir, String filename) throws IOException {
    return createEncryptedParquetFile(tempDir, filename, true, true);
  }

  public static File createEncryptedParquetFile(
      File tempDir, String filename, boolean enableBloomFilter, boolean encryptedFooter) throws IOException {

    MessageType schema = Types.buildMessage()
        .required(INT32)
        .named("id")
        .required(BINARY)
        .named("name")
        .required(BINARY)
        .named("email")
        .required(BINARY)
        .named("phone")
        .named("test_schema");

    File file = new File(tempDir, filename);
    file.deleteOnExit();

    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    String[] encryptColumns = {"name", "email", "phone"};
    FileEncryptionProperties encryptionProperties =
        createFileEncryptionProperties(encryptColumns, ParquetCipher.AES_GCM_CTR_V1, encryptedFooter);

    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    String[] nameValues = {"test_value_1", "test_value_2", "another_test", "bloom_filter_test", "final_value"};
    String[] emailValues = {
      "user1@test.com", "user2@test.com", "admin@test.com", "support@test.com", "sales@test.com"
    };
    String[] phoneValues = {"555-0001", "555-0002", "555-0003", "555-0004", "555-0005"};

    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new Path(file.toURI()))
        .withConf(conf)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withEncryption(encryptionProperties)
        .withPageSize(1024)
        .withRowGroupSize(4096);

    if (enableBloomFilter) {
      builder.withBloomFilterEnabled("name", true)
          .withBloomFilterEnabled("email", true)
          .withBloomFilterEnabled("phone", true);
    }

    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < nameValues.length; i++) {
        SimpleGroup group = (SimpleGroup) factory.newGroup();
        group.add("id", i + 1);
        group.add("name", Binary.fromString(nameValues[i]));
        group.add("email", Binary.fromString(emailValues[i]));
        group.add("phone", Binary.fromString(phoneValues[i]));
        writer.write(group);
      }
    }

    return file;
  }

  public static FileEncryptionProperties createFileEncryptionProperties(
      String[] encryptColumns, ParquetCipher cipher, boolean footerEncryption) {

    Map<String, byte[]> columnKeys = new HashMap<>();
    columnKeys.put("name", COLUMN_KEY_1);
    columnKeys.put("email", COLUMN_KEY_1);
    columnKeys.put("phone", COLUMN_KEY_2);

    Map<ColumnPath, ColumnEncryptionProperties> columnPropertyMap = new HashMap<>();
    for (String columnPath : encryptColumns) {
      ColumnPath column = ColumnPath.fromDotString(columnPath);
      byte[] columnKey = columnKeys.get(columnPath);

      ColumnEncryptionProperties columnProps = ColumnEncryptionProperties.builder(column)
          .withKey(columnKey)
          .withKeyMetaData(columnPath.getBytes(StandardCharsets.UTF_8))
          .build();
      columnPropertyMap.put(column, columnProps);
    }

    FileEncryptionProperties.Builder builder = FileEncryptionProperties.builder(FOOTER_KEY)
        .withFooterKeyMetadata("footkey".getBytes(StandardCharsets.UTF_8))
        .withAlgorithm(cipher)
        .withEncryptedColumns(columnPropertyMap);

    if (!footerEncryption) {
      builder.withPlaintextFooter();
    }

    return builder.build();
  }

  public static Configuration createDecryptionConfiguration() {
    Configuration conf = new Configuration();
    conf.set("parquet.encryption.footer.key", FOOTER_KEY_HEX);
    conf.set("parquet.encryption.column.keys", COLUMN_KEYS_CONFIG);
    return conf;
  }

  public static void setDecryptionProperties(Configuration conf) {
    conf.set("parquet.encryption.footer.key", FOOTER_KEY_HEX);
    conf.set("parquet.encryption.column.keys", COLUMN_KEYS_CONFIG);
  }
}
