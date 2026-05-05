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
import java.util.Arrays;
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
import org.junit.Assert;
import org.junit.Test;

public class ShowBloomFilterCommandTest extends ParquetFileTest {
  @Test
  public void testShowBloomFilterCommand() throws IOException {
    File file = parquetFile();
    ShowBloomFilterCommand command = new ShowBloomFilterCommand(createLogger());
    command.file = file.getAbsolutePath();
    command.columnPath = INT32_FIELD;
    command.testValues = Arrays.asList(new String[] {"1"});
    command.setConf(new Configuration());
    Assert.assertEquals(0, command.run());
  }

  @Test
  public void testEncryptedFileWithBloomFilter() throws IOException {
    File encryptedFile = createEncryptedFileWithBloomFilter();

    ShowBloomFilterCommand command = new ShowBloomFilterCommand(createLogger());
    command.file = encryptedFile.getAbsolutePath();
    command.columnPath = "name";
    command.testValues = Arrays.asList(new String[] {"test_value_1", "non_existent_value"});

    Configuration conf = new Configuration();
    conf.set("parquet.encryption.footer.key", "0102030405060708090a0b0c0d0e0f10");
    conf.set(
        "parquet.encryption.column.keys",
        "02030405060708090a0b0c0d0e0f1011:name,email;0405060708090a0b0c0d0e0f10111213:phone");
    command.setConf(conf);

    Assert.assertEquals(0, command.run());

    ShowBloomFilterCommand emailCommand = new ShowBloomFilterCommand(createLogger());
    emailCommand.file = encryptedFile.getAbsolutePath();
    emailCommand.columnPath = "email";
    emailCommand.testValues = Arrays.asList(new String[] {"user1@test.com", "missing@test.com"});
    emailCommand.setConf(conf);

    Assert.assertEquals(0, emailCommand.run());

    ShowBloomFilterCommand phoneCommand = new ShowBloomFilterCommand(createLogger());
    phoneCommand.file = encryptedFile.getAbsolutePath();
    phoneCommand.columnPath = "phone";
    phoneCommand.testValues = Arrays.asList(new String[] {"555-0001", "555-9999"});
    phoneCommand.setConf(conf);

    Assert.assertEquals(0, phoneCommand.run());

    encryptedFile.delete();
  }

  private File createEncryptedFileWithBloomFilter() throws IOException {
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

    File tempFile = new File(getTempFolder(), "encrypted_bloom_test.parquet");
    tempFile.deleteOnExit();

    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    String[] encryptColumns = {"name", "email", "phone"};
    FileEncryptionProperties encryptionProperties =
        createFileEncryptionProperties(encryptColumns, ParquetCipher.AES_GCM_CTR_V1, true);

    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    String[] nameValues = {"test_value_1", "test_value_2", "another_test", "bloom_filter_test", "final_value"};
    String[] emailValues = {
      "user1@test.com", "user2@test.com", "admin@test.com", "support@test.com", "sales@test.com"
    };
    String[] phoneValues = {"555-0001", "555-0002", "555-0003", "555-0004", "555-0005"};

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(tempFile.toURI()))
        .withConf(conf)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withEncryption(encryptionProperties)
        .withBloomFilterEnabled("name", true)
        .withBloomFilterEnabled("email", true)
        .withBloomFilterEnabled("phone", true)
        .withPageSize(1024)
        .withRowGroupSize(4096)
        .build()) {

      for (int i = 0; i < nameValues.length; i++) {
        SimpleGroup group = (SimpleGroup) factory.newGroup();
        group.add("id", i + 1);
        group.add("name", Binary.fromString(nameValues[i]));
        group.add("email", Binary.fromString(emailValues[i]));
        group.add("phone", Binary.fromString(phoneValues[i]));
        writer.write(group);
      }
    }

    return tempFile;
  }

  private FileEncryptionProperties createFileEncryptionProperties(
      String[] encryptColumns, ParquetCipher cipher, boolean footerEncryption) {

    byte[] footerKey = {
      0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10
    };

    byte[] sharedKey = new byte[] {
      0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11
    };
    byte[] phoneKey = new byte[] {
      0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13
    };

    Map<String, byte[]> columnKeys = new HashMap<>();
    columnKeys.put("name", sharedKey);
    columnKeys.put("email", sharedKey);
    columnKeys.put("phone", phoneKey);

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

    FileEncryptionProperties.Builder builder = FileEncryptionProperties.builder(footerKey)
        .withFooterKeyMetadata("footkey".getBytes(StandardCharsets.UTF_8))
        .withAlgorithm(cipher)
        .withEncryptedColumns(columnPropertyMap);

    if (!footerEncryption) {
      builder.withPlaintextFooter();
    }

    return builder.build();
  }
}
