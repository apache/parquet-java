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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;

import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.crypto.StringKeyIdRetriever;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;


import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.junit.rules.TemporaryFolder;

public class TestEncryption {

  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();
    Path root = new Path("target/tests/TestEncryption/");
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


  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
}

