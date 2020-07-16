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

package org.apache.parquet.crypto.CryptoPropertiesFactoryTests;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.EncryptionPropertiesFactory;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.parquet.schema.ExtType;
import org.apache.parquet.schema.Type;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SchemaControlEncryptionTest {

  private final static Log LOG = LogFactory.getLog(SchemaControlEncryptionTest.class);
  private final static int numRecord = 1000;
  private Random rnd = new Random(5);
  
  // In the test We use a map to tell WriteSupport which columns to be encrypted with what key. In real use cases, people
  // can find whatever easy way to do so basing on how do they get these information, for example people can choose to 
  // store in HMS, or other metastore. 
  private Map<String, Map<String, String>> crytoMetadatas = new HashMap<>();
  private Map<String, Object[]> testData = new HashMap<>();

  @Before
  public void generateTestData() {
    String[] names = new String[numRecord];
    Long[] ages = new Long[numRecord];
    String[] linkedInWebs = new String[numRecord];
    String[] twitterWebs = new String[numRecord];
    for (int i = 0; i < numRecord; i++) {
      names[i] = getString();
      ages[i] = getLong();
      linkedInWebs[i] = getString();
      twitterWebs[i] = getString();
    }

    testData.put("Name", names);
    testData.put("Age", ages);
    testData.put("LinkedIn", linkedInWebs);
    testData.put("Twitter", twitterWebs);
  }

  @Test
  public void testEncryptionDefault() throws Exception {
    Configuration conf = new Configuration();
    runTest(conf);
  }

  @Test
  public void testEncryptionGcm() throws Exception {
    Configuration conf = new Configuration();
    conf.set(SchemaCryptoPropertiesFactory.CONF_ENCRYPTION_ALGORITHM, "AES_GCM_CTR_V1");
    runTest(conf);
  }

  @Test
  public void testEncryptionGcmCtr() throws Exception {
    Configuration conf = new Configuration();
    conf.set(SchemaCryptoPropertiesFactory.CONF_ENCRYPTION_ALGORITHM, "AES_GCM_V1");
    runTest(conf);
  }

  @Test
  public void testEncryptionWithFooter() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(SchemaCryptoPropertiesFactory.CONF_ENCRYPTION_FOOTER, true);
    runTest(conf);
  }

  private void runTest(Configuration conf ) throws Exception {
    conf.set(EncryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME,
      "org.apache.parquet.crypto.CryptoPropertiesFactoryTests.SchemaCryptoPropertiesFactory");
    String file = createTempFile("test");
    markEncryptColumns();
    encryptParquetFile(file, conf);
    decryptParquetFileAndValid(file, conf);
  }
  
  private void markEncryptColumns() {
    Map<String, String> ageMetadata = new HashMap<>();
    ageMetadata.put("columnKeyMetaData", "age_key_id");
    ageMetadata.put("encrypted", "true");
    crytoMetadatas.put("Age", ageMetadata);

    Map<String, String> linkMetadata = new HashMap<>();
    linkMetadata.put("columnKeyMetaData", "link_key_id");
    linkMetadata.put("encrypted", "true");
    crytoMetadatas.put("LinkedIn", linkMetadata);
  }

  private String encryptParquetFile(String file, Configuration conf) throws IOException {
    MessageType schema = new MessageType("schema",
      new PrimitiveType(REQUIRED, BINARY, "Name"),
      new PrimitiveType(REQUIRED, INT64, "Age"),
      new GroupType(OPTIONAL, "WebLinks",
        new PrimitiveType(REPEATED, BINARY, "LinkedIn"),
        new PrimitiveType(REPEATED, BINARY, "Twitter")));

    conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());

    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new Path(file))
                                                              .withConf(conf)
                                                              .withWriteSupport(new CryptoGroupWriteSupport());
    try (ParquetWriter writer = builder.build()) {
      for (int i = 0; i < 1000; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("Name", (String)testData.get("Name")[i]);
        g.add("Age", (Long)testData.get("Age")[i]);
        Group links = g.addGroup("WebLinks");
        links.add(0, (String)testData.get("LinkedIn")[i]);
        links.add(1, (String)testData.get("Twitter")[i]);
        writer.write(g);
      }
    }

    return file;
  }

  private void decryptParquetFileAndValid(String file, Configuration conf) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      assertEquals(testData.get("Name")[i], group.getBinary("Name", 0).toStringUsingUTF8());
      assertEquals(testData.get("Age")[i], group.getLong("Age", 0));

      Group subGroup = group.getGroup("WebLinks", 0);
      assertArrayEquals(subGroup.getBinary("LinkedIn", 0).getBytes(), ((String)testData.get("LinkedIn")[i]).getBytes());
      assertArrayEquals(subGroup.getBinary("Twitter", 0).getBytes(), ((String)testData.get("Twitter")[i]).getBytes());
    }
    reader.close();
  }

  private static String createTempFile(String prefix) {
    try {
      return Files.createTempDirectory(prefix).toAbsolutePath().toString() + "/test.parquet";
    } catch (IOException e) {
      throw new AssertionError("Unable to create temporary file", e);
    }
  }

  private static long getLong() {
    return ThreadLocalRandom.current().nextLong(1000);
  }

  private String getString() {
    char[] chars = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'x', 'z', 'y'};
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      sb.append(chars[rnd.nextInt(10)]);
    }
    return sb.toString();
  }

  private class CryptoGroupWriteSupport extends GroupWriteSupport {

    public CryptoGroupWriteSupport() {
      super();
    }

    @Override
    public WriteContext init(Configuration configuration) {
      WriteContext writeContext = super.init(configuration);
      MessageType schema = writeContext.getSchema();
      List<Type> fields = schema.getFields();
      List<Type> newFields = new ArrayList<>();

      if (LOG.isDebugEnabled()) {
        LOG.debug("There are " + fields.size() + " fields");
      }

      for (Type field : fields) {
        ExtType<String> cryptoField = convertToExtTypeField(field);
        newFields.add(cryptoField);
      }

      MessageType newSchema = new MessageType(schema.getName(), newFields);
      Map<String, String> extraMetadata = new HashMap<>();

      return new WriteContext(newSchema, extraMetadata);
    }

    private ExtType<String> convertToExtTypeField(Type field) {
      System.out.println(field.getName());
      if (field.isPrimitive()) {
        ExtType<String> result = new ExtType<>(field);
        if (crytoMetadatas.containsKey(field.getName())) {
          result.setMetadata(crytoMetadatas.get(field.getName()));
        }
        return result;
      } else {
        List<Type> newFields = new ArrayList<>();
        for (Type childField : field.asGroupType().getFields()) {
          ExtType<String> newField = convertToExtTypeField(childField);
          newFields.add(newField);
        }
        ExtType<String> result = new ExtType<>(field.asGroupType().withNewFields(newFields));
        result.setMetadata(crytoMetadatas.get(field.getName()));
        return result;
      }
    }
  }
}


