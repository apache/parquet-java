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
package org.apache.parquet.hadoop.util;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertArrayEquals;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ColumnMaskerTest {

  private Configuration conf = new Configuration();
  private Map<String, String> extraMeta = ImmutableMap.of("key1", "value1", "key2", "value2");
  private ColumnMasker columnMasker = new ColumnMasker();
  private Random rnd = new Random(5);
  private final int numRecord = 1000;
  private String inputFile = null;
  private String outputFile = null;
  private TestDocs testDocs = null;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String newTempFile() throws IOException {
    File file = tempFolder.newFile();
    file.delete();
    return file.getAbsolutePath();
  }

  @Before
  public void testSetup() throws Exception {
    testDocs = new TestDocs(numRecord);
    inputFile = createParquetFile(
        conf,
        extraMeta,
        numRecord,
        "GZIP",
        ParquetProperties.WriterVersion.PARQUET_1_0,
        ParquetProperties.DEFAULT_PAGE_SIZE,
        testDocs);
    outputFile = newTempFile();
    nullifyColumns(conf, inputFile, outputFile);
  }

  @Test(expected = RuntimeException.class)
  public void testNullColumns() throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(outputFile))
        .withConf(conf)
        .build();
    Group group = reader.read();
    group.getLong("DocId", 0);
    reader.close();
  }

  @Test(expected = RuntimeException.class)
  public void testNullNestedColumns() throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(outputFile))
        .withConf(conf)
        .build();
    Group group = reader.read();
    Group subGroup = group.getGroup("Links", 0);
    subGroup.getBinary("Backward", 0).getBytes();
    reader.close();
  }

  @Test
  public void validateNonNuLLColumns() throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(outputFile))
        .withConf(conf)
        .build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      assertArrayEquals(group.getBinary("Name", 0).getBytes(), testDocs.name[i].getBytes());
      Group subGroup = group.getGroup("Links", 0);
      assertArrayEquals(subGroup.getBinary("Forward", 0).getBytes(), testDocs.linkForward[i].getBytes());
    }
    reader.close();
  }

  private void nullifyColumns(Configuration conf, String inputFile, String outputFile) throws IOException {
    Path inPath = new Path(inputFile);
    Path outPath = new Path(outputFile);

    ParquetMetadata metaData = ParquetFileReader.readFooter(conf, inPath, NO_FILTER);
    MessageType schema = metaData.getFileMetaData().getSchema();
    ParquetFileWriter writer = new ParquetFileWriter(conf, schema, outPath, ParquetFileWriter.Mode.OVERWRITE);
    writer.start();

    List<String> paths = new ArrayList<>();
    paths.add("DocId");
    paths.add("Gender");
    paths.add("Links.Backward");
    try (TransParquetFileReader reader = new TransParquetFileReader(
        HadoopInputFile.fromPath(inPath, conf),
        HadoopReadOptions.builder(conf).build())) {
      columnMasker.processBlocks(reader, writer, metaData, schema, paths, ColumnMasker.MaskMode.NULLIFY);
    } finally {
      writer.end(metaData.getFileMetaData().getKeyValueMetaData());
    }
  }

  private String createParquetFile(
      Configuration conf,
      Map<String, String> extraMeta,
      int numRecord,
      String codec,
      ParquetProperties.WriterVersion writerVersion,
      int pageSize,
      TestDocs testDocs)
      throws IOException {
    MessageType schema = new MessageType(
        "schema",
        new PrimitiveType(OPTIONAL, INT64, "DocId"),
        new PrimitiveType(REQUIRED, BINARY, "Name"),
        new PrimitiveType(OPTIONAL, BINARY, "Gender"),
        new GroupType(
            OPTIONAL,
            "Links",
            new PrimitiveType(REPEATED, BINARY, "Backward"),
            new PrimitiveType(REPEATED, BINARY, "Forward")));

    conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());

    String file = newTempFile();
    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new Path(file))
        .withConf(conf)
        .withWriterVersion(writerVersion)
        .withExtraMetaData(extraMeta)
        .withDictionaryEncoding("DocId", true)
        .withDictionaryEncoding("Name", true)
        .withValidation(true)
        .enablePageWriteChecksum()
        .withPageSize(pageSize)
        .withCompressionCodec(CompressionCodecName.valueOf(codec));
    try (ParquetWriter writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("DocId", testDocs.docId[i]);
        g.add("Name", testDocs.name[i]);
        g.add("Gender", testDocs.gender[i]);
        Group links = g.addGroup("Links");
        links.add(0, testDocs.linkBackward[i]);
        links.add(1, testDocs.linkForward[i]);
        writer.write(g);
      }
    }

    return file;
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

  private class TestDocs {
    public long[] docId;
    public String[] name;
    public String[] gender;
    public String[] linkBackward;
    public String[] linkForward;

    public TestDocs(int numRecord) {
      docId = new long[numRecord];
      for (int i = 0; i < numRecord; i++) {
        docId[i] = getLong();
      }

      name = new String[numRecord];
      for (int i = 0; i < numRecord; i++) {
        name[i] = getString();
      }

      gender = new String[numRecord];
      for (int i = 0; i < numRecord; i++) {
        gender[i] = getString();
      }

      linkBackward = new String[numRecord];
      for (int i = 0; i < numRecord; i++) {
        linkBackward[i] = getString();
      }

      linkForward = new String[numRecord];
      for (int i = 0; i < numRecord; i++) {
        linkForward[i] = getString();
      }
    }
  }
}
