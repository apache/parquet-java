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
package org.apache.parquet.hadoop.join;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.Type.Repetition.*;
import static org.junit.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.IndexCache;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.EncryptionTestFile;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.hadoop.util.TestFileBuilder;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ParquetJoinTest {

  private final int numRecord = 100000;
  private final Configuration conf = new Configuration();
  private final ParquetConfiguration parquetConf = new PlainParquetConfiguration();
  private final ParquetProperties.WriterVersion writerVersion;
  private final IndexCache.CacheStrategy indexCacheStrategy;
  private final boolean usingHadoop;

  private List<EncryptionTestFile> inputFilesL = null;
  private List<EncryptionTestFile> inputFilesR = null;
  private String outputFile = null;
  private ParquetJoiner joiner = null;

  @Parameterized.Parameters(name = "WriterVersion = {0}, IndexCacheStrategy = {1}, UsingHadoop = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
//       {"v1", "NONE", true},
//       {"v1", "PREFETCH_BLOCK", true},
//       {"v2", "NONE", true},
//       {"v2", "PREFETCH_BLOCK", true},
//       {"v1", "NONE", false},
//       {"v1", "PREFETCH_BLOCK", false},
//       {"v2", "NONE", false},
      {"v2", "PREFETCH_BLOCK", false}
    };
  }

  @Before
  public void setUp() {
    outputFile = TestFileBuilder.createTempFile("test");
  }

  public ParquetJoinTest(String writerVersion, String indexCacheStrategy, boolean usingHadoop) {
    this.writerVersion = ParquetProperties.WriterVersion.fromString(writerVersion);
    this.indexCacheStrategy = IndexCache.CacheStrategy.valueOf(indexCacheStrategy);
    this.usingHadoop = usingHadoop;
  }

  @Test
  public void testMergeTwoFilesOnly() throws Exception {
    testMultipleInputFilesSetup();

    // Only merge two files but do not change anything.
    List<Path> inputPathsL = new ArrayList<>();
    for (EncryptionTestFile inputFile : inputFilesL) {
      inputPathsL.add(new Path(inputFile.getFileName()));
    }
    List<Path> inputPathsR = new ArrayList<>();
    for (EncryptionTestFile inputFile : inputFilesR) {
      inputPathsR.add(new Path(inputFile.getFileName()));
    }
    JoinOptions.Builder builder = createBuilder(inputPathsL, ImmutableList.of(inputPathsR));
    JoinOptions options = builder.indexCacheStrategy(indexCacheStrategy).build();

    joiner = new ParquetJoiner(options);
    joiner.processBlocks();
    joiner.close();

    // Verify the schema are not changed
    ParquetMetadata pmd =
        ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    MessageType expectSchema = createSchemaL();
//     assertEquals(expectSchema, schema);

    // Verify the merged data are not changed
    validateColumnData(null);
  }

  private void testMultipleInputFilesSetup() throws IOException {
    inputFilesL = Lists.newArrayList();
    inputFilesL.add(new TestFileBuilder(conf, createSchemaL())
        .withNumRecord(numRecord)
        .withCodec("GZIP")
        .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
        .withWriterVersion(writerVersion)
        .build());
    inputFilesR = Lists.newArrayList();
    inputFilesR.add(new TestFileBuilder(conf, createSchemaR())
        .withNumRecord(numRecord)
        .withCodec("UNCOMPRESSED")
        .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
        .withWriterVersion(writerVersion)
        .build());
  }

  private MessageType createSchemaL() {
    return new MessageType(
        "schema",
        new PrimitiveType(OPTIONAL, INT64, "DocId"),
        new PrimitiveType(REQUIRED, BINARY, "Name"),
        new PrimitiveType(OPTIONAL, BINARY, "Gender"),
        new PrimitiveType(REPEATED, FLOAT, "FloatFraction"));
  }


  private MessageType createSchemaR() {
    return new MessageType(
        "schema",
        new PrimitiveType(OPTIONAL, DOUBLE, "DoubleFraction"),
        new GroupType(
            OPTIONAL,
            "Links",
            new PrimitiveType(REPEATED, BINARY, "Backward"),
            new PrimitiveType(REPEATED, BINARY, "Forward")));
  }

  private void validateColumnData(
      FileDecryptionProperties fileDecryptionProperties)
      throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(outputFile))
        .withConf(conf)
        .withDecryption(fileDecryptionProperties)
        .build();

    // Get total number of rows from input files
    int totalRows = 0;
    for (EncryptionTestFile inputFile : inputFilesL) {
      totalRows += inputFile.getFileContent().length;
    }

    for (int i = 0; i < totalRows; i++) {
      Group group = reader.read();
      assertNotNull(group);

      SimpleGroup expectGroupL = inputFilesL.get(i / numRecord).getFileContent()[i % numRecord];
      SimpleGroup expectGroupR = inputFilesR.get(i / numRecord).getFileContent()[i % numRecord];

      assertEquals(group.getLong("DocId", 0), expectGroupL.getLong("DocId", 0));
      assertArrayEquals(
          group.getBinary("Name", 0).getBytes(),
          expectGroupL.getBinary("Name", 0).getBytes());
      assertArrayEquals(
          group.getBinary("Gender", 0).getBytes(),
          expectGroupL.getBinary("Gender", 0).getBytes());
      assertEquals(group.getFloat("FloatFraction", 0), expectGroupL.getFloat("FloatFraction", 0), 0);
      assertEquals(group.getDouble("DoubleFraction", 0), expectGroupR.getDouble("DoubleFraction", 0), 0);
      Group subGroup = group.getGroup("Links", 0);
        assertArrayEquals(
            subGroup.getBinary("Backward", 0).getBytes(),
            expectGroupR
                .getGroup("Links", 0)
                .getBinary("Backward", 0)
                .getBytes());
      assertArrayEquals(
          subGroup.getBinary("Forward", 0).getBytes(),
          expectGroupR
              .getGroup("Links", 0)
              .getBinary("Forward", 0)
              .getBytes());
    }

    reader.close();
  }


  private JoinOptions.Builder createBuilder(List<Path> inputPathsL, List<List<Path>> inputPathsR) throws IOException {
    JoinOptions.Builder builder;
    if (usingHadoop) {
      Path outputPath = new Path(outputFile);
      builder = new JoinOptions.Builder(conf, inputPathsL, inputPathsR, outputPath);
    } else {
      OutputFile outputPath = HadoopOutputFile.fromPath(new Path(outputFile), conf);
      List<InputFile> inputsL = inputPathsL.stream()
          .map(p -> HadoopInputFile.fromPathUnchecked(p, conf))
          .collect(Collectors.toList());
      List<List<InputFile>> inputsR = inputPathsR
          .stream()
          .map(x -> x.stream().map(y -> (InputFile) HadoopInputFile.fromPathUnchecked(y, conf)).collect(Collectors.toList()))
          .collect(Collectors.toList());
      builder = new JoinOptions.Builder(parquetConf, inputsL, inputsR, outputPath);
    }
    return builder;
  }

}
