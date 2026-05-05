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

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestMergeMetadataFiles {
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static final MessageType schema = parseMessageType("message test { "
      + "required binary binary_field; "
      + "required int32 int32_field; "
      + "required int64 int64_field; "
      + "required boolean boolean_field; "
      + "required float float_field; "
      + "required double double_field; "
      + "required fixed_len_byte_array(3) flba_field; "
      + "required int96 int96_field; "
      + "} ");

  // schema1 with a field removed
  private static final MessageType schema2 = parseMessageType("message test { "
      + "required binary binary_field; "
      + "required int32 int32_field; "
      + "required int64 int64_field; "
      + "required boolean boolean_field; "
      + "required float float_field; "
      + "required double double_field; "
      + "required fixed_len_byte_array(3) flba_field; "
      + "} ");

  private static void writeFile(File out, Configuration conf, boolean useSchema2) throws IOException {
    if (!useSchema2) {
      GroupWriteSupport.setSchema(schema, conf);
    } else {
      GroupWriteSupport.setSchema(schema2, conf);
    }
    SimpleGroupFactory f = new SimpleGroupFactory(schema);

    Map<String, String> extraMetaData = new HashMap<String, String>();
    extraMetaData.put("schema_num", useSchema2 ? "2" : "1");

    ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(out.getAbsolutePath()))
        .withConf(conf)
        .withExtraMetaData(extraMetaData)
        .build();

    for (int i = 0; i < 1000; i++) {
      Group g = f.newGroup()
          .append("binary_field", "test" + i)
          .append("int32_field", i)
          .append("int64_field", (long) i)
          .append("boolean_field", i % 2 == 0)
          .append("float_field", (float) i)
          .append("double_field", (double) i)
          .append("flba_field", "foo");

      if (!useSchema2) {
        g = g.append("int96_field", Binary.fromConstantByteArray(new byte[12]));
      }

      writer.write(g);
    }
    writer.close();
  }

  private static class WrittenFileInfo {
    public Configuration conf;
    public Path metaPath1;
    public Path metaPath2;
    public Path commonMetaPath1;
    public Path commonMetaPath2;
  }

  private WrittenFileInfo writeFiles(boolean mixedSchemas) throws Exception {
    WrittenFileInfo info = new WrittenFileInfo();
    Configuration conf = new Configuration();
    info.conf = conf;

    File root1 = new File(temp.getRoot(), "out1");
    File root2 = new File(temp.getRoot(), "out2");
    Path rootPath1 = new Path(root1.getAbsolutePath());
    Path rootPath2 = new Path(root2.getAbsolutePath());

    for (int i = 0; i < 10; i++) {
      writeFile(new File(root1, i + ".parquet"), conf, true);
    }

    List<Footer> footers = ParquetFileReader.readFooters(
        conf, rootPath1.getFileSystem(conf).getFileStatus(rootPath1), false);
    ParquetFileWriter.writeMetadataFile(conf, rootPath1, footers, JobSummaryLevel.ALL);

    for (int i = 0; i < 7; i++) {
      writeFile(new File(root2, i + ".parquet"), conf, !mixedSchemas);
    }

    footers = ParquetFileReader.readFooters(
        conf, rootPath2.getFileSystem(conf).getFileStatus(rootPath2), false);
    ParquetFileWriter.writeMetadataFile(conf, rootPath2, footers, JobSummaryLevel.ALL);

    info.commonMetaPath1 =
        new Path(new File(root1, ParquetFileWriter.PARQUET_COMMON_METADATA_FILE).getAbsolutePath());
    info.commonMetaPath2 =
        new Path(new File(root2, ParquetFileWriter.PARQUET_COMMON_METADATA_FILE).getAbsolutePath());
    info.metaPath1 = new Path(new File(root1, ParquetFileWriter.PARQUET_METADATA_FILE).getAbsolutePath());
    info.metaPath2 = new Path(new File(root2, ParquetFileWriter.PARQUET_METADATA_FILE).getAbsolutePath());

    return info;
  }

  @Test
  public void testMergeMetadataFiles() throws Exception {
    WrittenFileInfo info = writeFiles(false);

    ParquetMetadata commonMeta1 =
        ParquetFileReader.readFooter(info.conf, info.commonMetaPath1, ParquetMetadataConverter.NO_FILTER);
    ParquetMetadata commonMeta2 =
        ParquetFileReader.readFooter(info.conf, info.commonMetaPath2, ParquetMetadataConverter.NO_FILTER);
    ParquetMetadata meta1 =
        ParquetFileReader.readFooter(info.conf, info.metaPath1, ParquetMetadataConverter.NO_FILTER);
    ParquetMetadata meta2 =
        ParquetFileReader.readFooter(info.conf, info.metaPath2, ParquetMetadataConverter.NO_FILTER);

    assertTrue(commonMeta1.getBlocks().isEmpty());
    assertTrue(commonMeta2.getBlocks().isEmpty());
    assertEquals(
        commonMeta1.getFileMetaData().getSchema(),
        commonMeta2.getFileMetaData().getSchema());

    assertFalse(meta1.getBlocks().isEmpty());
    assertFalse(meta2.getBlocks().isEmpty());
    assertEquals(
        meta1.getFileMetaData().getSchema(), meta2.getFileMetaData().getSchema());

    assertEquals(
        commonMeta1.getFileMetaData().getKeyValueMetaData(),
        commonMeta2.getFileMetaData().getKeyValueMetaData());
    assertEquals(
        meta1.getFileMetaData().getKeyValueMetaData(),
        meta2.getFileMetaData().getKeyValueMetaData());

    // test file serialization
    Path mergedOut = new Path(new File(temp.getRoot(), "merged_meta").getAbsolutePath());
    Path mergedCommonOut = new Path(new File(temp.getRoot(), "merged_common_meta").getAbsolutePath());
    ParquetFileWriter.writeMergedMetadataFile(List.of(info.metaPath1, info.metaPath2), mergedOut, info.conf);
    ParquetFileWriter.writeMergedMetadataFile(
        List.of(info.commonMetaPath1, info.commonMetaPath2), mergedCommonOut, info.conf);

    ParquetMetadata mergedMeta =
        ParquetFileReader.readFooter(info.conf, mergedOut, ParquetMetadataConverter.NO_FILTER);
    ParquetMetadata mergedCommonMeta =
        ParquetFileReader.readFooter(info.conf, mergedCommonOut, ParquetMetadataConverter.NO_FILTER);

    // ideally we'd assert equality here, but BlockMetaData and it's references don't implement equals
    assertEquals(
        meta1.getBlocks().size() + meta2.getBlocks().size(),
        mergedMeta.getBlocks().size());
    assertTrue(mergedCommonMeta.getBlocks().isEmpty());

    assertEquals(
        meta1.getFileMetaData().getSchema(),
        mergedMeta.getFileMetaData().getSchema());
    assertEquals(
        commonMeta1.getFileMetaData().getSchema(),
        mergedCommonMeta.getFileMetaData().getSchema());

    assertEquals(
        meta1.getFileMetaData().getKeyValueMetaData(),
        mergedMeta.getFileMetaData().getKeyValueMetaData());
    assertEquals(
        commonMeta1.getFileMetaData().getKeyValueMetaData(),
        mergedCommonMeta.getFileMetaData().getKeyValueMetaData());
  }

  @Test
  public void testThrowsWhenIncompatible() throws Exception {
    WrittenFileInfo info = writeFiles(true);

    Path mergedOut = new Path(new File(temp.getRoot(), "merged_meta").getAbsolutePath());
    Path mergedCommonOut = new Path(new File(temp.getRoot(), "merged_common_meta").getAbsolutePath());

    try {
      ParquetFileWriter.writeMergedMetadataFile(List.of(info.metaPath1, info.metaPath2), mergedOut, info.conf);
      fail("this should throw");
    } catch (RuntimeException e) {
      boolean eq1 =
          e.getMessage().equals("could not merge metadata: key schema_num has conflicting values: [2, 1]");
      boolean eq2 =
          e.getMessage().equals("could not merge metadata: key schema_num has conflicting values: [1, 2]");

      assertEquals(eq1 || eq2, true);
    }

    try {
      ParquetFileWriter.writeMergedMetadataFile(
          List.of(info.commonMetaPath1, info.commonMetaPath2), mergedCommonOut, info.conf);
      fail("this should throw");
    } catch (RuntimeException e) {
      boolean eq1 =
          e.getMessage().equals("could not merge metadata: key schema_num has conflicting values: [2, 1]");
      boolean eq2 =
          e.getMessage().equals("could not merge metadata: key schema_num has conflicting values: [1, 2]");

      assertEquals(eq1 || eq2, true);
    }
  }
}
