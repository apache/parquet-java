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
package org.apache.parquet.hadoop.rewrite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.hadoop.util.EncryptionTestFile;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.TestFileBuilder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.*;
import static org.junit.Assert.*;

public class ParquetRewriterTest {

  private final int numRecord = 100000;
  private Configuration conf = new Configuration();
  private EncryptionTestFile inputFile = null;
  private String outputFile = null;
  private ParquetRewriter rewriter = null;

  @Test
  public void testPruneSingleColumnAndTranslateCodec() throws Exception {
    testSetup("GZIP");

    Path inputPath = new Path(inputFile.getFileName());
    Path outputPath = new Path(outputFile);
    List<String> pruneColumns = Arrays.asList("Gender");
    CompressionCodecName newCodec = CompressionCodecName.ZSTD;
    RewriteOptions.Builder builder = new RewriteOptions.Builder(conf, inputPath, outputPath);
    RewriteOptions options = builder.prune(pruneColumns).transform(newCodec).build();

    rewriter = new ParquetRewriter(options);
    rewriter.processBlocks();
    rewriter.close();

    // Verify the schema are not changed for the columns not pruned
    ParquetMetadata pmd = ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    List<Type> fields = schema.getFields();
    assertEquals(fields.size(), 3);
    assertEquals(fields.get(0).getName(), "DocId");
    assertEquals(fields.get(1).getName(), "Name");
    assertEquals(fields.get(2).getName(), "Links");
    List<Type> subFields = fields.get(2).asGroupType().getFields();
    assertEquals(subFields.size(), 2);
    assertEquals(subFields.get(0).getName(), "Backward");
    assertEquals(subFields.get(1).getName(), "Forward");

    // Verify codec has been translated
    verifyCodec(outputFile, CompressionCodecName.ZSTD);

    // Verify the data are not changed for the columns not pruned
    validateColumnData(outputFile, inputFile.getFileContent(), new HashSet<>(pruneColumns), Collections.emptySet());

    // Verify the page index
    validatePageIndex(new HashMap<Integer, Integer>() {{
      put(0, 0);
      put(1, 1);
      put(2, 3);
      put(3, 4);
    }});
  }

  @Test
  public void testPruneNullifyAndTranslateCodec() throws Exception {
    testSetup("UNCOMPRESSED");

    Path inputPath = new Path(inputFile.getFileName());
    Path outputPath = new Path(outputFile);
    List<String> pruneColumns = Arrays.asList("Gender");
    Map<String, MaskMode> maskColumns = new HashMap<>();
    maskColumns.put("Links.Forward", MaskMode.NULLIFY);
    CompressionCodecName newCodec = CompressionCodecName.GZIP;
    RewriteOptions.Builder builder = new RewriteOptions.Builder(conf, inputPath, outputPath);
    RewriteOptions options = builder.prune(pruneColumns).mask(maskColumns).transform(newCodec).build();

    rewriter = new ParquetRewriter(options);
    rewriter.processBlocks();
    rewriter.close();

    // Verify the schema are not changed for the columns not pruned
    ParquetMetadata pmd = ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    List<Type> fields = schema.getFields();
    assertEquals(fields.size(), 3);
    assertEquals(fields.get(0).getName(), "DocId");
    assertEquals(fields.get(1).getName(), "Name");
    assertEquals(fields.get(2).getName(), "Links");
    List<Type> subFields = fields.get(2).asGroupType().getFields();
    assertEquals(subFields.size(), 2);
    assertEquals(subFields.get(0).getName(), "Backward");
    assertEquals(subFields.get(1).getName(), "Forward");

    // Verify codec has been translated
    verifyCodec(outputFile, newCodec);

    // Verify the data are not changed for the columns not pruned
    validateColumnData(outputFile, inputFile.getFileContent(), new HashSet<>(pruneColumns), maskColumns.keySet());

    // Verify the page index
    validatePageIndex(new HashMap<Integer, Integer>() {{
      put(0, 0);
      put(1, 1);
      put(2, 3);
    }});
  }

  private void testSetup(String compression) throws IOException {
    MessageType schema = createSchema();
    inputFile = new TestFileBuilder(conf, schema)
      .withNumRecord(numRecord)
      .withCodec(compression)
      .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
      .build();
    outputFile = TestFileBuilder.createTempFile("test");
  }

  private MessageType createSchema() {
    return new MessageType("schema",
      new PrimitiveType(OPTIONAL, INT64, "DocId"),
      new PrimitiveType(REQUIRED, BINARY, "Name"),
      new PrimitiveType(OPTIONAL, BINARY, "Gender"),
      new GroupType(OPTIONAL, "Links",
        new PrimitiveType(REPEATED, BINARY, "Backward"),
        new PrimitiveType(REPEATED, BINARY, "Forward")));
  }

  private void validateColumnData(String file,
                                  SimpleGroup[] fileContent,
                                  Set<String> prunePaths,
                                  Set<String> nullifiedPaths) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      if (!prunePaths.contains("DocId") && !nullifiedPaths.contains("DocId")) {
        assertTrue(group.getLong("DocId", 0) == fileContent[i].getLong("DocId", 0));
      }
      if (!prunePaths.contains("Name") && !nullifiedPaths.contains("Name")) {
        assertArrayEquals(group.getBinary("Name", 0).getBytes(),
          fileContent[i].getBinary("Name", 0).getBytes());
      }
      if (!prunePaths.contains("Gender") && !nullifiedPaths.contains("Gender")) {
        assertArrayEquals(group.getBinary("Gender", 0).getBytes(),
          fileContent[i].getBinary("Gender", 0).getBytes());
      }
      Group subGroup = group.getGroup("Links", 0);
      if (!prunePaths.contains("Links.Backward") && !nullifiedPaths.contains("Links.Backward")) {
        assertArrayEquals(subGroup.getBinary("Backward", 0).getBytes(),
          fileContent[i].getGroup("Links", 0).getBinary("Backward", 0).getBytes());
      }
      if (!prunePaths.contains("Links.Forward") && !nullifiedPaths.contains("Links.Forward")) {
        assertArrayEquals(subGroup.getBinary("Forward", 0).getBytes(),
          fileContent[i].getGroup("Links", 0).getBinary("Forward", 0).getBytes());
      }
    }
    reader.close();
  }

  private void verifyCodec(String file, CompressionCodecName codec) throws IOException {
    ParquetMetadata pmd = ParquetFileReader.readFooter(conf, new Path(file), ParquetMetadataConverter.NO_FILTER);
    for (int i = 0; i < pmd.getBlocks().size(); i++) {
      BlockMetaData block = pmd.getBlocks().get(i);
      for (int j = 0; j < block.getColumns().size(); ++j) {
        ColumnChunkMetaData columnChunkMetaData = block.getColumns().get(j);
        assertEquals(columnChunkMetaData.getCodec(), codec);
      }
    }
  }

  /**
   * Verify the page index is correct.
   *
   * @param outFileColumnMapping the column mapping from the output file to the input file.
   */
  private void validatePageIndex(Map<Integer, Integer> outFileColumnMapping) throws Exception {
    ParquetMetadata inMetaData = ParquetFileReader.readFooter(conf, new Path(inputFile.getFileName()), NO_FILTER);
    ParquetMetadata outMetaData = ParquetFileReader.readFooter(conf, new Path(outputFile), NO_FILTER);
    Assert.assertEquals(inMetaData.getBlocks().size(), outMetaData.getBlocks().size());

    try (TransParquetFileReader inReader = new TransParquetFileReader(
      HadoopInputFile.fromPath(new Path(inputFile.getFileName()), conf), HadoopReadOptions.builder(conf).build());
         TransParquetFileReader outReader = new TransParquetFileReader(
           HadoopInputFile.fromPath(new Path(outputFile), conf), HadoopReadOptions.builder(conf).build())) {

      for (int i = 0; i < inMetaData.getBlocks().size(); i++) {
        BlockMetaData inBlockMetaData = inMetaData.getBlocks().get(i);
        BlockMetaData outBlockMetaData = outMetaData.getBlocks().get(i);

        for (int j = 0; j < outBlockMetaData.getColumns().size(); j++) {
          if (!outFileColumnMapping.containsKey(j)) {
            continue;
          }
          int columnIdFromInputFile = outFileColumnMapping.get(j);
          ColumnChunkMetaData inChunk = inBlockMetaData.getColumns().get(columnIdFromInputFile);
          ColumnIndex inColumnIndex = inReader.readColumnIndex(inChunk);
          OffsetIndex inOffsetIndex = inReader.readOffsetIndex(inChunk);
          ColumnChunkMetaData outChunk = outBlockMetaData.getColumns().get(j);
          ColumnIndex outColumnIndex = outReader.readColumnIndex(outChunk);
          OffsetIndex outOffsetIndex = outReader.readOffsetIndex(outChunk);
          if (inColumnIndex != null) {
            Assert.assertEquals(inColumnIndex.getBoundaryOrder(), outColumnIndex.getBoundaryOrder());
            Assert.assertEquals(inColumnIndex.getMaxValues(), outColumnIndex.getMaxValues());
            Assert.assertEquals(inColumnIndex.getMinValues(), outColumnIndex.getMinValues());
            Assert.assertEquals(inColumnIndex.getNullCounts(), outColumnIndex.getNullCounts());
          }
          if (inOffsetIndex != null) {
            List<Long> inOffsets = getOffsets(inReader, inChunk);
            List<Long> outOffsets = getOffsets(outReader, outChunk);
            Assert.assertEquals(inOffsets.size(), outOffsets.size());
            Assert.assertEquals(inOffsets.size(), inOffsetIndex.getPageCount());
            Assert.assertEquals(inOffsetIndex.getPageCount(), outOffsetIndex.getPageCount());
            for (int k = 0; k < inOffsetIndex.getPageCount(); k++) {
              Assert.assertEquals(inOffsetIndex.getFirstRowIndex(k), outOffsetIndex.getFirstRowIndex(k));
              Assert.assertEquals(inOffsetIndex.getLastRowIndex(k, inChunk.getValueCount()),
                outOffsetIndex.getLastRowIndex(k, outChunk.getValueCount()));
              Assert.assertEquals(inOffsetIndex.getOffset(k), (long) inOffsets.get(k));
              Assert.assertEquals(outOffsetIndex.getOffset(k), (long) outOffsets.get(k));
            }
          }
        }
      }
    }
  }

  private List<Long> getOffsets(TransParquetFileReader reader, ColumnChunkMetaData chunk) throws IOException {
    List<Long> offsets = new ArrayList<>();
    reader.setStreamPosition(chunk.getStartingPos());
    long readValues = 0;
    long totalChunkValues = chunk.getValueCount();
    while (readValues < totalChunkValues) {
      long curOffset = reader.getPos();
      PageHeader pageHeader = reader.readPageHeader();
      switch (pageHeader.type) {
        case DICTIONARY_PAGE:
          rewriter.readBlock(pageHeader.getCompressed_page_size(), reader);
          break;
        case DATA_PAGE:
          DataPageHeader headerV1 = pageHeader.data_page_header;
          offsets.add(curOffset);
          rewriter.readBlock(pageHeader.getCompressed_page_size(), reader);
          readValues += headerV1.getNum_values();
          break;
        case DATA_PAGE_V2:
          DataPageHeaderV2 headerV2 = pageHeader.data_page_header_v2;
          offsets.add(curOffset);
          int rlLength = headerV2.getRepetition_levels_byte_length();
          rewriter.readBlock(rlLength, reader);
          int dlLength = headerV2.getDefinition_levels_byte_length();
          rewriter.readBlock(dlLength, reader);
          int payLoadLength = pageHeader.getCompressed_page_size() - rlLength - dlLength;
          rewriter.readBlock(payLoadLength, reader);
          readValues += headerV2.getNum_values();
          break;
        default:
          throw new IOException("Not recognized page type");
      }
    }
    return offsets;
  }

}
