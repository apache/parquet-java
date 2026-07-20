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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.util.TestFileBuilder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIndexCache {
  private final Configuration conf = new Configuration();
  private final int numRecords = 100000;
  private final MessageType schema = new MessageType(
      "schema",
      new PrimitiveType(OPTIONAL, INT64, "DocId"),
      new PrimitiveType(REQUIRED, BINARY, "Name"),
      new PrimitiveType(OPTIONAL, BINARY, "Gender"),
      new GroupType(
          OPTIONAL,
          "Links",
          new PrimitiveType(REPEATED, BINARY, "Backward"),
          new PrimitiveType(REPEATED, BINARY, "Forward")));

  private final ParquetProperties.WriterVersion writerVersion;

  @Parameterized.Parameters(name = "WriterVersion = {0}, IndexCacheStrategy = {1}")
  public static Object[] parameters() {
    return new Object[] {"v1", "v2"};
  }

  public TestIndexCache(String writerVersion) {
    this.writerVersion = ParquetProperties.WriterVersion.fromString(writerVersion);
  }

  @Test
  public void testNoneCacheStrategy() throws IOException {
    String file = createTestFile("DocID");

    ParquetReadOptions options = ParquetReadOptions.builder().build();
    ParquetFileReader fileReader = new ParquetFileReader(new LocalInputFile(Paths.get(file)), options);
    IndexCache indexCache = IndexCache.create(fileReader, new HashSet<>(), IndexCache.CacheStrategy.NONE, false);
    assertThat(indexCache).isInstanceOf(NoneIndexCache.class);
    List<BlockMetaData> blocks = fileReader.getFooter().getBlocks();
    for (BlockMetaData blockMetaData : blocks) {
      indexCache.setBlockMetadata(blockMetaData);
      for (ColumnChunkMetaData chunk : blockMetaData.getColumns()) {
        validateColumnIndex(fileReader.readColumnIndex(chunk), indexCache.getColumnIndex(chunk));
        validateOffsetIndex(fileReader.readOffsetIndex(chunk), indexCache.getOffsetIndex(chunk));

        assertThat(indexCache.getBloomFilter(chunk))
            .as("BloomFilter should match")
            .isEqualTo(fileReader.readBloomFilter(chunk));
      }
    }
  }

  @Test
  public void testPrefetchCacheStrategy() throws IOException {
    String file = createTestFile("DocID", "Name");

    ParquetReadOptions options = ParquetReadOptions.builder().build();
    ParquetFileReader fileReader = new ParquetFileReader(new LocalInputFile(Paths.get(file)), options);
    Set<ColumnPath> columns = new HashSet<>();
    columns.add(ColumnPath.fromDotString("DocId"));
    columns.add(ColumnPath.fromDotString("Name"));
    columns.add(ColumnPath.fromDotString("Gender"));
    columns.add(ColumnPath.fromDotString("Links.Backward"));
    columns.add(ColumnPath.fromDotString("Links.Forward"));

    IndexCache indexCache = IndexCache.create(fileReader, columns, IndexCache.CacheStrategy.PREFETCH_BLOCK, false);
    assertThat(indexCache).isInstanceOf(PrefetchIndexCache.class);
    validPrecacheIndexCache(fileReader, indexCache, columns, false);

    indexCache = IndexCache.create(fileReader, columns, IndexCache.CacheStrategy.PREFETCH_BLOCK, true);
    assertThat(indexCache).isInstanceOf(PrefetchIndexCache.class);
    validPrecacheIndexCache(fileReader, indexCache, columns, true);
  }

  private String createTestFile(String... bloomFilterEnabledColumns) throws IOException {
    return new TestFileBuilder(conf, schema)
        .withNumRecord(numRecords)
        .withCodec("ZSTD")
        .withRowGroupSize(8L * 1024 * 1024)
        .withBloomFilterEnabled(bloomFilterEnabledColumns)
        .withWriterVersion(writerVersion)
        .build()
        .getFileName();
  }

  private static void validPrecacheIndexCache(
      ParquetFileReader fileReader, IndexCache indexCache, Set<ColumnPath> columns, boolean freeCacheAfterGet)
      throws IOException {
    List<BlockMetaData> blocks = fileReader.getFooter().getBlocks();
    for (BlockMetaData blockMetaData : blocks) {
      indexCache.setBlockMetadata(blockMetaData);
      for (ColumnChunkMetaData chunk : blockMetaData.getColumns()) {
        validateColumnIndex(fileReader.readColumnIndex(chunk), indexCache.getColumnIndex(chunk));
        validateOffsetIndex(fileReader.readOffsetIndex(chunk), indexCache.getOffsetIndex(chunk));

        assertThat(indexCache.getBloomFilter(chunk))
            .as("BloomFilter should match")
            .isEqualTo(fileReader.readBloomFilter(chunk));

        if (freeCacheAfterGet) {
          assertThatThrownBy(() -> indexCache.getColumnIndex(chunk))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("Not found cached ColumnIndex");
          assertThatThrownBy(() -> indexCache.getOffsetIndex(chunk))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("Not found cached OffsetIndex");
          if (columns.contains(chunk.getPath())) {
            assertThatThrownBy(() -> indexCache.getBloomFilter(chunk))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Not found cached BloomFilter");
          }
        }
      }
    }
  }

  private static void validateColumnIndex(ColumnIndex expected, ColumnIndex target) {
    if (expected == null) {
      assertThat(target).as("ColumnIndex should should equal").isEqualTo(expected);
    } else {
      assertThat(target).as("ColumnIndex should not be null").isNotNull();
      assertThat(target.getClass()).isEqualTo(expected.getClass());
      assertThat(target.getMinValues()).containsExactlyElementsOf(expected.getMinValues());
      assertThat(target.getMaxValues()).containsExactlyElementsOf(expected.getMaxValues());
      assertThat(target.getBoundaryOrder()).isEqualTo(expected.getBoundaryOrder());
      assertThat(target.getNullCounts()).containsExactlyElementsOf(expected.getNullCounts());
      assertThat(target.getNullPages()).containsExactlyElementsOf(expected.getNullPages());
    }
  }

  private static void validateOffsetIndex(OffsetIndex expected, OffsetIndex target) {
    if (expected == null) {
      assertThat(target).as("OffsetIndex should should equal").isEqualTo(expected);
    } else {
      assertThat(target).as("OffsetIndex should not be null").isNotNull();
      assertThat(target.getClass()).isEqualTo(expected.getClass());
      assertThat(target).asString().isEqualTo(expected.toString());
    }
  }
}
