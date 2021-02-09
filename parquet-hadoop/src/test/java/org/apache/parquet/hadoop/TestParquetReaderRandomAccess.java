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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.*;
import org.apache.parquet.statistics.DataGenerationContext;
import org.apache.parquet.statistics.RandomValues;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.*;

public class TestParquetReaderRandomAccess {
  private static final int KILOBYTE = 1 << 10;
  private static final long RANDOM_SEED = 7174252115631550700L;

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void test() throws IOException {
    Random random = new Random(RANDOM_SEED);

    File file = temp.newFile("test_file.parquet");
    file.delete();

    int blockSize = 500 * KILOBYTE;
    int pageSize = 20 * KILOBYTE;

    List<DataContext> contexts = new ArrayList<>();

    for (boolean enableDictionary : new boolean[]{false, true}) {
      for (WriterVersion writerVersion : new WriterVersion[]{WriterVersion.PARQUET_1_0, WriterVersion.PARQUET_2_0}) {
       contexts.add(
         new DataContextRandom(random.nextLong(), file, blockSize,
           pageSize, enableDictionary, writerVersion));
       contexts.add(
         new DataContextRandomAndSequential(random.nextLong(), file, blockSize,
           pageSize, enableDictionary, writerVersion));
      }
    }

    for (DataContext context : contexts) {
      DataGenerationContext.writeAndTest(context);
    }
  }

  public static abstract class DataContext extends DataGenerationContext.WriteContext {

    private static final int recordCount = 100_000;

    private final Random random;

    private final List<RandomValues.RandomValueGenerator<?>> randomGenerators;

    protected final ColumnDescriptor testColumnDescriptor;

    public DataContext(long seed, File path, int blockSize, int pageSize, boolean enableDictionary, ParquetProperties.WriterVersion version) throws IOException {
      super(path, buildSchema(seed), blockSize, pageSize, enableDictionary, true, version);

      this.random = new Random(seed);

      int fixedLength = schema.getType("fixed-binary").asPrimitiveType().getTypeLength();

      this.testColumnDescriptor = super.schema.getColumnDescription(new String[]{"unconstrained-i64"});

      randomGenerators = Arrays.asList(
        new RandomValues.IntGenerator(random.nextLong()),
        new RandomValues.LongGenerator(random.nextLong()),
        new RandomValues.FloatGenerator(random.nextLong()),
        new RandomValues.DoubleGenerator(random.nextLong()),
        new RandomValues.StringGenerator(random.nextLong()),
        new RandomValues.FixedGenerator(random.nextLong(), fixedLength),
        new RandomValues.UnconstrainedIntGenerator(random.nextLong()),
        new RandomValues.UnconstrainedLongGenerator(random.nextLong())
      );
    }

    private static MessageType buildSchema(long seed) {
      Random random = new Random(seed);
      int fixedBinaryLength = random.nextInt(21) + 1;

      return new MessageType("schema",
        new PrimitiveType(OPTIONAL, INT32, "i32"),
        new PrimitiveType(OPTIONAL, INT64, "i64"),
        new PrimitiveType(OPTIONAL, FLOAT, "sngl"),
        new PrimitiveType(OPTIONAL, DOUBLE, "dbl"),
        new PrimitiveType(OPTIONAL, BINARY, "strings"),
        new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, fixedBinaryLength, "fixed-binary"),
        new PrimitiveType(REQUIRED, INT32, "unconstrained-i32"),
        new PrimitiveType(REQUIRED, INT64, "unconstrained-i64")
      );
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < recordCount; index++) {
        Group group = new SimpleGroup(super.schema);

        for (int column = 0, columnCnt = schema.getFieldCount(); column < columnCnt; ++column) {
          Type type = schema.getType(column);
          RandomValues.RandomValueGenerator<?> generator = randomGenerators.get(column);
          if (type.isRepetition(OPTIONAL) && generator.shouldGenerateNull()) {
            continue;
          }
          switch (type.asPrimitiveType().getPrimitiveTypeName()) {
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
            case INT96:
              group.append(type.getName(), ((RandomValues.RandomBinaryBase<?>) generator).nextBinaryValue());
              break;
            case INT32:
              group.append(type.getName(), (Integer) generator.nextValue());
              break;
            case INT64:
              group.append(type.getName(), (Long) generator.nextValue());
              break;
            case FLOAT:
              group.append(type.getName(), (Float) generator.nextValue());
              break;
            case DOUBLE:
              group.append(type.getName(), (Double) generator.nextValue());
              break;
            case BOOLEAN:
              group.append(type.getName(), (Boolean) generator.nextValue());
              break;
          }
        }
        writer.write(group);
      }
    }

    public static byte[] getBytesFromPage(DataPage page) throws IOException {
      if (page instanceof  DataPageV1) {
        return ((DataPageV1) page).getBytes().toByteArray();
      } else if (page instanceof DataPageV2) {
        return ((DataPageV2) page).getData().toByteArray();
      } else {
        fail();
        return null;
      }
    }

    @Override
    public void test() throws IOException {
      Configuration configuration = new Configuration();
      ParquetReadOptions options = ParquetReadOptions.builder().build();

      List<byte[]> testPageBytes = new ArrayList<>();
      List<Integer> testPageValueCounts = new ArrayList<>();

      try (ParquetFileReader reader = new ParquetFileReader(HadoopInputFile.fromPath(super.fsPath, configuration), options)) {
        PageReadStore pages;
        while ((pages = reader.readNextRowGroup()) != null) {
          DataPage page = pages.getPageReader(testColumnDescriptor).readPage();
          testPageBytes.add(getBytesFromPage(page));
          testPageValueCounts.add(page.getValueCount());
        }
      }

      try (ParquetFileReader reader = new ParquetFileReader(HadoopInputFile.fromPath(super.fsPath, configuration), options)) {
        List<BlockMetaData> blocks = reader.getRowGroups();

        // Randomize indexes
        List<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < blocks.size(); i++) {
          for (int j = 0; j < 4; j++) {
            indexes.add(i);
          }
        }

        Collections.shuffle(indexes, random);

        test(reader, indexes, testPageBytes, testPageValueCounts);
      }
    }

    protected abstract void test(ParquetFileReader reader, List<Integer> indexes, List<byte[]> idPageBytes, List<Integer> idPageValueCounts) throws IOException;
  }

  public static class DataContextRandom extends DataContext {

    public DataContextRandom(long seed, File path, int blockSize, int pageSize, boolean enableDictionary, ParquetProperties.WriterVersion version) throws IOException {
      super(seed, path, blockSize, pageSize, enableDictionary, version);
    }

    @Override
    protected void test(ParquetFileReader reader, List<Integer> indexes, List<byte[]> testPageBytes, List<Integer> testPageValueCounts) throws IOException {
      List<BlockMetaData> blocks = reader.getRowGroups();

      for (int index: indexes) {
        PageReadStore pages = reader.readRowGroup(blocks.get(index));
        DataPage page = pages.getPageReader(testColumnDescriptor).readPage();
        assertArrayEquals(testPageBytes.get(index), getBytesFromPage(page));
        assertEquals((int) testPageValueCounts.get(index), page.getValueCount());
      }
    }
  }

  public static class DataContextRandomAndSequential extends DataContext {

    public DataContextRandomAndSequential(long seed, File path, int blockSize, int pageSize, boolean enableDictionary, ParquetProperties.WriterVersion version) throws IOException {
      super(seed, path, blockSize, pageSize, enableDictionary, version);
    }

    @Override
    protected void test(ParquetFileReader reader, List<Integer> indexes, List<byte[]> testPageBytes, List<Integer> testPageValueCounts) throws IOException {
      List<BlockMetaData> blocks = reader.getRowGroups();
      int splitPoint = indexes.size()/2;

      {
        PageReadStore pages = reader.readNextRowGroup();
        DataPage page = pages.getPageReader(testColumnDescriptor).readPage();
        assertArrayEquals(testPageBytes.get(0), getBytesFromPage(page));
        assertEquals((int) testPageValueCounts.get(0), page.getValueCount());
      }
      for (int i = 0; i < splitPoint; i++) {
        int index = indexes.get(i);
        PageReadStore pages = reader.readRowGroup(blocks.get(index));
        DataPage page = pages.getPageReader(testColumnDescriptor).readPage();
        assertArrayEquals(testPageBytes.get(index), getBytesFromPage(page));
        assertEquals((int) testPageValueCounts.get(index), page.getValueCount());
      }
      {
        PageReadStore pages = reader.readNextRowGroup();
        DataPage page = pages.getPageReader(testColumnDescriptor).readPage();
        assertArrayEquals(testPageBytes.get(1), getBytesFromPage(page));
        assertEquals((int) testPageValueCounts.get(1), page.getValueCount());
      }
      for (int i = splitPoint; i < indexes.size(); i++) {
        int index = indexes.get(i);
        PageReadStore pages = reader.readRowGroup(blocks.get(index));
        DataPage page = pages.getPageReader(testColumnDescriptor).readPage();
        assertArrayEquals(testPageBytes.get(index), getBytesFromPage(page));
        assertEquals((int) testPageValueCounts.get(index), page.getValueCount());
      }
      {
        PageReadStore pages = reader.readNextRowGroup();
        DataPage page = pages.getPageReader(testColumnDescriptor).readPage();
        assertArrayEquals(testPageBytes.get(2), getBytesFromPage(page));
        assertEquals((int) testPageValueCounts.get(2), page.getValueCount());
      }
    }
  }

}
